use std::sync::Arc;

use apache_avro::{from_avro_datum, Reader, Schema as AvroSchema};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;

use crate::buffer::avro_converter::RecordBatchBuilder;
use crate::buffer::error::{Result, TurbineError};
use crate::buffer::parquet_writer::{ParquetFileWriter, ParquetWriterConfig};

/// Application state shared across handlers
pub struct AppState {
    pub parquet_writer: ParquetFileWriter,
    pub schemas: RwLock<std::collections::HashMap<String, AvroSchema>>,
}

impl AppState {
    pub fn new(config: ParquetWriterConfig) -> Result<Self> {
        Ok(Self {
            parquet_writer: ParquetFileWriter::new(config)?,
            schemas: RwLock::new(std::collections::HashMap::new()),
        })
    }
}

/// Response for successful writes
#[derive(Serialize, Deserialize)]
pub struct WriteResponse {
    pub file_path: String,
    pub rows_written: usize,
    pub file_id: String,
}

/// Request to register a schema
#[derive(Deserialize)]
pub struct RegisterSchemaRequest {
    pub name: String,
    pub schema: String, // JSON schema
}

/// Response for schema registration
#[derive(Serialize, Deserialize)]
pub struct RegisterSchemaResponse {
    pub name: String,
    pub registered: bool,
}

/// Health check response
#[derive(Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

/// Build the Axum router with all endpoints
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/schema", post(register_schema))
        .route("/schema/:name", get(get_schema))
        .route("/write/:schema_name", post(write_avro_data))
        .route("/write-raw", post(write_avro_container))
        .with_state(state)
}

/// Health check endpoint
async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Register an Avro schema for later use
async fn register_schema(
    State(state): State<Arc<AppState>>,
    Json(request): Json<RegisterSchemaRequest>,
) -> Result<Json<RegisterSchemaResponse>> {
    let schema = AvroSchema::parse_str(&request.schema)
        .map_err(|e| TurbineError::InvalidSchema(e.to_string()))?;

    state
        .schemas
        .write()
        .insert(request.name.clone(), schema);

    info!(schema_name = %request.name, "Schema registered");

    Ok(Json(RegisterSchemaResponse {
        name: request.name,
        registered: true,
    }))
}

/// Get a registered schema
async fn get_schema(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Response> {
    let schemas = state.schemas.read();

    match schemas.get(&name) {
        Some(schema) => {
            let json = schema.canonical_form();
            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                json,
            )
                .into_response())
        }
        None => Ok((StatusCode::NOT_FOUND, "Schema not found").into_response()),
    }
}

/// Write Avro data using a pre-registered schema
/// Expects raw Avro binary data (not container format)
async fn write_avro_data(
    State(state): State<Arc<AppState>>,
    Path(schema_name): Path<String>,
    body: Bytes,
) -> Result<Json<WriteResponse>> {
    let schema = {
        let schemas = state.schemas.read();
        schemas
            .get(&schema_name)
            .cloned()
            .ok_or_else(|| TurbineError::InvalidSchema(format!("Schema '{}' not found", schema_name)))?
    };

    // Parse Avro records and convert to Arrow
    let mut builder = RecordBatchBuilder::new(schema.clone())?;

    let mut cursor = body.as_ref();
    while !cursor.is_empty() {
        match from_avro_datum(&schema, &mut cursor, None) {
            Ok(value) => {
                builder.append_value(&value)?;
            }
            Err(e) => {
                if cursor.is_empty() {
                    break;
                }
                warn!(error = %e, "Failed to parse Avro datum");
                return Err(TurbineError::AvroParse(e));
            }
        }
    }

    let rows = builder.row_count();
    if rows == 0 {
        return Err(TurbineError::Conversion("No records found in payload".to_string()));
    }

    let batch = builder.finish()?;
    let file_id = Uuid::new_v4().to_string();
    let filename = format!("{}.parquet", file_id);

    let path = state.parquet_writer.write_batch(&batch, &filename)?;

    info!(
        schema = %schema_name,
        rows = rows,
        file = %path.display(),
        "Wrote Parquet file"
    );

    Ok(Json(WriteResponse {
        file_path: path.to_string_lossy().to_string(),
        rows_written: rows,
        file_id,
    }))
}

/// Write Avro container file (with embedded schema)
async fn write_avro_container(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<Json<WriteResponse>> {
    let reader = Reader::new(body.as_ref())?;
    let schema = reader.writer_schema().clone();

    let mut builder = RecordBatchBuilder::new(schema)?;

    for value_result in reader {
        let value = value_result?;
        builder.append_value(&value)?;
    }

    let rows = builder.row_count();
    if rows == 0 {
        return Err(TurbineError::Conversion("No records found in Avro container".to_string()));
    }

    let batch = builder.finish()?;
    let file_id = Uuid::new_v4().to_string();
    let filename = format!("{}.parquet", file_id);

    let path = state.parquet_writer.write_batch(&batch, &filename)?;

    info!(rows = rows, file = %path.display(), "Wrote Parquet file from container");

    Ok(Json(WriteResponse {
        file_path: path.to_string_lossy().to_string(),
        rows_written: rows,
        file_id,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{to_avro_datum, types::Value as AvroValue, Writer};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use serde_json::json;
    use tempfile::tempdir;
    use tower::ServiceExt;

    async fn create_test_app() -> (Router, Arc<AppState>, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let state = Arc::new(AppState::new(config).unwrap());
        let app = build_router(state.clone());
        (app, state, temp_dir)
    }

    fn sample_schema_json() -> &'static str {
        r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#
    }

    #[tokio::test]
    async fn test_health_check() {
        let (app, _, _temp) = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let health: HealthResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(health.status, "healthy");
        assert_eq!(health.version, env!("CARGO_PKG_VERSION").to_string());
    }

    #[tokio::test]
    async fn test_register_schema() {
        let (app, state, _temp) = create_test_app().await;

        let request_body = json!({
            "name": "user",
            "schema": sample_schema_json()
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/schema")
                    .header("content-type", "application/json")
                    .body(Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let resp: RegisterSchemaResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(resp.name, "user");
        assert!(resp.registered);

        // Verify schema was stored
        assert!(state.schemas.read().contains_key("user"));
    }

    #[tokio::test]
    async fn test_register_invalid_schema() {
        let (app, _, _temp) = create_test_app().await;

        let request_body = json!({
            "name": "invalid",
            "schema": "not valid json schema"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/schema")
                    .header("content-type", "application/json")
                    .body(Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_schema() {
        let (app, state, _temp) = create_test_app().await;

        // Pre-register a schema
        let schema = AvroSchema::parse_str(sample_schema_json()).unwrap();
        state.schemas.write().insert("test_schema".to_string(), schema);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/schema/test_schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = String::from_utf8_lossy(&body);

        // Should contain the schema fields
        assert!(body_str.contains("User"));
        assert!(body_str.contains("id"));
        assert!(body_str.contains("name"));
    }

    #[tokio::test]
    async fn test_get_schema_not_found() {
        let (app, _, _temp) = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/schema/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_write_avro_data() {
        let (app, state, temp_dir) = create_test_app().await;

        // Register schema first
        let schema = AvroSchema::parse_str(sample_schema_json()).unwrap();
        state.schemas.write().insert("user".to_string(), schema.clone());

        // Create Avro binary data
        let record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(1)),
            ("name".to_string(), AvroValue::String("Alice".to_string())),
        ]);
        let mut avro_data = to_avro_datum(&schema, record.clone()).unwrap();

        // Add another record
        let record2 = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(2)),
            ("name".to_string(), AvroValue::String("Bob".to_string())),
        ]);
        avro_data.extend(to_avro_datum(&schema, record2).unwrap());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write/user")
                    .body(Body::from(avro_data))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let write_resp: WriteResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(write_resp.rows_written, 2);
        assert!(!write_resp.file_id.is_empty());
        assert!(write_resp.file_path.contains(".parquet"));

        // Verify file exists
        let expected_path = temp_dir.path().join(format!("{}.parquet", write_resp.file_id));
        assert!(expected_path.exists());
    }

    #[tokio::test]
    async fn test_write_avro_data_schema_not_found() {
        let (app, _, _temp) = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write/nonexistent")
                    .body(Body::from(vec![0u8; 10]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_write_avro_container() {
        let (app, _, temp_dir) = create_test_app().await;

        // Create an Avro container file
        let schema = AvroSchema::parse_str(sample_schema_json()).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        for i in 0..5 {
            let record = AvroValue::Record(vec![
                ("id".to_string(), AvroValue::Long(i)),
                ("name".to_string(), AvroValue::String(format!("User{}", i))),
            ]);
            writer.append(record).unwrap();
        }

        let container_data = writer.into_inner().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write-raw")
                    .body(Body::from(container_data))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let write_resp: WriteResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(write_resp.rows_written, 5);

        // Verify file exists
        let expected_path = temp_dir.path().join(format!("{}.parquet", write_resp.file_id));
        assert!(expected_path.exists());
    }

    #[tokio::test]
    async fn test_write_avro_container_empty() {
        let (app, _, _temp) = create_test_app().await;

        // Create an empty Avro container
        let schema = AvroSchema::parse_str(sample_schema_json()).unwrap();
        let writer = Writer::new(&schema, Vec::new());
        let container_data = writer.into_inner().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write-raw")
                    .body(Body::from(container_data))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn test_write_avro_container_invalid_data() {
        let (app, _, _temp) = create_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write-raw")
                    .body(Body::from("not valid avro data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_app_state_creation() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let state = AppState::new(config).unwrap();

        assert!(state.schemas.read().is_empty());
    }

    #[tokio::test]
    async fn test_concurrent_schema_registration() {
        let (_app, state, _temp) = create_test_app().await;

        // Register multiple schemas
        let schemas = vec![
            ("schema1", r#"{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}"#),
            ("schema2", r#"{"type":"record","name":"B","fields":[{"name":"y","type":"string"}]}"#),
            ("schema3", r#"{"type":"record","name":"C","fields":[{"name":"z","type":"boolean"}]}"#),
        ];

        for (name, schema_json) in schemas {
            let schema = AvroSchema::parse_str(schema_json).unwrap();
            state.schemas.write().insert(name.to_string(), schema);
        }

        assert_eq!(state.schemas.read().len(), 3);
        assert!(state.schemas.read().contains_key("schema1"));
        assert!(state.schemas.read().contains_key("schema2"));
        assert!(state.schemas.read().contains_key("schema3"));
    }

    #[tokio::test]
    async fn test_write_response_serialization() {
        let response = WriteResponse {
            file_path: "/tmp/test.parquet".to_string(),
            rows_written: 100,
            file_id: "abc-123".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("file_path"));
        assert!(json.contains("rows_written"));
        assert!(json.contains("file_id"));
        assert!(json.contains("100"));
    }
}
