use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterVersion};

use crate::buffer::error::Result;

/// Configuration for Parquet file writing
#[derive(Debug, Clone)]
pub struct ParquetWriterConfig {
    pub output_dir: PathBuf,
    pub compression: Compression,
    pub row_group_size: usize,
    pub data_page_size: usize,
    pub writer_version: WriterVersion,
    pub enable_dictionary: bool,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("./output"),
            compression: Compression::ZSTD(Default::default()),
            row_group_size: 1024 * 1024, // 1M rows per row group
            data_page_size: 1024 * 1024, // 1MB page size
            writer_version: WriterVersion::PARQUET_2_0,
            enable_dictionary: true,
        }
    }
}

impl ParquetWriterConfig {
    pub fn with_output_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.output_dir = dir.into();
        self
    }

    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }
}

/// High-performance Parquet file writer
pub struct ParquetFileWriter {
    config: ParquetWriterConfig,
}

impl ParquetFileWriter {
    pub fn new(config: ParquetWriterConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.output_dir)?;
        Ok(Self { config })
    }

    /// Write a RecordBatch to a new Parquet file
    pub fn write_batch(&self, batch: &RecordBatch, filename: &str) -> Result<PathBuf> {
        let path = self.config.output_dir.join(filename);
        self.write_batch_to_path(batch, &path)?;
        Ok(path)
    }

    /// Write a RecordBatch to a specific path
    pub fn write_batch_to_path(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let props = self.build_writer_properties();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;

        Ok(())
    }

    /// Write multiple RecordBatches to a single Parquet file
    pub fn write_batches(&self, batches: &[RecordBatch], filename: &str) -> Result<PathBuf> {
        if batches.is_empty() {
            return Err(crate::buffer::error::TurbineError::Conversion(
                "No batches to write".to_string(),
            ));
        }

        let path = self.config.output_dir.join(filename);
        let file = File::create(&path)?;
        let props = self.build_writer_properties();
        let schema = batches[0].schema();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

        for batch in batches {
            writer.write(batch)?;
        }

        writer.close()?;
        Ok(path)
    }

    fn build_writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_writer_version(self.config.writer_version)
            .set_compression(self.config.compression)
            .set_max_row_group_size(self.config.row_group_size)
            .set_data_page_size_limit(self.config.data_page_size);

        if self.config.enable_dictionary {
            builder = builder.set_dictionary_enabled(true);
        }

        builder.build()
    }

    pub fn config(&self) -> &ParquetWriterConfig {
        &self.config
    }
}

/// Streaming Parquet writer for large datasets
pub struct StreamingParquetWriter {
    writer: ArrowWriter<File>,
    path: PathBuf,
    rows_written: usize,
}

impl StreamingParquetWriter {
    pub fn new(
        path: impl Into<PathBuf>,
        schema: Arc<arrow::datatypes::Schema>,
        config: &ParquetWriterConfig,
    ) -> Result<Self> {
        let path = path.into();
        let file = File::create(&path)?;

        let props = WriterProperties::builder()
            .set_writer_version(config.writer_version)
            .set_compression(config.compression)
            .set_max_row_group_size(config.row_group_size)
            .set_data_page_size_limit(config.data_page_size)
            .set_dictionary_enabled(config.enable_dictionary)
            .build();

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        Ok(Self {
            writer,
            path,
            rows_written: 0,
        })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.rows_written += batch.num_rows();
        self.writer.write(batch)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    pub fn close(self) -> Result<PathBuf> {
        self.writer.close()?;
        Ok(self.path)
    }

    pub fn rows_written(&self) -> usize {
        self.rows_written
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs;
    use tempfile::tempdir;

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]));

        let ids: Vec<i64> = (0..num_rows as i64).collect();
        let names: Vec<String> = (0..num_rows).map(|i| format!("user_{}", i)).collect();
        let scores: Vec<f64> = (0..num_rows).map(|i| i as f64 * 1.5).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(scores)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_parquet_writer_config_default() {
        let config = ParquetWriterConfig::default();
        assert_eq!(config.output_dir, PathBuf::from("./output"));
        assert_eq!(config.row_group_size, 1024 * 1024);
        assert_eq!(config.data_page_size, 1024 * 1024);
        assert!(config.enable_dictionary);
    }

    #[test]
    fn test_parquet_writer_config_builder() {
        let config = ParquetWriterConfig::default()
            .with_output_dir("/tmp/test")
            .with_compression(Compression::SNAPPY)
            .with_row_group_size(500_000);

        assert_eq!(config.output_dir, PathBuf::from("/tmp/test"));
        assert_eq!(config.compression, Compression::SNAPPY);
        assert_eq!(config.row_group_size, 500_000);
    }

    #[test]
    fn test_parquet_file_writer_creates_directory() {
        let temp_dir = tempdir().unwrap();
        let output_dir = temp_dir.path().join("nested/output/dir");

        let config = ParquetWriterConfig::default().with_output_dir(&output_dir);
        let _writer = ParquetFileWriter::new(config).unwrap();

        assert!(output_dir.exists());
    }

    #[test]
    fn test_write_single_batch() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let writer = ParquetFileWriter::new(config).unwrap();

        let batch = create_test_batch(100);
        let path = writer.write_batch(&batch, "test.parquet").unwrap();

        assert!(path.exists());
        assert_eq!(path.file_name().unwrap(), "test.parquet");

        // Verify file can be read back
        let file = fs::File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);
    }

    #[test]
    fn test_write_batch_to_specific_path() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let writer = ParquetFileWriter::new(config).unwrap();

        let batch = create_test_batch(50);
        let custom_path = temp_dir.path().join("custom/path/data.parquet");
        fs::create_dir_all(custom_path.parent().unwrap()).unwrap();

        writer.write_batch_to_path(&batch, &custom_path).unwrap();

        assert!(custom_path.exists());
    }

    #[test]
    fn test_write_multiple_batches() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let writer = ParquetFileWriter::new(config).unwrap();

        let batches: Vec<RecordBatch> = (0..5).map(|_| create_test_batch(100)).collect();
        let path = writer.write_batches(&batches, "multi.parquet").unwrap();

        assert!(path.exists());

        // Verify all rows were written
        let file = fs::File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let read_batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let total_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 500);
    }

    #[test]
    fn test_write_empty_batches_error() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let writer = ParquetFileWriter::new(config).unwrap();

        let result = writer.write_batches(&[], "empty.parquet");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No batches to write"));
    }

    #[test]
    fn test_streaming_writer() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let path = temp_dir.path().join("streaming.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]));

        let mut writer = StreamingParquetWriter::new(&path, schema, &config).unwrap();

        // Write multiple batches
        for _ in 0..10 {
            let batch = create_test_batch(100);
            writer.write_batch(&batch).unwrap();
        }

        assert_eq!(writer.rows_written(), 1000);
        assert_eq!(writer.path(), path.as_path());

        let final_path = writer.close().unwrap();
        assert!(final_path.exists());

        // Verify content
        let file = fs::File::open(&final_path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1000);
    }

    #[test]
    fn test_streaming_writer_flush() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let path = temp_dir.path().join("flush_test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let mut writer = StreamingParquetWriter::new(&path, schema, &config).unwrap();

        let batch = create_test_batch(50);
        writer.write_batch(&batch).unwrap();
        writer.flush().unwrap();

        // Should still be able to write more after flush
        writer.write_batch(&batch).unwrap();
        writer.close().unwrap();

        assert!(path.exists());
    }

    #[test]
    fn test_compression_options() {
        let temp_dir = tempdir().unwrap();

        // Test with different compression types
        let compressions = vec![
            Compression::UNCOMPRESSED,
            Compression::SNAPPY,
            Compression::ZSTD(Default::default()),
            Compression::LZ4,
        ];

        for (i, compression) in compressions.into_iter().enumerate() {
            let config = ParquetWriterConfig::default()
                .with_output_dir(temp_dir.path())
                .with_compression(compression);
            let writer = ParquetFileWriter::new(config).unwrap();

            let batch = create_test_batch(100);
            let filename = format!("compressed_{}.parquet", i);
            let path = writer.write_batch(&batch, &filename).unwrap();

            assert!(path.exists());

            // Verify file can be read
            let file = fs::File::open(&path).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();
            let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
            assert!(!batches.is_empty());
        }
    }

    #[test]
    fn test_config_accessor() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default()
            .with_output_dir(temp_dir.path())
            .with_row_group_size(12345);

        let writer = ParquetFileWriter::new(config).unwrap();
        assert_eq!(writer.config().row_group_size, 12345);
        assert_eq!(writer.config().output_dir, temp_dir.path());
    }

    #[test]
    fn test_large_batch() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default()
            .with_output_dir(temp_dir.path())
            .with_row_group_size(10_000);
        let writer = ParquetFileWriter::new(config).unwrap();

        // Write a large batch that will span multiple row groups
        let batch = create_test_batch(50_000);
        let path = writer.write_batch(&batch, "large.parquet").unwrap();

        assert!(path.exists());

        let file = fs::File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 50_000);
    }
}
