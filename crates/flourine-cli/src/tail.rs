//! Tail command — live-tail a topic via ratatui TUI or JSON output.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use apache_avro::Schema as AvroSchema;
use apache_avro::from_avro_datum;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use serde::Serialize;
use tokio::sync::{Mutex, mpsc};
use flourine_common::ids::{Offset, PartitionId, SchemaId, TopicId};
use flourine_sdk::reader::{Reader, ReaderConfig};

use crate::client::FlourineClient;

const MAX_RECORDS: usize = 10_000;

/// Output format for the tail command.
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Tui,
    Json,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Tui => write!(f, "tui"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
}

/// A record ready for display or serialization.
#[derive(Serialize)]
pub struct TailRecord {
    pub partition: u32,
    pub offset: u64,
    pub key: Option<String>,
    pub value: String,
}

/// Parse "partition:offset,partition:offset,..." into a map.
pub fn parse_offsets(s: &str) -> Result<HashMap<u32, u64>> {
    let mut map = HashMap::new();
    for pair in s.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (p, o) = pair.split_once(':').ok_or_else(|| {
            anyhow::anyhow!("invalid offset format '{}', expected partition:offset", pair)
        })?;
        let partition: u32 = p.parse().map_err(|_| anyhow::anyhow!("invalid partition '{}'", p))?;
        let offset: u64 = o.parse().map_err(|_| anyhow::anyhow!("invalid offset '{}'", o))?;
        map.insert(partition, offset);
    }
    if map.is_empty() {
        bail!("no offsets specified");
    }
    Ok(map)
}

/// Cached schema decoder — fetches schemas from the admin API on first use.
struct SchemaCache {
    client: FlourineClient,
    schemas: HashMap<u32, AvroSchema>,
}

impl SchemaCache {
    fn new(client: FlourineClient) -> Self {
        Self {
            client,
            schemas: HashMap::new(),
        }
    }

    async fn decode(&mut self, schema_id: SchemaId, bytes: &[u8]) -> String {
        let sid = schema_id.0;
        // Try Avro decode if we have or can fetch the schema
        if let Some(schema) = self.get_or_fetch(sid).await {
            if let Ok(value) = from_avro_datum(&schema, &mut &*bytes, None) {
                if let Ok(json) = serde_json::Value::try_from(value) {
                    return serde_json::to_string(&json).unwrap_or_else(|_| format!("{json:?}"));
                }
            }
        }
        // Fallback: show as UTF-8 string (for pre-Avro records or decode failures)
        String::from_utf8_lossy(bytes).into_owned()
    }

    async fn get_or_fetch(&mut self, schema_id: u32) -> Option<&AvroSchema> {
        if !self.schemas.contains_key(&schema_id) {
            if let Ok(resp) = self.client.get_schema(schema_id).await {
                let json = serde_json::to_string(&resp.schema).ok()?;
                if let Ok(schema) = AvroSchema::parse_str(&json) {
                    self.schemas.insert(schema_id, schema);
                }
            }
        }
        self.schemas.get(&schema_id)
    }
}

pub async fn run(
    ws_url: &str,
    api_key: Option<&str>,
    client: &FlourineClient,
    topic_id: u32,
    start: Option<&str>,
    end: Option<&str>,
    output: OutputFormat,
) -> Result<()> {
    let start_offsets = start.map(parse_offsets).transpose()?;
    let end_offsets = end.map(parse_offsets).transpose()?;

    let (tx, rx) = mpsc::channel::<TailRecord>(1024);

    let group_id = format!("flourine-cli-tail-{}", uuid::Uuid::new_v4());
    let config = ReaderConfig {
        url: ws_url.to_string(),
        api_key: api_key.map(str::to_string),
        group_id,
        topic_id: TopicId(topic_id),
        ..Default::default()
    };

    let reader = Reader::join(config).await?;
    let _heartbeat = reader.start_heartbeat();

    // Apply start offsets if specified
    if let Some(ref starts) = start_offsets {
        for (&partition, &offset) in starts {
            reader.seek(PartitionId(partition), Offset(offset)).await;
        }
    }

    let cache = Arc::new(Mutex::new(SchemaCache::new(client.clone())));

    // Spawn reader task
    let reader_clone = reader.clone();
    let end_clone = end_offsets.clone();
    let cache_clone = cache.clone();
    tokio::spawn(async move {
        poll_loop_with_cache(reader_clone, tx, end_clone, Some(cache_clone)).await;
    });

    let result = match output {
        OutputFormat::Tui => run_tui(rx, topic_id).await,
        OutputFormat::Json => run_json(rx).await,
    };

    let _ = reader.stop().await;
    result
}

#[allow(dead_code)] // used via lib crate in integration tests
pub async fn poll_loop(
    reader: Arc<Reader>,
    tx: mpsc::Sender<TailRecord>,
    end_offsets: Option<HashMap<u32, u64>>,
) {
    poll_loop_with_cache(reader, tx, end_offsets, None).await;
}

async fn poll_loop_with_cache(
    reader: Arc<Reader>,
    tx: mpsc::Sender<TailRecord>,
    end_offsets: Option<HashMap<u32, u64>>,
    schema_cache: Option<Arc<Mutex<SchemaCache>>>,
) {
    // Track which partitions have reached their end offset
    let mut finished_partitions: HashSet<u32> = HashSet::new();

    loop {
        match reader.poll().await {
            Ok(results) => {
                for result in results {
                    let pid = result.partition_id.0;
                    let schema_id = result.schema_id;

                    // Skip partitions that have reached their end
                    if finished_partitions.contains(&pid) {
                        continue;
                    }

                    let base_offset = result
                        .high_watermark
                        .0
                        .saturating_sub(result.records.len() as u64);

                    let record_count = result.records.len() as u64;

                    for (i, record) in result.records.into_iter().enumerate() {
                        let offset = base_offset + i as u64;

                        // Check end offset
                        if let Some(ref ends) = end_offsets {
                            if let Some(&end_off) = ends.get(&pid) {
                                if offset >= end_off {
                                    finished_partitions.insert(pid);
                                    break;
                                }
                            }
                        }

                        let key = record
                            .key
                            .as_ref()
                            .map(|b| String::from_utf8_lossy(b).into_owned());
                        let value = match &schema_cache {
                            Some(cache) => {
                                cache.lock().await.decode(schema_id, &record.value).await
                            }
                            None => String::from_utf8_lossy(&record.value).into_owned(),
                        };

                        if tx
                            .send(TailRecord {
                                partition: pid,
                                offset,
                                key,
                                value,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }

                    // Also mark finished if high_watermark reached end (no more data)
                    if let Some(ref ends) = end_offsets {
                        if let Some(&end_off) = ends.get(&pid) {
                            let next_offset = base_offset + record_count;
                            if next_offset >= end_off {
                                finished_partitions.insert(pid);
                            }
                        }
                    }

                    // Check if all end-offset partitions are finished
                    if let Some(ref ends) = end_offsets {
                        if ends.keys().all(|p| finished_partitions.contains(p)) {
                            return;
                        }
                    }
                }
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if tx.is_closed() {
                    return;
                }
            }
        }
    }
}

// ============ JSON output ============

async fn run_json(mut rx: mpsc::Receiver<TailRecord>) -> Result<()> {
    while let Some(record) = rx.recv().await {
        println!("{}", serde_json::to_string(&record)?);
    }
    Ok(())
}

// ============ TUI output ============

struct TuiApp {
    records: Vec<TailRecord>,
    topic_id: u32,
}

impl TuiApp {
    fn push(&mut self, record: TailRecord) {
        self.records.push(record);
        if self.records.len() > MAX_RECORDS {
            self.records.drain(..self.records.len() - MAX_RECORDS);
        }
    }

    fn unique_partitions(&self) -> usize {
        let mut seen = HashSet::new();
        for r in &self.records {
            seen.insert(r.partition);
        }
        seen.len()
    }
}

fn partition_color(pid: u32) -> Color {
    const COLORS: [Color; 8] = [
        Color::Cyan,
        Color::Green,
        Color::Yellow,
        Color::Magenta,
        Color::Blue,
        Color::Red,
        Color::LightCyan,
        Color::LightGreen,
    ];
    COLORS[(pid as usize) % COLORS.len()]
}

async fn run_tui(mut rx: mpsc::Receiver<TailRecord>, topic_id: u32) -> Result<()> {
    terminal::enable_raw_mode()?;
    io::stdout().execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let mut app = TuiApp {
        records: Vec::new(),
        topic_id,
    };

    loop {
        while let Ok(record) = rx.try_recv() {
            app.push(record);
        }

        terminal.draw(|f| render(f, &app))?;

        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press
                    && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                {
                    break;
                }
            }
        }
    }

    terminal::disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}

fn render(f: &mut Frame, app: &TuiApp) {
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(1)])
        .split(area);

    let rows: Vec<Row> = app
        .records
        .iter()
        .map(|r| {
            let color = partition_color(r.partition);
            Row::new(vec![
                Cell::from(format!("P{}", r.partition)).style(Style::default().fg(color)),
                Cell::from(r.offset.to_string()),
                Cell::from(r.key.as_deref().unwrap_or("")),
                Cell::from(r.value.as_str()),
            ])
        })
        .collect();

    let total = rows.len();
    let table_height = chunks[0].height.saturating_sub(3) as usize;
    let skip = rows.len().saturating_sub(table_height);
    let visible_rows: Vec<Row> = rows.into_iter().skip(skip).collect();

    let header = Row::new(vec![
        Cell::from("Partition").style(Style::default().bold()),
        Cell::from("Offset").style(Style::default().bold()),
        Cell::from("Key").style(Style::default().bold()),
        Cell::from("Value").style(Style::default().bold()),
    ]);

    let widths = [
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(20),
        Constraint::Fill(1),
    ];

    let title = format!(" Flourine Tail: topic {} ", app.topic_id);
    let table = Table::new(visible_rows, widths)
        .header(header)
        .block(Block::default().title(title).borders(Borders::ALL));

    f.render_widget(table, chunks[0]);

    let status = format!(
        " Records: {} | Partitions: {} | Press q to quit ",
        total,
        app.unique_partitions(),
    );
    let status_bar = Paragraph::new(status).style(Style::default().fg(Color::DarkGray));
    f.render_widget(status_bar, chunks[1]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_offsets_single() {
        let offsets = parse_offsets("0:100").unwrap();
        assert_eq!(offsets[&0], 100);
    }

    #[test]
    fn test_parse_offsets_multiple() {
        let offsets = parse_offsets("0:100,1:200,2:50").unwrap();
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets[&0], 100);
        assert_eq!(offsets[&1], 200);
        assert_eq!(offsets[&2], 50);
    }

    #[test]
    fn test_parse_offsets_with_spaces() {
        let offsets = parse_offsets("0:100, 1:200").unwrap();
        assert_eq!(offsets.len(), 2);
    }

    #[test]
    fn test_parse_offsets_invalid_format() {
        assert!(parse_offsets("0-100").is_err());
    }

    #[test]
    fn test_parse_offsets_empty() {
        assert!(parse_offsets("").is_err());
    }

    #[test]
    fn test_parse_offsets_invalid_number() {
        assert!(parse_offsets("abc:100").is_err());
        assert!(parse_offsets("0:abc").is_err());
    }
}
