use crate::config::IngestConfig;
use crate::errors::IngestionError;
use crate::sink::Sink;
use crate::source::{CheckpointHandle, Source};
use crate::telemetry::IngestMetrics;
use anyhow::Result;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::sync::Mutex;

/// The ingestor orchestrates the data flow from source -> sink.
pub struct Ingestor {
    source: Arc<dyn Source>,
    sink: Arc<dyn Sink>,
    metrics: IngestMetrics,
    schema: Arc<ArrowSchema>,
    uncommitted_handles: Mutex<Vec<CheckpointHandle>>,
}

impl Ingestor {
    pub fn new(source: Arc<dyn Source>, sink: Arc<dyn Sink>, schema: Arc<ArrowSchema>) -> Self {
        Self {
            source: source.clone(),
            sink: sink.clone(),
            metrics: IngestMetrics::default(),
            schema,
            uncommitted_handles: Mutex::new(Vec::new()),
        }
    }

    pub fn from_config(ingest_config: IngestConfig) -> Result<Self> {
        todo!()
    }

    /// Runs a single poll-process-flush-commit cycle.
    /// This is the main unit of work to be called by a runner.
    ///
    /// High-level logic:
    /// 1. Fetch a batch of messages from the source.
    /// 2. Parse the messages into a RecordBatch.
    /// 3. Add the RecordBatch to the IngestionSink's buffer.
    /// 4. If the sink flushed a file, commit the corresponding message handles to the source.
    /// 5. If the sink did not flush, do NOT commit, as the data is only in memory.
    pub async fn run_once(&self) -> Result<(), IngestionError> {
        let message_batch = self.source.next_batch().await?;

        if message_batch.messages.is_empty() {
            return Ok(());
        }

        // 1. Collect JSON payloads into a single buffer
        let mut json_payloads = Vec::new();
        for msg in &message_batch.messages {
            json_payloads.extend_from_slice(&msg.payload);
            json_payloads.push(b'\n');
        }

        // 2. Parse newline-delimited JSON to RecordBatch(es)
        let cursor = std::io::Cursor::new(json_payloads);
        let reader = arrow::json::reader::ReaderBuilder::new(self.schema.clone()).build(cursor)?;
        let record_batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;

        if record_batches.iter().all(|b| b.num_rows() == 0) {
            // Still commit even if there are no records, to advance past empty/unparseable messages.
            self.source.commit(&message_batch.handles).await?;
            return Ok(());
        }

        // 3. Add to sink
        for record_batch in record_batches {
            if record_batch.num_rows() > 0 {
                self.sink.add(record_batch).await?;
            }
        }

        // 4. Store handles for later commit in shutdown.
        // The logic for committing after a sink flushes in `run_once` is deferred,
        // as the current `Sink` trait doesn't provide feedback on flushes from `add`.
        let mut handles_guard = self.uncommitted_handles.lock().await;
        handles_guard.extend(message_batch.handles);

        Ok(())
    }

    /// Flushes any remaining data in the sink's buffer and commits their
    /// corresponding offsets. This MUST be called during a graceful shutdown.
    ///
    /// High-level:
    /// 1. Flush the sink.
    /// 2. If successful, find the corresponding handles for the flushed messages.
    /// 3. Commit with source those handles.
    /// 4. Shutdown source.
    pub async fn shutdown(&self) -> Result<(), IngestionError> {
        // 1. Flush the sink
        self.sink.flush().await?;

        // 2. Commit handles for all flushed messages
        let mut handles_guard = self.uncommitted_handles.lock().await;
        if !handles_guard.is_empty() {
            self.source.commit(&handles_guard).await?;
            handles_guard.clear();
        }

        // 4. Shutdown source
        self.source.shutdown().await?;

        Ok(())
    }

    pub fn get_metrics(&self) -> &IngestMetrics {
        &self.metrics
    }
}
