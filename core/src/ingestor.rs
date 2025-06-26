use crate::config::IngestConfig;
use crate::errors;
use crate::errors::IngestionError;
use crate::sink::delta::DeltaSink;
use crate::source::kafka::KafkaConsumerClient;
use crate::telemetry::IngestMetrics;
use anyhow::Result;

/// The ingestor orchestrates the data flow from source -> sink.
pub struct Ingestor {
    config: IngestConfig,
    kafka_client: KafkaConsumerClient,
    delta_sink: DeltaSink,
    metrics: IngestMetrics,
}

impl Ingestor {
    pub async fn new(config: IngestConfig) -> errors::Result<Self> {
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
        todo!()
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
        todo!()
    }

    pub fn get_metrics(&self) -> &IngestMetrics {
        &self.metrics
    }
}
