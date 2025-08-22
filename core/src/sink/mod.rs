use anyhow::Result;
use async_trait::async_trait;
use arrow::record_batch::RecordBatch;

pub mod delta;

/// The `DataSink` trait defines the contract for any destination system.
/// It is responsible for taking a batch of data in Arrow format and persisting it.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Add a micro-batch to the sink
    async fn add(&self, record_batch: RecordBatch) -> Result<()>;

    /// Flush the sink, make sure all buffered messages are flushed
    async fn flush(&self) -> Result<()>;
}