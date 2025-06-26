pub mod kafka;
mod kinesis_stream;

use std::collections::HashMap;
use std::error::Error;
use crate::errors::SourceError;

/// A message received from source regardless of whether it's Kafka, Kinesis Stream, Pulsar, etc.
pub struct SourceMessage {
    /// raw payload of message e.g. kafka message body
    pub payload: Vec<u8>,
    /// Topic or stream name of this message
    pub topic: String,
    /// partition or shard ID
    pub partition: usize,
    /// Optional key/value headers for metadata.
    pub headers: HashMap<String, Vec<u8>>,
}

pub struct MessageBatch {
    messages: Vec<SourceMessage>,
}

/// An opaque handle used to commit a message's progress.
/// For example: offset for Kafka, sequence number for Kinesis Stream, etc.
pub type CheckpointHandle = Vec<u8>;

trait Source {
    /// Receives a batch of messages from the source.
    ///
    /// This method should block until messages are available or a timeout occurs.
    /// An empty `MessageBatch` indicates that the poll timed out without new messages.
    async fn next_batch(&self) -> Result<MessageBatch, SourceError>;

    /// Commits the progress for a set of messages.
    async fn commit(&self, handles: &[CheckpointHandle]) -> Result<(), SourceError>;

    /// Closes the connection to the source and cleans up resources.
    /// This should be called during graceful shutdown.
    async fn shutdown(&self) -> Result<(), SourceError>;
}
