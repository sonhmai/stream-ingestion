use crate::errors::SourceError;
use crate::source::{CheckpointHandle, MessageBatch, Source, SourceMessage};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

/// In-memory implementation of the Source trait.
///
/// Useful for tests and local development where messages are supplied
/// programmatically and consumed in bounded batches.
pub struct MemorySource {
    messages_queue: Mutex<VecDeque<SourceMessage>>,
    batch_size: usize,
    next_sequence_number: AtomicU64,
}

impl MemorySource {
    /// Creates a new MemorySource with an initial set of messages and a batch size.
    pub fn new(initial_messages: Vec<SourceMessage>, batch_size: usize) -> Self {
        let effective_batch_size = if batch_size == 0 { 1 } else { batch_size };
        Self {
            messages_queue: Mutex::new(initial_messages.into()),
            batch_size: effective_batch_size,
            next_sequence_number: AtomicU64::new(0),
        }
    }

    /// Creates an empty MemorySource with the provided batch size.
    pub fn with_capacity(batch_size: usize) -> Self {
        Self::new(Vec::new(), batch_size)
    }

    /// Appends a single message to the in-memory queue.
    pub async fn push_message(&self, message: SourceMessage) {
        let mut queue_guard = self.messages_queue.lock().await;
        queue_guard.push_back(message);
    }
}

impl Source for MemorySource {
    async fn next_batch(&self) -> Result<MessageBatch, SourceError> {
        let mut queue_guard = self.messages_queue.lock().await;

        if queue_guard.is_empty() {
            return Ok(MessageBatch {
                messages: Vec::new(),
                handles: Vec::new(),
            });
        }

        let mut drained_messages: Vec<SourceMessage> = Vec::with_capacity(self.batch_size);
        let mut checkpoint_handles: Vec<CheckpointHandle> =
            Vec::with_capacity(self.batch_size);

        for _ in 0..self.batch_size {
            if let Some(message) = queue_guard.pop_front() {
                let seq = self.next_sequence_number.fetch_add(1, Ordering::Relaxed);
                let handle = seq.to_le_bytes().to_vec();
                drained_messages.push(message);
                checkpoint_handles.push(handle);
            } else {
                break;
            }
        }

        Ok(MessageBatch {
            messages: drained_messages,
            handles: checkpoint_handles,
        })
    }

    async fn commit(&self, _handles: &[CheckpointHandle]) -> Result<(), SourceError> {
        // No-op for in-memory source.
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), SourceError> {
        // Nothing to clean up for in-memory source.
        Ok(())
    }
}


