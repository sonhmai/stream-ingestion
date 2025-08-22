use crate::errors::SourceError;
use crate::source::{CheckpointHandle, MessageBatch, Source};
use async_trait::async_trait;

pub struct KinesisStreamSource {}

#[async_trait]
impl Source for KinesisStreamSource {
    async fn next_batch(&self) -> Result<MessageBatch, SourceError> {
        todo!()
    }

    async fn commit(&self, handles: &[CheckpointHandle]) -> Result<(), SourceError> {
        todo!()
    }

    async fn shutdown(&self) -> Result<(), SourceError> {
        todo!()
    }
}