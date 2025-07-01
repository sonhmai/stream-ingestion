use std::error::Error;
use crate::errors::SourceError;
use crate::source::{CheckpointHandle, MessageBatch, Source};

pub struct KinesisStreamSource {}

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