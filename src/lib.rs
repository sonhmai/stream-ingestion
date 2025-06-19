pub mod config;
// pub mod kafka;  // Temporarily disabled for testing
// pub mod delta;  // Temporarily disabled for testing
pub mod s3;
pub mod errors;

pub use config::IngestConfig;
// pub use kafka::{KafkaConsumerClient, KafkaMessage, parse_json_message, extract_partition_values};  // Temporarily disabled for testing
// pub use delta::DeltaWriter;  // Temporarily disabled for testing
pub use s3::S3Client;
pub use errors::{StreamIngestError, Result};