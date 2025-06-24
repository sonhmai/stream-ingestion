pub mod config;
pub mod delta;
pub mod errors;
pub mod kafka;
pub mod s3; // Re-enabled for testing

pub use config::IngestConfig;
pub use kafka::{KafkaConsumerClient, KafkaMessage, extract_partition_values, parse_json_message};
// pub use delta::DeltaWriter;  // Temporarily disabled for testing
pub use errors::{Result, StreamIngestError};
pub use s3::S3Client; // Re-enabled for testing
