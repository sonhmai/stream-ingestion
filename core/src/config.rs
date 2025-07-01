use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IngestConfig {
    pub version: u32,
    pub compute: ComputeConfig,
    pub streams: Vec<StreamConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ComputeConfig {
    pub backend: String,
    pub options: ComputeOptions,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ComputeOptions {
    pub lambda_name: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamConfig {
    pub name: String,
    pub source: SourceConfig,
    pub processing: ProcessingConfig,
    pub sink: SinkConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub source_type: String,
    pub options: KafkaOptions,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KafkaOptions {
    pub bootstrap_servers: String,
    pub topic: String,
    pub consumer_group: String,
    pub security_protocol: Option<String>,
    pub sasl_mechanisms: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_location: Option<String>,
    pub auto_offset_reset: Option<String>,
    pub session_timeout_ms: Option<u32>,
    pub heartbeat_interval_ms: Option<u32>,
    pub max_poll_records: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SinkConfig {
    #[serde(rename = "type")]
    pub target_type: String,
    pub options: S3Options,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct S3Options {
    pub format: String,
    pub path: String,
    pub region: String,
    pub database_name: String,
    pub catalog_name: String,
    pub table_name: String,
    pub write_mode: WriteMode,
    pub merge_schema: bool,
    pub overwrite_schema: bool,
    pub partition_columns: Option<Vec<String>>,
    pub table_schema: Vec<SchemaField>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SchemaField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Date32,
    Date64,
    Time32Millisecond,
    Time64Microsecond,
    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
    #[serde(alias = "int")]
    Int,
    #[serde(alias = "long")]
    Long,
    #[serde(alias = "timestamp")]
    Timestamp,
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    List(Box<DataType>),
    Struct(Vec<SchemaField>),
    Map {
        key: Box<DataType>,
        value: Box<DataType>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Append,
    Overwrite,
    ErrorIfExists,
    Ignore,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProcessingConfig {
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub max_retries: u32,
    pub retry_delay_ms: u64,
    pub dead_letter_topic: Option<String>,
    pub enable_compression: bool,
    pub compression_type: Option<CompressionType>,
    pub enable_metrics: bool,
    pub checkpoint_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionType {
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Default for ProcessingConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_timeout_ms: 30000,
            max_retries: 3,
            retry_delay_ms: 1000,
            dead_letter_topic: None,
            enable_compression: true,
            compression_type: Some(CompressionType::Snappy),
            enable_metrics: true,
            checkpoint_interval_ms: Some(60000),
        }
    }
}

pub async fn load_config(config_path: Option<&str>) -> anyhow::Result<IngestConfig> {
    match config_path {
        Some(path) => IngestConfig::from_file(path),
        None => IngestConfig::from_env(),
    }
}

impl IngestConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: IngestConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let config_str = std::env::var("INGEST_CONFIG")
            .map_err(|_| anyhow::anyhow!("INGEST_CONFIG environment variable not set"))?;
        let config: IngestConfig = serde_yaml::from_str(&config_str)?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.streams.is_empty() {
            return Err(anyhow::anyhow!("At least one stream must be configured"));
        }
        
        for stream in &self.streams {
            if stream.source.options.bootstrap_servers.is_empty() {
                return Err(anyhow::anyhow!("Kafka bootstrap servers cannot be empty"));
            }
            if stream.source.options.topic.is_empty() {
                return Err(anyhow::anyhow!("Kafka topic cannot be empty"));
            }
            if stream.sink.options.table_name.is_empty() {
                return Err(anyhow::anyhow!("Table name cannot be empty"));
            }
            if stream.sink.options.table_schema.is_empty() {
                return Err(anyhow::anyhow!("Table schema cannot be empty"));
            }
        }
        Ok(())
    }

    pub fn get_first_stream(&self) -> Option<&StreamConfig> {
        self.streams.first()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_actual_config_file() {
        // let config = IngestConfig::from_file("configs/kafka_sasl_to_s3.yaml").unwrap();
        //
        // assert_eq!(config.version, 1);
        // assert_eq!(config.compute.backend, "lambda");
        // assert_eq!(config.streams.len(), 1);
        //
        // let stream = &config.streams[0];
        // assert_eq!(stream.name, "source1-schema1-customer");
        // assert_eq!(stream.source.source_type, "kafka");
        // assert_eq!(stream.source.options.topic, "financial-transactions");
        // assert_eq!(stream.sink.target_type, "s3");
        // assert_eq!(stream.sink.options.table_name, "source1-schema1-customer");
        // assert_eq!(stream.sink.options.table_schema.len(), 6);
        //
        // config.validate().unwrap();
    }
}

