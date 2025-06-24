use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IngestConfig {
    pub kafka: KafkaConfig,
    pub s3: S3Config,
    pub delta: DeltaConfig,
    pub processing: ProcessingConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KafkaConfig {
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
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub endpoint_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeltaConfig {
    pub table_name: String,
    pub schema: Vec<SchemaField>,
    pub partition_columns: Option<Vec<String>>,
    pub write_mode: WriteMode,
    pub merge_schema: bool,
    pub overwrite_schema: bool,
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
        if self.kafka.bootstrap_servers.is_empty() {
            return Err(anyhow::anyhow!("Kafka bootstrap servers cannot be empty"));
        }
        if self.kafka.topic.is_empty() {
            return Err(anyhow::anyhow!("Kafka topic cannot be empty"));
        }
        if self.s3.bucket.is_empty() {
            return Err(anyhow::anyhow!("S3 bucket cannot be empty"));
        }
        if self.delta.table_name.is_empty() {
            return Err(anyhow::anyhow!("Delta table name cannot be empty"));
        }
        if self.delta.schema.is_empty() {
            return Err(anyhow::anyhow!("Delta schema cannot be empty"));
        }
        Ok(())
    }

    pub fn get_delta_table_path(&self) -> String {
        format!(
            "s3a://{}/{}/{}",
            self.s3.bucket,
            self.s3.prefix.trim_end_matches('/'),
            self.delta.table_name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_config() -> IngestConfig {
        IngestConfig {
            kafka: KafkaConfig {
                bootstrap_servers: "localhost:9092".to_string(),
                topic: "test-topic".to_string(),
                consumer_group: "test-group".to_string(),
                security_protocol: Some("PLAINTEXT".to_string()),
                sasl_mechanisms: None,
                sasl_username: None,
                sasl_password: None,
                ssl_ca_location: None,
                auto_offset_reset: Some("earliest".to_string()),
                session_timeout_ms: Some(30000),
                heartbeat_interval_ms: Some(3000),
                max_poll_records: Some(1000),
            },
            s3: S3Config {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                prefix: "test-prefix".to_string(),
                access_key_id: None,
                secret_access_key: None,
                session_token: None,
                endpoint_url: None,
            },
            delta: DeltaConfig {
                table_name: "test_table".to_string(),
                schema: vec![
                    SchemaField {
                        name: "id".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        metadata: None,
                    },
                    SchemaField {
                        name: "name".to_string(),
                        data_type: DataType::Utf8,
                        nullable: true,
                        metadata: None,
                    },
                ],
                partition_columns: Some(vec!["date".to_string()]),
                write_mode: WriteMode::Append,
                merge_schema: false,
                overwrite_schema: false,
            },
            processing: ProcessingConfig::default(),
        }
    }

    #[test]
    fn test_config_validation_success() {
        let config = create_test_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_empty_bootstrap_servers() {
        let mut config = create_test_config();
        config.kafka.bootstrap_servers = "".to_string();

        let result = config.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("bootstrap servers cannot be empty")
        );
    }

    #[test]
    fn test_config_validation_empty_topic() {
        let mut config = create_test_config();
        config.kafka.topic = "".to_string();

        let result = config.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("topic cannot be empty")
        );
    }

    #[test]
    fn test_config_validation_empty_bucket() {
        let mut config = create_test_config();
        config.s3.bucket = "".to_string();

        let result = config.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("bucket cannot be empty")
        );
    }

    #[test]
    fn test_config_validation_empty_table_name() {
        let mut config = create_test_config();
        config.delta.table_name = "".to_string();

        let result = config.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("table name cannot be empty")
        );
    }

    #[test]
    fn test_config_validation_empty_schema() {
        let mut config = create_test_config();
        config.delta.schema = vec![];

        let result = config.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("schema cannot be empty")
        );
    }

    #[test]
    fn test_get_delta_table_path() {
        let config = create_test_config();
        let path = config.get_delta_table_path();
        assert_eq!(path, "s3a://test-bucket/test-prefix/test_table");
    }

    #[test]
    fn test_get_delta_table_path_with_trailing_slash() {
        let mut config = create_test_config();
        config.s3.prefix = "test-prefix/".to_string();

        let path = config.get_delta_table_path();
        assert_eq!(path, "s3a://test-bucket/test-prefix/test_table");
    }

    #[test]
    fn test_config_from_yaml_file() {
        let yaml_content = r#"
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "test-topic"
  consumer_group: "test-group"
  security_protocol: "PLAINTEXT"
  auto_offset_reset: "earliest"
  session_timeout_ms: 30000
  heartbeat_interval_ms: 3000
  max_poll_records: 1000

s3:
  bucket: "test-bucket"
  region: "us-east-1"
  prefix: "test-prefix"

delta:
  table_name: "test_table"
  write_mode: "append"
  merge_schema: false
  overwrite_schema: false
  partition_columns:
    - "date"
  schema:
    - name: "id"
      data_type: "int64"
      nullable: false
    - name: "name"
      data_type: "utf8"
      nullable: true

processing:
  batch_size: 1000
  batch_timeout_ms: 30000
  max_retries: 3
  retry_delay_ms: 1000
  enable_compression: true
  compression_type: "snappy"
  enable_metrics: true
  checkpoint_interval_ms: 60000
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();

        let config = IngestConfig::from_file(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(config.kafka.bootstrap_servers, "localhost:9092");
        assert_eq!(config.kafka.topic, "test-topic");
        assert_eq!(config.s3.bucket, "test-bucket");
        assert_eq!(config.delta.table_name, "test_table");
        assert_eq!(config.delta.schema.len(), 2);
        assert_eq!(config.processing.batch_size, 1000);
    }

    #[test]
    fn test_config_from_env() {
        let yaml_content = r#"
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "env-topic"
  consumer_group: "env-group"

s3:
  bucket: "env-bucket"
  region: "us-west-2"
  prefix: "env-prefix"

delta:
  table_name: "env_table"
  write_mode: "append"
  merge_schema: false
  overwrite_schema: false
  schema:
    - name: "test_field"
      data_type: "utf8"
      nullable: true

processing:
  batch_size: 500
  batch_timeout_ms: 15000
  max_retries: 2
  retry_delay_ms: 2000
  enable_compression: false
  enable_metrics: true
"#;

        // Clean up before setting
        unsafe {
            std::env::remove_var("INGEST_CONFIG");
        }
        unsafe {
            std::env::set_var("INGEST_CONFIG", yaml_content);
        }

        let config = IngestConfig::from_env().unwrap();

        assert_eq!(config.kafka.topic, "env-topic");
        assert_eq!(config.s3.bucket, "env-bucket");
        assert_eq!(config.processing.batch_size, 500);

        // Clean up after test
        unsafe {
            std::env::remove_var("INGEST_CONFIG");
        }
    }

    #[test]
    fn test_config_from_env_missing() {
        unsafe {
            std::env::remove_var("INGEST_CONFIG");
        }

        let result = IngestConfig::from_env();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("INGEST_CONFIG environment variable not set")
        );
    }

    #[test]
    fn test_processing_config_default() {
        let config = ProcessingConfig::default();

        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.batch_timeout_ms, 30000);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay_ms, 1000);
        assert!(config.enable_compression);
        assert!(matches!(
            config.compression_type,
            Some(CompressionType::Snappy)
        ));
        assert!(config.enable_metrics);
        assert_eq!(config.checkpoint_interval_ms, Some(60000));
    }

    #[test]
    fn test_data_type_serialization() {
        let types = vec![
            DataType::Boolean,
            DataType::Int64,
            DataType::Utf8,
            DataType::TimestampMillisecond,
            DataType::Decimal128 {
                precision: 10,
                scale: 2,
            },
        ];

        for data_type in types {
            let serialized = serde_yaml::to_string(&data_type).unwrap();
            let deserialized: DataType = serde_yaml::from_str(&serialized).unwrap();
            // Just ensure serialization/deserialization works without errors
            let _ = deserialized;
        }
    }

    #[test]
    fn test_write_mode_serialization() {
        let modes = vec![
            WriteMode::Append,
            WriteMode::Overwrite,
            WriteMode::ErrorIfExists,
            WriteMode::Ignore,
        ];

        for mode in modes {
            let serialized = serde_yaml::to_string(&mode).unwrap();
            let deserialized: WriteMode = serde_yaml::from_str(&serialized).unwrap();
            // Just ensure serialization/deserialization works without errors
            let _ = deserialized;
        }
    }
}
