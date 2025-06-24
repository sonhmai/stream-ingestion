use thiserror::Error;

#[derive(Error, Debug)]
pub enum StreamIngestError {
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    #[error("Kafka error: {0}")]
    Kafka(#[from] KafkaError),

    #[error("Delta Lake error: {0}")]
    Delta(#[from] DeltaError),

    #[error("S3 error: {0}")]
    S3(#[from] S3Error),

    #[error("Lambda error: {0}")]
    Lambda(#[from] LambdaError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] SerializationError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Generic error: {0}")]
    Generic(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Invalid configuration: {message}")]
    Invalid { message: String },

    #[error("Missing required field: {field}")]
    MissingField { field: String },

    #[error("Configuration validation failed: {reason}")]
    ValidationFailed { reason: String },

    #[error("Failed to load configuration from {source}: {error}")]
    LoadFailed {
        source: String,
        #[source]
        error: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Error, Debug)]
pub enum KafkaError {
    #[error("Failed to create consumer: {reason}")]
    ConsumerCreation { reason: String },

    #[error("Failed to subscribe to topic {topic}: {reason}")]
    Subscription { topic: String, reason: String },

    #[error("Failed to consume messages: {reason}")]
    Consumption { reason: String },

    #[error("Failed to commit offsets: {reason}")]
    OffsetCommit { reason: String },

    #[error("Message parsing failed: {reason}")]
    MessageParsing { reason: String },

    #[error("Consumer health check failed: {reason}")]
    HealthCheck { reason: String },
}

#[derive(Error, Debug)]
pub enum DeltaError {
    #[error("Failed to create Delta table: {reason}")]
    TableCreation { reason: String },

    #[error("Failed to write to Delta table: {reason}")]
    Write { reason: String },

    #[error("Schema conversion failed: {reason}")]
    SchemaConversion { reason: String },

    #[error("Failed to load Delta table from {path}: {reason}")]
    TableLoad { path: String, reason: String },

    #[error("Table optimization failed: {reason}")]
    Optimization { reason: String },

    #[error("Table vacuum failed: {reason}")]
    Vacuum { reason: String },
}

#[derive(Error, Debug)]
pub enum S3Error {
    #[error("S3 client creation failed: {reason}")]
    ClientCreation { reason: String },

    #[error("Bucket {bucket} is not accessible: {reason}")]
    BucketAccess { bucket: String, reason: String },

    #[error("Failed to list objects in bucket {bucket}: {reason}")]
    ListObjects { bucket: String, reason: String },

    #[error("Failed to upload object to s3://{bucket}/{key}: {reason}")]
    Upload {
        bucket: String,
        key: String,
        reason: String,
    },

    #[error("Failed to download object from s3://{bucket}/{key}: {reason}")]
    Download {
        bucket: String,
        key: String,
        reason: String,
    },

    #[error("Failed to delete object s3://{bucket}/{key}: {reason}")]
    Delete {
        bucket: String,
        key: String,
        reason: String,
    },

    #[error("S3 health check failed: {reason}")]
    HealthCheck { reason: String },
}

#[derive(Error, Debug)]
pub enum LambdaError {
    #[error("Lambda initialization failed: {reason}")]
    Initialization { reason: String },

    #[error("Lambda invocation failed: {reason}")]
    Invocation { reason: String },

    #[error("Lambda timeout exceeded")]
    Timeout,

    #[error("Lambda memory limit exceeded")]
    MemoryLimit,

    #[error("Lambda cold start detected")]
    ColdStart,
}

#[derive(Error, Debug)]
pub enum SerializationError {
    #[error("JSON serialization failed: {reason}")]
    Json { reason: String },

    #[error("YAML serialization failed: {reason}")]
    Yaml { reason: String },

    #[error("Arrow serialization failed: {reason}")]
    Arrow { reason: String },

    #[error("Data type conversion failed from {from_type} to {to_type}: {reason}")]
    TypeConversion {
        from_type: String,
        to_type: String,
        reason: String,
    },
}

pub type Result<T> = std::result::Result<T, StreamIngestError>;

// Temporarily disabled for testing
// impl From<rdkafka::error::KafkaError> for StreamIngestError {
//     fn from(err: rdkafka::error::KafkaError) -> Self {
//         StreamIngestError::Kafka(KafkaError::Consumption {
//             reason: err.to_string(),
//         })
//     }
// }

// impl From<deltalake::errors::DeltaTableError> for StreamIngestError {
//     fn from(err: deltalake::errors::DeltaTableError) -> Self {
//         StreamIngestError::Delta(DeltaError::TableLoad {
//             path: "unknown".to_string(),
//             reason: err.to_string(),
//         })
//     }
// }

impl From<aws_sdk_s3::Error> for StreamIngestError {
    fn from(err: aws_sdk_s3::Error) -> Self {
        StreamIngestError::S3(S3Error::ClientCreation {
            reason: err.to_string(),
        })
    }
}

impl From<serde_json::Error> for StreamIngestError {
    fn from(err: serde_json::Error) -> Self {
        StreamIngestError::Serialization(SerializationError::Json {
            reason: err.to_string(),
        })
    }
}

impl From<serde_yaml::Error> for StreamIngestError {
    fn from(err: serde_yaml::Error) -> Self {
        StreamIngestError::Serialization(SerializationError::Yaml {
            reason: err.to_string(),
        })
    }
}

impl StreamIngestError {
    pub fn is_retryable(&self) -> bool {
        match self {
            StreamIngestError::Kafka(KafkaError::Consumption { .. }) => true,
            StreamIngestError::Kafka(KafkaError::OffsetCommit { .. }) => true,
            StreamIngestError::Delta(DeltaError::Write { .. }) => true,
            StreamIngestError::S3(S3Error::Upload { .. }) => true,
            StreamIngestError::S3(S3Error::Download { .. }) => true,
            StreamIngestError::Lambda(LambdaError::Timeout) => false,
            StreamIngestError::Lambda(LambdaError::MemoryLimit) => false,
            StreamIngestError::Config(_) => false,
            StreamIngestError::Serialization(_) => false,
            _ => true,
        }
    }

    pub fn should_skip_message(&self) -> bool {
        match self {
            StreamIngestError::Serialization(_) => true,
            StreamIngestError::Kafka(KafkaError::MessageParsing { .. }) => true,
            StreamIngestError::Delta(DeltaError::SchemaConversion { .. }) => true,
            _ => false,
        }
    }

    pub fn get_retry_delay_ms(&self) -> u64 {
        match self {
            StreamIngestError::Kafka(_) => 1000,
            StreamIngestError::Delta(_) => 2000,
            StreamIngestError::S3(_) => 3000,
            _ => 5000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_error_display() {
        let error = ConfigError::Invalid {
            message: "Test message".to_string(),
        };
        assert_eq!(error.to_string(), "Invalid configuration: Test message");
    }

    #[test]
    fn test_kafka_error_display() {
        let error = KafkaError::ConsumerCreation {
            reason: "Connection failed".to_string(),
        };
        assert_eq!(
            error.to_string(),
            "Failed to create consumer: Connection failed"
        );
    }

    #[test]
    fn test_delta_error_display() {
        let error = DeltaError::TableCreation {
            reason: "Schema invalid".to_string(),
        };
        assert_eq!(
            error.to_string(),
            "Failed to create Delta table: Schema invalid"
        );
    }

    #[test]
    fn test_s3_error_display() {
        let error = S3Error::BucketAccess {
            bucket: "test-bucket".to_string(),
            reason: "Access denied".to_string(),
        };
        assert_eq!(
            error.to_string(),
            "Bucket test-bucket is not accessible: Access denied"
        );
    }

    #[test]
    fn test_lambda_error_display() {
        let error = LambdaError::Timeout;
        assert_eq!(error.to_string(), "Lambda timeout exceeded");
    }

    #[test]
    fn test_serialization_error_display() {
        let error = SerializationError::Json {
            reason: "Invalid JSON".to_string(),
        };
        assert_eq!(error.to_string(), "JSON serialization failed: Invalid JSON");
    }

    #[test]
    fn test_stream_ingest_error_from_config_error() {
        let config_error = ConfigError::ValidationFailed {
            reason: "Test validation".to_string(),
        };
        let stream_error = StreamIngestError::from(config_error);

        match stream_error {
            StreamIngestError::Config(ConfigError::ValidationFailed { reason }) => {
                assert_eq!(reason, "Test validation");
            }
            _ => panic!("Expected Config error"),
        }
    }

    #[test]
    fn test_stream_ingest_error_from_serde_json_error() {
        let json_str = r#"{"invalid": json}"#;
        let json_error = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
        let stream_error = StreamIngestError::from(json_error);

        match stream_error {
            StreamIngestError::Serialization(SerializationError::Json { .. }) => {}
            _ => panic!("Expected Serialization error"),
        }
    }

    #[test]
    fn test_stream_ingest_error_from_serde_yaml_error() {
        let yaml_str = r#"
invalid yaml content:
  - missing quotes
  - "improper: structure
"#;
        let yaml_error = serde_yaml::from_str::<serde_yaml::Value>(yaml_str).unwrap_err();
        let stream_error = StreamIngestError::from(yaml_error);

        match stream_error {
            StreamIngestError::Serialization(SerializationError::Yaml { .. }) => {}
            _ => panic!("Expected Serialization error"),
        }
    }

    #[test]
    fn test_is_retryable() {
        let retryable_errors = vec![
            StreamIngestError::Kafka(KafkaError::Consumption {
                reason: "test".to_string(),
            }),
            StreamIngestError::Kafka(KafkaError::OffsetCommit {
                reason: "test".to_string(),
            }),
            StreamIngestError::Delta(DeltaError::Write {
                reason: "test".to_string(),
            }),
            StreamIngestError::S3(S3Error::Upload {
                bucket: "test".to_string(),
                key: "test".to_string(),
                reason: "test".to_string(),
            }),
            StreamIngestError::S3(S3Error::Download {
                bucket: "test".to_string(),
                key: "test".to_string(),
                reason: "test".to_string(),
            }),
        ];

        for error in retryable_errors {
            assert!(
                error.is_retryable(),
                "Error should be retryable: {:?}",
                error
            );
        }

        let non_retryable_errors = vec![
            StreamIngestError::Lambda(LambdaError::Timeout),
            StreamIngestError::Lambda(LambdaError::MemoryLimit),
            StreamIngestError::Config(ConfigError::ValidationFailed {
                reason: "test".to_string(),
            }),
            StreamIngestError::Serialization(SerializationError::Json {
                reason: "test".to_string(),
            }),
        ];

        for error in non_retryable_errors {
            assert!(
                !error.is_retryable(),
                "Error should not be retryable: {:?}",
                error
            );
        }
    }

    #[test]
    fn test_should_skip_message() {
        let skip_errors = vec![
            StreamIngestError::Serialization(SerializationError::Json {
                reason: "test".to_string(),
            }),
            StreamIngestError::Kafka(KafkaError::MessageParsing {
                reason: "test".to_string(),
            }),
            StreamIngestError::Delta(DeltaError::SchemaConversion {
                reason: "test".to_string(),
            }),
        ];

        for error in skip_errors {
            assert!(
                error.should_skip_message(),
                "Error should skip message: {:?}",
                error
            );
        }

        let no_skip_errors = vec![
            StreamIngestError::Kafka(KafkaError::Consumption {
                reason: "test".to_string(),
            }),
            StreamIngestError::S3(S3Error::Upload {
                bucket: "test".to_string(),
                key: "test".to_string(),
                reason: "test".to_string(),
            }),
            StreamIngestError::Lambda(LambdaError::Timeout),
        ];

        for error in no_skip_errors {
            assert!(
                !error.should_skip_message(),
                "Error should not skip message: {:?}",
                error
            );
        }
    }

    #[test]
    fn test_get_retry_delay_ms() {
        let test_cases = vec![
            (
                StreamIngestError::Kafka(KafkaError::Consumption {
                    reason: "test".to_string(),
                }),
                1000,
            ),
            (
                StreamIngestError::Delta(DeltaError::Write {
                    reason: "test".to_string(),
                }),
                2000,
            ),
            (
                StreamIngestError::S3(S3Error::Upload {
                    bucket: "test".to_string(),
                    key: "test".to_string(),
                    reason: "test".to_string(),
                }),
                3000,
            ),
            (StreamIngestError::Lambda(LambdaError::Timeout), 5000),
        ];

        for (error, expected_delay) in test_cases {
            assert_eq!(
                error.get_retry_delay_ms(),
                expected_delay,
                "Retry delay mismatch for error: {:?}",
                error
            );
        }
    }

    #[test]
    fn test_serialization_error_type_conversion() {
        let error = SerializationError::TypeConversion {
            from_type: "String".to_string(),
            to_type: "Integer".to_string(),
            reason: "Invalid format".to_string(),
        };

        assert_eq!(
            error.to_string(),
            "Data type conversion failed from String to Integer: Invalid format"
        );
    }

    #[test]
    fn test_error_chain_display() {
        let inner_error = ConfigError::MissingField {
            field: "bootstrap_servers".to_string(),
        };
        let outer_error = StreamIngestError::Config(inner_error);

        let error_string = outer_error.to_string();
        assert!(error_string.contains("Configuration error"));
        assert!(error_string.contains("Missing required field: bootstrap_servers"));
    }

    #[test]
    fn test_all_error_variants_coverage() {
        let _config_errors = vec![
            ConfigError::Invalid {
                message: "test".to_string(),
            },
            ConfigError::MissingField {
                field: "test".to_string(),
            },
            ConfigError::ValidationFailed {
                reason: "test".to_string(),
            },
            ConfigError::LoadFailed {
                source: "test".to_string(),
                error: Box::new(std::io::Error::new(std::io::ErrorKind::Other, "test error")),
            },
        ];

        let _kafka_errors = vec![
            KafkaError::ConsumerCreation {
                reason: "test".to_string(),
            },
            KafkaError::Subscription {
                topic: "test".to_string(),
                reason: "test".to_string(),
            },
            KafkaError::Consumption {
                reason: "test".to_string(),
            },
            KafkaError::OffsetCommit {
                reason: "test".to_string(),
            },
            KafkaError::MessageParsing {
                reason: "test".to_string(),
            },
            KafkaError::HealthCheck {
                reason: "test".to_string(),
            },
        ];

        let _delta_errors = vec![
            DeltaError::TableCreation {
                reason: "test".to_string(),
            },
            DeltaError::Write {
                reason: "test".to_string(),
            },
            DeltaError::SchemaConversion {
                reason: "test".to_string(),
            },
            DeltaError::TableLoad {
                path: "test".to_string(),
                reason: "test".to_string(),
            },
            DeltaError::Optimization {
                reason: "test".to_string(),
            },
            DeltaError::Vacuum {
                reason: "test".to_string(),
            },
        ];

        let _s3_errors = vec![
            S3Error::ClientCreation {
                reason: "test".to_string(),
            },
            S3Error::BucketAccess {
                bucket: "test".to_string(),
                reason: "test".to_string(),
            },
            S3Error::ListObjects {
                bucket: "test".to_string(),
                reason: "test".to_string(),
            },
            S3Error::Upload {
                bucket: "test".to_string(),
                key: "test".to_string(),
                reason: "test".to_string(),
            },
            S3Error::Download {
                bucket: "test".to_string(),
                key: "test".to_string(),
                reason: "test".to_string(),
            },
            S3Error::Delete {
                bucket: "test".to_string(),
                key: "test".to_string(),
                reason: "test".to_string(),
            },
            S3Error::HealthCheck {
                reason: "test".to_string(),
            },
        ];

        let _lambda_errors = vec![
            LambdaError::Initialization {
                reason: "test".to_string(),
            },
            LambdaError::Invocation {
                reason: "test".to_string(),
            },
            LambdaError::Timeout,
            LambdaError::MemoryLimit,
            LambdaError::ColdStart,
        ];

        let _serialization_errors = vec![
            SerializationError::Json {
                reason: "test".to_string(),
            },
            SerializationError::Yaml {
                reason: "test".to_string(),
            },
            SerializationError::Arrow {
                reason: "test".to_string(),
            },
            SerializationError::TypeConversion {
                from_type: "test".to_string(),
                to_type: "test".to_string(),
                reason: "test".to_string(),
            },
        ];

        // If this compiles, all variants are covered
    }
}
