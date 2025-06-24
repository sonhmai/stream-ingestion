use stream_ingest::config::*;
use stream_ingest::errors::*;

#[test]
fn test_basic_config_loading() {
    let config = IngestConfig {
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
            schema: vec![SchemaField {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                metadata: None,
            }],
            partition_columns: Some(vec!["date".to_string()]),
            write_mode: WriteMode::Append,
            merge_schema: false,
            overwrite_schema: false,
        },
        processing: ProcessingConfig::default(),
    };

    assert!(config.validate().is_ok());
}

#[test]
fn test_error_types() {
    let kafka_error = KafkaError::ConsumerCreation {
        reason: "Connection failed".to_string(),
    };

    let stream_error = StreamIngestError::Kafka(kafka_error);
    assert!(stream_error.is_retryable());
    assert_eq!(stream_error.get_retry_delay_ms(), 1000);

    let config_error = ConfigError::ValidationFailed {
        reason: "Invalid field".to_string(),
    };

    let stream_error2 = StreamIngestError::Config(config_error);
    assert!(!stream_error2.is_retryable());
}
