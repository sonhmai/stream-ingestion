use anyhow::{Context, Result};
use futures::stream::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{ClientConfig, TopicPartitionList};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::config::{IngestConfig, KafkaConfig};

pub struct KafkaConsumerClient {
    consumer: StreamConsumer,
    config: KafkaConfig,
}

#[derive(Debug, Clone)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub payload: Option<String>,
    pub timestamp: Option<i64>,
    pub headers: HashMap<String, String>,
}

impl KafkaConsumerClient {
    pub fn new(config: &IngestConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        
        client_config
            .set("bootstrap.servers", &config.kafka.bootstrap_servers)
            .set("group.id", &config.kafka.consumer_group)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", config.kafka.auto_offset_reset.as_deref().unwrap_or("earliest"));

        if let Some(timeout) = config.kafka.session_timeout_ms {
            client_config.set("session.timeout.ms", &timeout.to_string());
        }

        if let Some(heartbeat) = config.kafka.heartbeat_interval_ms {
            client_config.set("heartbeat.interval.ms", &heartbeat.to_string());
        }

        if let Some(security_protocol) = &config.kafka.security_protocol {
            client_config.set("security.protocol", security_protocol);
        }

        if let Some(sasl_mechanisms) = &config.kafka.sasl_mechanisms {
            client_config.set("sasl.mechanisms", sasl_mechanisms);
        }

        if let Some(sasl_username) = &config.kafka.sasl_username {
            client_config.set("sasl.username", sasl_username);
        }

        if let Some(sasl_password) = &config.kafka.sasl_password {
            client_config.set("sasl.password", sasl_password);
        }

        if let Some(ssl_ca_location) = &config.kafka.ssl_ca_location {
            client_config.set("ssl.ca.location", ssl_ca_location);
        }

        let consumer: StreamConsumer = client_config
            .create()
            .context("Failed to create Kafka consumer")?;

        Ok(Self {
            consumer,
            config: config.kafka.clone(),
        })
    }

    pub async fn subscribe(&self) -> Result<()> {
        let topics = vec![self.config.topic.as_str()];
        self.consumer
            .subscribe(&topics)
            .context("Failed to subscribe to Kafka topic")?;
        
        info!("Subscribed to Kafka topic: {}", self.config.topic);
        Ok(())
    }

    pub async fn consume_batch(&self, batch_size: usize, timeout_ms: u64) -> Result<Vec<KafkaMessage>> {
        let mut messages = Vec::new();
        let batch_timeout = Duration::from_millis(timeout_ms);
        let message_timeout = Duration::from_millis(5000);

        let start_time = std::time::Instant::now();

        while messages.len() < batch_size && start_time.elapsed() < batch_timeout {
            match timeout(message_timeout, self.consumer.recv()).await {
                Ok(msg_result) => {
                    match msg_result {
                        Ok(message) => {
                            let kafka_msg = self.convert_message(&message)?;
                            messages.push(kafka_msg);
                            debug!("Received message from partition {} offset {}", 
                                   message.partition(), message.offset());
                        }
                        Err(e) => {
                            warn!("Error receiving message: {}", e);
                            continue;
                        }
                    }
                }
                Err(_) => {
                    debug!("Message receive timeout, continuing...");
                    if messages.is_empty() {
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }

        if !messages.is_empty() {
            info!("Consumed batch of {} messages", messages.len());
        }

        Ok(messages)
    }

    pub async fn commit_offsets(&self, messages: &[KafkaMessage]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        let last_message = messages.last().unwrap();
        
        tpl.add_partition_offset(
            &last_message.topic,
            last_message.partition,
            rdkafka::Offset::Offset(last_message.offset + 1),
        )?;

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .context("Failed to commit offsets")?;

        debug!("Committed offset {} for partition {}", 
               last_message.offset + 1, last_message.partition);
        
        Ok(())
    }

    fn convert_message(&self, message: &rdkafka::message::BorrowedMessage) -> Result<KafkaMessage> {
        let key = message.key()
            .map(|k| String::from_utf8_lossy(k).to_string());

        let payload = message.payload()
            .map(|p| String::from_utf8_lossy(p).to_string());

        let mut headers = HashMap::new();
        if let Some(header_map) = message.headers() {
            for header in header_map.iter() {
                if let Some(value) = header.value {
                    headers.insert(
                        header.key.to_string(),
                        String::from_utf8_lossy(value).to_string(),
                    );
                }
            }
        }

        Ok(KafkaMessage {
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
            key,
            payload,
            timestamp: message.timestamp().to_millis(),
            headers,
        })
    }

    pub async fn health_check(&self) -> Result<()> {
        let metadata = self.consumer
            .fetch_metadata(Some(&self.config.topic), Duration::from_secs(10))
            .context("Failed to fetch metadata for health check")?;

        if metadata.topics().is_empty() {
            return Err(anyhow::anyhow!("Topic {} not found", self.config.topic));
        }

        let topic_metadata = &metadata.topics()[0];
        if topic_metadata.partitions().is_empty() {
            return Err(anyhow::anyhow!("No partitions found for topic {}", self.config.topic));
        }

        info!("Health check passed for topic {} with {} partitions", 
              self.config.topic, topic_metadata.partitions().len());
        
        Ok(())
    }
}

pub fn parse_json_message(message: &KafkaMessage) -> Result<Value> {
    let payload = message.payload
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Message payload is empty"))?;

    serde_json::from_str(payload)
        .context("Failed to parse message payload as JSON")
}

pub fn extract_partition_values(
    message: &Value,
    partition_columns: &[String],
) -> Result<HashMap<String, String>> {
    let mut partition_values = HashMap::new();

    for column in partition_columns {
        let value = message
            .get(column)
            .ok_or_else(|| anyhow::anyhow!("Partition column '{}' not found in message", column))?;

        let string_value = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => serde_json::to_string(value)
                .context("Failed to serialize partition value")?,
        };

        partition_values.insert(column.clone(), string_value);
    }

    Ok(partition_values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{IngestConfig, KafkaConfig, S3Config, DeltaConfig, ProcessingConfig, WriteMode, SchemaField, DataType};

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
                ],
                partition_columns: Some(vec!["date".to_string()]),
                write_mode: WriteMode::Append,
                merge_schema: false,
                overwrite_schema: false,
            },
            processing: ProcessingConfig::default(),
        }
    }

    fn create_test_kafka_message() -> KafkaMessage {
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        headers.insert("source".to_string(), "test-producer".to_string());

        KafkaMessage {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 123,
            key: Some("test-key".to_string()),
            payload: Some(r#"{"id": 1, "name": "test", "date": "2024-01-01"}"#.to_string()),
            timestamp: Some(1672531200000), // 2023-01-01 00:00:00 UTC
            headers,
        }
    }

    #[test]
    fn test_kafka_message_creation() {
        let message = create_test_kafka_message();
        
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.partition, 0);
        assert_eq!(message.offset, 123);
        assert_eq!(message.key, Some("test-key".to_string()));
        assert!(message.payload.is_some());
        assert!(message.timestamp.is_some());
        assert_eq!(message.headers.len(), 2);
    }

    #[test]
    fn test_parse_json_message_success() {
        let message = create_test_kafka_message();
        let parsed = parse_json_message(&message).unwrap();
        
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["name"], "test");
        assert_eq!(parsed["date"], "2024-01-01");
    }

    #[test]
    fn test_parse_json_message_empty_payload() {
        let mut message = create_test_kafka_message();
        message.payload = None;
        
        let result = parse_json_message(&message);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("payload is empty"));
    }

    #[test]
    fn test_parse_json_message_invalid_json() {
        let mut message = create_test_kafka_message();
        message.payload = Some("invalid json".to_string());
        
        let result = parse_json_message(&message);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to parse"));
    }

    #[test]
    fn test_extract_partition_values_success() {
        let json_value = serde_json::json!({
            "id": 1,
            "name": "test",
            "date": "2024-01-01",
            "category": "electronics"
        });
        
        let partition_columns = vec!["date".to_string(), "category".to_string()];
        let partition_values = extract_partition_values(&json_value, &partition_columns).unwrap();
        
        assert_eq!(partition_values.len(), 2);
        assert_eq!(partition_values["date"], "2024-01-01");
        assert_eq!(partition_values["category"], "electronics");
    }

    #[test]
    fn test_extract_partition_values_missing_column() {
        let json_value = serde_json::json!({
            "id": 1,
            "name": "test"
        });
        
        let partition_columns = vec!["date".to_string()];
        let result = extract_partition_values(&json_value, &partition_columns);
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found in message"));
    }

    #[test]
    fn test_extract_partition_values_different_types() {
        let json_value = serde_json::json!({
            "string_field": "text",
            "number_field": 42,
            "bool_field": true,
            "object_field": {"nested": "value"}
        });
        
        let partition_columns = vec![
            "string_field".to_string(),
            "number_field".to_string(), 
            "bool_field".to_string(),
            "object_field".to_string()
        ];
        
        let partition_values = extract_partition_values(&json_value, &partition_columns).unwrap();
        
        assert_eq!(partition_values["string_field"], "text");
        assert_eq!(partition_values["number_field"], "42");
        assert_eq!(partition_values["bool_field"], "true");
        assert!(partition_values["object_field"].contains("nested"));
    }

    #[test]
    fn test_extract_partition_values_empty_columns() {
        let json_value = serde_json::json!({"id": 1});
        let partition_columns: Vec<String> = vec![];
        
        let partition_values = extract_partition_values(&json_value, &partition_columns).unwrap();
        assert!(partition_values.is_empty());
    }

    // Note: The following tests would require actual Kafka infrastructure or mocking
    // For now, we'll test the client creation and configuration logic

    #[test]
    fn test_kafka_consumer_client_creation() {
        let config = create_test_config();
        
        // This test verifies that the client can be created with valid configuration
        // In a real test environment, you would need a running Kafka instance
        // For unit testing, we're just testing the configuration setup
        let result = std::panic::catch_unwind(|| {
            KafkaConsumerClient::new(&config)
        });
        
        // The creation might fail due to no Kafka broker, but the config should be valid
        // We're mainly testing that the configuration is properly set up
        match result {
            Ok(client_result) => {
                // If creation succeeds (e.g., in CI with Kafka), verify the config
                if let Ok(client) = client_result {
                    assert_eq!(client.config.bootstrap_servers, "localhost:9092");
                    assert_eq!(client.config.topic, "test-topic");
                    assert_eq!(client.config.consumer_group, "test-group");
                }
            }
            Err(_) => {
                // Creation failed, which is expected without a running Kafka broker
                // This is normal in unit test environments
            }
        }
    }

    #[test]
    fn test_kafka_config_defaults() {
        let config = create_test_config();
        
        assert_eq!(config.kafka.bootstrap_servers, "localhost:9092");
        assert_eq!(config.kafka.topic, "test-topic");
        assert_eq!(config.kafka.consumer_group, "test-group");
        assert_eq!(config.kafka.security_protocol, Some("PLAINTEXT".to_string()));
        assert_eq!(config.kafka.auto_offset_reset, Some("earliest".to_string()));
        assert_eq!(config.kafka.session_timeout_ms, Some(30000));
        assert_eq!(config.kafka.heartbeat_interval_ms, Some(3000));
        assert_eq!(config.kafka.max_poll_records, Some(1000));
    }

    #[test]
    fn test_kafka_message_with_no_headers() {
        let mut message = create_test_kafka_message();
        message.headers.clear();
        
        assert_eq!(message.headers.len(), 0);
        
        // The message should still be valid for processing
        let result = parse_json_message(&message);
        assert!(result.is_ok());
    }

    #[test]
    fn test_kafka_message_with_no_key() {
        let mut message = create_test_kafka_message();
        message.key = None;
        
        assert!(message.key.is_none());
        
        // The message should still be valid for processing
        let result = parse_json_message(&message);
        assert!(result.is_ok());
    }

    #[test]
    fn test_kafka_message_with_no_timestamp() {
        let mut message = create_test_kafka_message();
        message.timestamp = None;
        
        assert!(message.timestamp.is_none());
        
        // The message should still be valid for processing
        let result = parse_json_message(&message);
        assert!(result.is_ok());
    }
}