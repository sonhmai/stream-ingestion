use anyhow::{Context, Result};
use futures::stream::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use rdkafka::{ClientConfig, TopicPartitionList};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::config::{IngestConfig, KafkaOptions};
use crate::errors::{KafkaError, SourceError};
use crate::source::{CheckpointHandle, MessageBatch, Source, SourceMessage};

pub struct KafkaSource {
    client: KafkaConsumerClient,
    batch_size: usize,
    batch_timeout_ms: u64,
}

impl KafkaSource {
    pub async fn new(config: &KafkaOptions) -> Result<Self> {
        let client = KafkaConsumerClient::new(config)?;
        client.subscribe().await?;

        Ok(Self {
            client,
            batch_size: config.max_poll_records.unwrap_or(100) as usize,
            batch_timeout_ms: 1000, // Default 1 second timeout for batch collection
        })
    }
}

impl Source for KafkaSource {
    async fn next_batch(&self) -> std::result::Result<MessageBatch, SourceError> {
    let messages = self
        .client
        .consume_batch(self.batch_size, self.batch_timeout_ms)
        .await
        .map_err(|e| SourceError::Connection {
            source: Box::new(KafkaError::Consumption {
                reason: e.to_string(),
            })
        })?;


        if messages.is_empty() {
        return Ok(MessageBatch { messages: vec![] });
    }

    let source_messages: Vec<SourceMessage> = messages
        .into_iter()
        .filter_map(|kafka_msg| {
            let payload = kafka_msg.payload?;
            
            let mut headers = HashMap::new();
            for (key, value) in kafka_msg.headers {
                headers.insert(key, value.into_bytes());
            }
            Some(SourceMessage {
                payload: payload.into_bytes(),
                topic: kafka_msg.topic,
                partition: kafka_msg.partition as usize,
                headers,
            })
        })
        .collect();

    Ok(MessageBatch {
        messages: source_messages,
    })
}

    async fn commit(&self, handles: &[CheckpointHandle]) -> std::result::Result<(), SourceError> {
        let messages: Vec<KafkaMessage> = handles
            .iter()
            .map(|handle| KafkaMessage {
                topic: handle.topic.clone(),
                partition: handle.partition,
                offset: handle.offset,
                key: None,
                payload: None,
                timestamp: None,
                headers: HashMap::new(),
            })
            .collect();

        self.client
            .commit_offsets(&messages)
            .await
            .map_err(|e| SourceError::Kafka(KafkaError::OffsetCommit {
                reason: e.to_string(),
            }))
    }

    async fn shutdown(&self) -> std::result::Result<(), SourceError> {
        // The rdkafka consumer will automatically close when dropped
        Ok(())
    }
}

pub struct KafkaConsumerClient {
    consumer: StreamConsumer,
    config: KafkaOptions,
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
    pub fn new(config: &KafkaOptions) -> Result<Self> {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.consumer_group)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set(
                "auto.offset.reset",
                config
                    .auto_offset_reset
                    .as_deref()
                    .unwrap_or("earliest"),
            );

        if let Some(timeout) = config.session_timeout_ms {
            client_config.set("session.timeout.ms", &timeout.to_string());
        }

        if let Some(heartbeat) = config.heartbeat_interval_ms {
            client_config.set("heartbeat.interval.ms", &heartbeat.to_string());
        }

        if let Some(security_protocol) = &config.security_protocol {
            client_config.set("security.protocol", security_protocol);
        }

        if let Some(sasl_mechanisms) = &config.sasl_mechanisms {
            client_config.set("sasl.mechanisms", sasl_mechanisms);
        }

        if let Some(sasl_username) = &config.sasl_username {
            client_config.set("sasl.username", sasl_username);
        }

        if let Some(sasl_password) = &config.sasl_password {
            client_config.set("sasl.password", sasl_password);
        }

        if let Some(ssl_ca_location) = &config.ssl_ca_location {
            client_config.set("ssl.ca.location", ssl_ca_location);
        }

        let consumer: StreamConsumer = client_config
            .create()
            .context("Failed to create Kafka consumer")?;

        Ok(Self {
            consumer,
            config: config.clone(),
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

    pub async fn consume_batch(
        &self,
        batch_size: usize,
        timeout_ms: u64,
    ) -> Result<Vec<KafkaMessage>> {
        let mut messages = Vec::new();
        let batch_timeout = Duration::from_millis(timeout_ms);
        let message_timeout = Duration::from_millis(5000);

        let start_time = std::time::Instant::now();

        while messages.len() < batch_size && start_time.elapsed() < batch_timeout {
            match timeout(message_timeout, self.consumer.recv()).await {
                Ok(msg_result) => match msg_result {
                    Ok(message) => {
                        let kafka_msg = self.convert_message(&message)?;
                        messages.push(kafka_msg);
                        debug!(
                            "Received message from partition {} offset {}",
                            message.partition(),
                            message.offset()
                        );
                    }
                    Err(e) => {
                        warn!("Error receiving message: {}", e);
                        continue;
                    }
                },
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

        debug!(
            "Committed offset {} for partition {}",
            last_message.offset + 1,
            last_message.partition
        );

        Ok(())
    }

    fn convert_message(&self, message: &rdkafka::message::BorrowedMessage) -> Result<KafkaMessage> {
        let key = message
            .key()
            .map(|k| String::from_utf8_lossy(k).to_string());

        let payload = message
            .payload()
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
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.config.topic), Duration::from_secs(10))
            .context("Failed to fetch metadata for health check")?;

        if metadata.topics().is_empty() {
            return Err(anyhow::anyhow!("Topic {} not found", self.config.topic));
        }

        let topic_metadata = &metadata.topics()[0];
        if topic_metadata.partitions().is_empty() {
            return Err(anyhow::anyhow!(
                "No partitions found for topic {}",
                self.config.topic
            ));
        }

        info!(
            "Health check passed for topic {} with {} partitions",
            self.config.topic,
            topic_metadata.partitions().len()
        );

        Ok(())
    }
}

pub fn parse_json_message(message: &KafkaMessage) -> Result<Value> {
    let payload = message
        .payload
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Message payload is empty"))?;

    serde_json::from_str(payload).context("Failed to parse message payload as JSON")
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
            _ => serde_json::to_string(value).context("Failed to serialize partition value")?,
        };

        partition_values.insert(column.clone(), string_value);
    }

    Ok(partition_values)
}

#[cfg(test)]
mod tests {
    #[test]
    fn kafka_source_consume() {
        println!("dummy test")
    }
}
