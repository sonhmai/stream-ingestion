use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::write::{WriteBuilder, WriteMode as DeltaWriteMode};
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable, DeltaTableBuilder};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::{DataType, DeltaConfig, SchemaField, WriteMode};
use crate::kafka::KafkaMessage;

pub struct DeltaWriter {
    config: DeltaConfig,
    table_uri: String,
    schema: Arc<Schema>,
}

impl DeltaWriter {
    pub fn new(config: DeltaConfig, table_uri: String) -> Result<Self> {
        let schema = Self::build_arrow_schema(&config.schema)?;
        
        Ok(Self {
            config,
            table_uri,
            schema: Arc::new(schema),
        })
    }

    pub async fn ensure_table_exists(&self) -> Result<DeltaTable> {
        match DeltaTableBuilder::from_uri(&self.table_uri).load().await {
            Ok(table) => {
                info!("Delta table exists at: {}", self.table_uri);
                Ok(table)
            }
            Err(_) => {
                info!("Creating new Delta table at: {}", self.table_uri);
                self.create_table().await
            }
        }
    }

    async fn create_table(&self) -> Result<DeltaTable> {
        let mut builder = CreateBuilder::new()
            .with_location(&self.table_uri)
            .with_table_name(&self.config.table_name)
            .with_columns(
                self.schema
                    .fields()
                    .iter()
                    .map(|f| {
                        deltalake::kernel::StructField::new(
                            f.name().clone(),
                            self.arrow_to_delta_type(f.data_type()).unwrap(),
                            f.is_nullable(),
                        )
                    })
                    .collect(),
            );

        if let Some(partition_cols) = &self.config.partition_columns {
            builder = builder.with_partition_columns(partition_cols.clone());
        }

        let table = builder
            .await
            .context("Failed to create Delta table")?;

        info!("Successfully created Delta table: {}", self.config.table_name);
        Ok(table)
    }

    pub async fn write_batch(&self, messages: Vec<KafkaMessage>) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let batch_data = self.convert_messages_to_records(messages)?;
        let record_batch = RecordBatch::try_new(self.schema.clone(), batch_data)
            .context("Failed to create record batch")?;

        let table = self.ensure_table_exists().await?;
        
        let write_mode = match self.config.write_mode {
            WriteMode::Append => DeltaWriteMode::Append,
            WriteMode::Overwrite => DeltaWriteMode::Overwrite,
            WriteMode::ErrorIfExists => DeltaWriteMode::ErrorIfExists,
            WriteMode::Ignore => DeltaWriteMode::Ignore,
        };

        let mut write_builder = WriteBuilder::new()
            .with_input_batches(vec![record_batch])
            .with_save_mode(SaveMode::Append);

        if let Some(partition_cols) = &self.config.partition_columns {
            write_builder = write_builder.with_partition_columns(partition_cols.clone());
        }

        let _table = DeltaOps(table)
            .write(vec![record_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .context("Failed to write batch to Delta table")?;

        info!("Successfully wrote batch to Delta table");
        Ok(())
    }

    fn convert_messages_to_records(&self, messages: Vec<KafkaMessage>) -> Result<Vec<ArrayRef>> {
        let mut columns: HashMap<String, Vec<Option<Value>>> = HashMap::new();

        for field in self.schema.fields() {
            columns.insert(field.name().clone(), Vec::new());
        }

        for message in &messages {
            let json_value: Value = if let Some(payload) = &message.payload {
                serde_json::from_str(payload).unwrap_or(Value::Null)
            } else {
                Value::Null
            };

            for field in self.schema.fields() {
                let column_data = columns.get_mut(field.name()).unwrap();
                
                let value = if field.name() == "_kafka_topic" {
                    Some(Value::String(message.topic.clone()))
                } else if field.name() == "_kafka_partition" {
                    Some(Value::Number(serde_json::Number::from(message.partition)))
                } else if field.name() == "_kafka_offset" {
                    Some(Value::Number(serde_json::Number::from(message.offset)))
                } else if field.name() == "_kafka_timestamp" {
                    message.timestamp.map(|ts| Value::Number(serde_json::Number::from(ts)))
                } else if field.name() == "_kafka_key" {
                    message.key.as_ref().map(|k| Value::String(k.clone()))
                } else {
                    json_value.get(field.name()).cloned()
                };

                column_data.push(value);
            }
        }

        let mut arrays: Vec<ArrayRef> = Vec::new();

        for field in self.schema.fields() {
            let column_data = columns.get(field.name()).unwrap();
            let array = self.create_array_from_values(field, column_data)?;
            arrays.push(array);
        }

        Ok(arrays)
    }

    fn create_array_from_values(&self, field: &Field, values: &[Option<Value>]) -> Result<ArrayRef> {
        match field.data_type() {
            ArrowDataType::Boolean => {
                let array: BooleanArray = values
                    .iter()
                    .map(|v| v.as_ref().and_then(|val| val.as_bool()))
                    .collect();
                Ok(Arc::new(array))
            }
            ArrowDataType::Int32 => {
                let array: Int32Array = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|val| {
                            val.as_i64().map(|i| i as i32)
                        })
                    })
                    .collect();
                Ok(Arc::new(array))
            }
            ArrowDataType::Int64 => {
                let array: Int64Array = values
                    .iter()
                    .map(|v| v.as_ref().and_then(|val| val.as_i64()))
                    .collect();
                Ok(Arc::new(array))
            }
            ArrowDataType::UInt32 => {
                let array: UInt32Array = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|val| {
                            val.as_u64().map(|u| u as u32)
                        })
                    })
                    .collect();
                Ok(Arc::new(array))
            }
            ArrowDataType::UInt64 => {
                let array: UInt64Array = values
                    .iter()
                    .map(|v| v.as_ref().and_then(|val| val.as_u64()))
                    .collect();
                Ok(Arc::new(array))
            }
            ArrowDataType::Float32 => {
                let array: Float32Array = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|val| {
                            val.as_f64().map(|f| f as f32)
                        })
                    })
                    .collect();
                Ok(Arc::new(array))
            }
            ArrowDataType::Float64 => {
                let array: Float64Array = values
                    .iter()
                    .map(|v| v.as_ref().and_then(|val| val.as_f64()))
                    .collect();
                Ok(Arc::new(array))
            }
            ArrowDataType::Utf8 => {
                let array: StringArray = values
                    .iter()
                    .map(|v| {
                        v.as_ref().map(|val| match val {
                            Value::String(s) => s.clone(),
                            _ => val.to_string(),
                        })
                    })
                    .collect();
                Ok(Arc::new(array))
            }
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
                let array: TimestampMillisecondArray = values
                    .iter()
                    .map(|v| v.as_ref().and_then(|val| val.as_i64()))
                    .collect();
                Ok(Arc::new(array))
            }
            ArrowDataType::Date32 => {
                let array: Date32Array = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|val| {
                            val.as_i64().map(|i| i as i32)
                        })
                    })
                    .collect();
                Ok(Arc::new(array))
            }
            _ => {
                warn!("Unsupported data type: {:?}, converting to string", field.data_type());
                let array: StringArray = values
                    .iter()
                    .map(|v| {
                        v.as_ref().map(|val| val.to_string())
                    })
                    .collect();
                Ok(Arc::new(array))
            }
        }
    }

    fn build_arrow_schema(schema_fields: &[SchemaField]) -> Result<Schema> {
        let mut fields = Vec::new();

        fields.push(Field::new("_kafka_topic", ArrowDataType::Utf8, false));
        fields.push(Field::new("_kafka_partition", ArrowDataType::Int32, false));
        fields.push(Field::new("_kafka_offset", ArrowDataType::Int64, false));
        fields.push(Field::new("_kafka_timestamp", ArrowDataType::Timestamp(TimeUnit::Millisecond, None), true));
        fields.push(Field::new("_kafka_key", ArrowDataType::Utf8, true));

        for schema_field in schema_fields {
            let arrow_type = Self::convert_data_type(&schema_field.data_type)?;
            fields.push(Field::new(&schema_field.name, arrow_type, schema_field.nullable));
        }

        Ok(Schema::new(fields))
    }

    fn convert_data_type(data_type: &DataType) -> Result<ArrowDataType> {
        let arrow_type = match data_type {
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Utf8 => ArrowDataType::Utf8,
            DataType::Binary => ArrowDataType::Binary,
            DataType::Date32 => ArrowDataType::Date32,
            DataType::Date64 => ArrowDataType::Date64,
            DataType::TimestampSecond => ArrowDataType::Timestamp(TimeUnit::Second, None),
            DataType::TimestampMillisecond => ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::TimestampMicrosecond => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::TimestampNanosecond => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Decimal128 { precision, scale } => {
                ArrowDataType::Decimal128(*precision, *scale)
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported data type: {:?}", data_type));
            }
        };
        Ok(arrow_type)
    }

    fn arrow_to_delta_type(&self, arrow_type: &ArrowDataType) -> Result<deltalake::kernel::DataType> {
        use deltalake::kernel::DataType as DeltaDataType;
        use deltalake::kernel::PrimitiveType;

        let delta_type = match arrow_type {
            ArrowDataType::Boolean => DeltaDataType::Primitive(PrimitiveType::Boolean),
            ArrowDataType::Int8 => DeltaDataType::Primitive(PrimitiveType::Byte),
            ArrowDataType::Int16 => DeltaDataType::Primitive(PrimitiveType::Short),
            ArrowDataType::Int32 => DeltaDataType::Primitive(PrimitiveType::Integer),
            ArrowDataType::Int64 => DeltaDataType::Primitive(PrimitiveType::Long),
            ArrowDataType::Float32 => DeltaDataType::Primitive(PrimitiveType::Float),
            ArrowDataType::Float64 => DeltaDataType::Primitive(PrimitiveType::Double),
            ArrowDataType::Utf8 => DeltaDataType::Primitive(PrimitiveType::String),
            ArrowDataType::Binary => DeltaDataType::Primitive(PrimitiveType::Binary),
            ArrowDataType::Date32 => DeltaDataType::Primitive(PrimitiveType::Date),
            ArrowDataType::Timestamp(_, _) => DeltaDataType::Primitive(PrimitiveType::Timestamp),
            ArrowDataType::Decimal128(precision, scale) => {
                DeltaDataType::decimal(*precision, *scale)?
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported Arrow type for Delta: {:?}", arrow_type));
            }
        };
        Ok(delta_type)
    }

    pub async fn optimize_table(&self) -> Result<()> {
        let table = self.ensure_table_exists().await?;
        
        let _optimized_table = DeltaOps(table)
            .optimize()
            .await
            .context("Failed to optimize Delta table")?;

        info!("Successfully optimized Delta table");
        Ok(())
    }

    pub async fn vacuum_table(&self, retention_hours: Option<u64>) -> Result<()> {
        let table = self.ensure_table_exists().await?;
        let retention = retention_hours.unwrap_or(168); // Default 7 days

        let _vacuumed_table = DeltaOps(table)
            .vacuum()
            .with_retention_period(std::time::Duration::from_secs(retention * 3600))
            .await
            .context("Failed to vacuum Delta table")?;

        info!("Successfully vacuumed Delta table with retention {} hours", retention);
        Ok(())
    }
}