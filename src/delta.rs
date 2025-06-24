use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow::{
    array::{
        ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
        StringArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
    },
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::{
    DeltaTable, DeltaTableBuilder,
    kernel::{DataType as DeltaDataType, PrimitiveType, StructField},
    operations::{DeltaOps, create::CreateBuilder},
    protocol::SaveMode,
};
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::{
    config::{DataType, DeltaConfig, SchemaField, WriteMode},
    kafka::KafkaMessage,
};

pub struct DeltaWriter {
    config: DeltaConfig,
    table_uri: String,
    schema: Arc<ArrowSchema>,
}

impl DeltaWriter {
    pub fn new(config: DeltaConfig, table_uri: String) -> Result<Self> {
        let arrow_schema = Self::build_arrow_schema(&config.schema)?;
        Ok(Self {
            config,
            table_uri,
            schema: Arc::new(arrow_schema),
        })
    }

    /* ---------- table helpers ---------- */
    pub async fn ensure_table_exists(&self) -> Result<DeltaTable> {
        match DeltaTableBuilder::from_uri(&self.table_uri).load().await {
            Ok(table) => {
                debug!("Delta table already exists: {}", self.table_uri);
                Ok(table)
            }
            Err(err) => {
                info!("Creating Delta table at {} ({err})", self.table_uri);
                self.create_table().await
            }
        }
    }

    async fn create_table(&self) -> Result<DeltaTable> {
        // Convert Arrow → Delta schema
        let delta_fields: Vec<StructField> = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                let dt = self
                    .arrow_to_delta_type(f.data_type())
                    .with_context(|| format!("while converting column '{}'", f.name()))?;
                Ok(StructField::new(f.name().clone(), dt, f.is_nullable()))
            })
            .collect::<Result<_>>()?;

        let mut builder = CreateBuilder::new()
            .with_location(&self.table_uri)
            .with_table_name(&self.config.table_name)
            .with_columns(delta_fields);

        if let Some(partitions) = &self.config.partition_columns {
            if !partitions.is_empty() {
                builder = builder.with_partition_columns(partitions.clone());
            }
        }

        builder
            .await
            .context("failed to create Delta table")
            .map(|tbl| {
                info!("Delta table created: {}", self.config.table_name);
                tbl
            })
    }

    /* ---------- write path ---------- */
    pub async fn write_batch(&self, messages: Vec<KafkaMessage>) -> Result<()> {
        if messages.is_empty() {
            debug!("Skipping write – empty batch");
            return Ok(());
        }

        let arrays = self.convert_messages_to_arrays(messages)?;
        let record_batch =
            RecordBatch::try_new(self.schema.clone(), arrays).context("building RecordBatch")?;

        let table = self.ensure_table_exists().await?;

        // Map user config → Delta `SaveMode`
        let save_mode = match self.config.write_mode {
            WriteMode::Append => SaveMode::Append,
            WriteMode::Overwrite => SaveMode::Overwrite,
            WriteMode::ErrorIfExists => SaveMode::ErrorIfExists,
            WriteMode::Ignore => SaveMode::Ignore,
        };

        let mut writer = DeltaOps(table)
            .write(vec![record_batch])
            .with_save_mode(save_mode);

        if let Some(partitions) = &self.config.partition_columns {
            if !partitions.is_empty() {
                writer = writer.with_partition_columns(partitions.clone());
            }
        }

        writer
            .await
            .context("writing batch to Delta table")
            .map(|_| info!("Batch successfully written"))
    }

    pub async fn optimize_table(&self) -> Result<()> {
        let table = self.ensure_table_exists().await?;
        DeltaOps(table).optimize().await.context("optimize")?;
        info!("Optimize finished");
        Ok(())
    }

    pub async fn vacuum_table(&self, retention_hours: Option<u64>) -> Result<()> {
        const DEFAULT_RETENTION_HOURS: u64 = 168;
        let table = self.ensure_table_exists().await?;
        let retention_hours = retention_hours.unwrap_or(DEFAULT_RETENTION_HOURS);

        DeltaOps(table)
            .vacuum()
            .with_retention_period(chrono::Duration::hours(retention_hours as i64))
            .await
            .context("vacuum")?;
        info!("Vacuum finished");
        Ok(())
    }

    /* ---------- data conversion ---------- */
    fn convert_messages_to_arrays(&self, messages: Vec<KafkaMessage>) -> Result<Vec<ArrayRef>> {
        // Pre-allocate column buffers
        let mut cols: HashMap<&str, Vec<Option<Value>>> = self
            .schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), Vec::new()))
            .collect();

        for msg in &messages {
            // User payload
            let json = msg
                .payload
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()
                .unwrap_or(None)
                .unwrap_or(Value::Null);

            for f in self.schema.fields() {
                let entry = cols.get_mut(f.name().as_str()).expect("col exists");

                let value = match f.name().as_str() {
                    "_kafka_topic" => Some(Value::String(msg.topic.clone())),
                    "_kafka_partition" => Some(Value::Number(msg.partition.into())),
                    "_kafka_offset" => Some(Value::Number(msg.offset.into())),
                    "_kafka_timestamp" => msg.timestamp.map(|ts| Value::Number(ts.into())),
                    "_kafka_key" => msg.key.as_ref().map(|k| Value::String(k.clone())),
                    other => json.get(other).cloned(),
                };

                entry.push(value);
            }
        }

        // Build Arrow arrays in the table schema order
        self.schema
            .fields()
            .iter()
            .map(|f| {
                let data = cols.get(f.name().as_str()).unwrap();
                self.array_from_json_values(f, data)
                    .with_context(|| format!("building array '{}'", f.name()))
            })
            .collect()
    }

    fn array_from_json_values(&self, field: &Field, values: &[Option<Value>]) -> Result<ArrayRef> {
        Ok(match field.data_type() {
            ArrowDataType::Boolean => Arc::new(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(Value::as_bool))
                    .collect::<BooleanArray>(),
            ),
            ArrowDataType::Int32 => Arc::new(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(Value::as_i64).map(|i| i as i32))
                    .collect::<Int32Array>(),
            ),
            ArrowDataType::Int64 => Arc::new(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(Value::as_i64))
                    .collect::<Int64Array>(),
            ),
            ArrowDataType::UInt32 => Arc::new(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(Value::as_u64).map(|u| u as u32))
                    .collect::<UInt32Array>(),
            ),
            ArrowDataType::UInt64 => Arc::new(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(Value::as_u64))
                    .collect::<UInt64Array>(),
            ),
            ArrowDataType::Float32 => Arc::new(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(Value::as_f64).map(|f| f as f32))
                    .collect::<Float32Array>(),
            ),
            ArrowDataType::Float64 => Arc::new(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(Value::as_f64))
                    .collect::<Float64Array>(),
            ),
            ArrowDataType::Utf8 => Arc::new(
                values
                    .iter()
                    .map(|v| {
                        v.as_ref().map(|val| match val {
                            Value::String(s) => s.clone(),
                            _ => val.to_string(),
                        })
                    })
                    .collect::<StringArray>(),
            ),
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => Arc::new(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(Value::as_i64))
                    .collect::<TimestampMillisecondArray>(),
            ),
            ArrowDataType::Date32 => Arc::new(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(Value::as_i64).map(|i| i as i32))
                    .collect::<Date32Array>(),
            ),
            other => {
                warn!("Unsupported Arrow type {:?} – storing as Utf8", other);
                Arc::new(
                    values
                        .iter()
                        .map(|v| v.as_ref().map(|val| val.to_string()))
                        .collect::<StringArray>(),
                )
            }
        })
    }

    /* ---------- schema helpers ---------- */
    fn build_arrow_schema(schema_fields: &[SchemaField]) -> Result<ArrowSchema> {
        let mut fields = vec![
            Field::new("_kafka_topic", ArrowDataType::Utf8, false),
            Field::new("_kafka_partition", ArrowDataType::Int32, false),
            Field::new("_kafka_offset", ArrowDataType::Int64, false),
            Field::new(
                "_kafka_timestamp",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("_kafka_key", ArrowDataType::Utf8, true),
        ];

        for sf in schema_fields {
            let dt = Self::data_type_to_arrow(&sf.data_type)
                .with_context(|| format!("column '{}'", sf.name))?;
            fields.push(Field::new(&sf.name, dt, sf.nullable));
        }

        Ok(ArrowSchema::new(fields))
    }

    fn data_type_to_arrow(dt: &DataType) -> Result<ArrowDataType> {
        use DataType::*;
        Ok(match dt {
            Boolean => ArrowDataType::Boolean,
            Int8 => ArrowDataType::Int8,
            Int16 => ArrowDataType::Int16,
            Int32 => ArrowDataType::Int32,
            Int64 => ArrowDataType::Int64,
            UInt8 => ArrowDataType::UInt8,
            UInt16 => ArrowDataType::UInt16,
            UInt32 => ArrowDataType::UInt32,
            UInt64 => ArrowDataType::UInt64,
            Float32 => ArrowDataType::Float32,
            Float64 => ArrowDataType::Float64,
            Utf8 => ArrowDataType::Utf8,
            Binary => ArrowDataType::Binary,
            Date32 => ArrowDataType::Date32,
            Date64 => ArrowDataType::Date64,
            TimestampSecond => ArrowDataType::Timestamp(TimeUnit::Second, None),
            TimestampMillisecond => ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            TimestampMicrosecond => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            TimestampNanosecond => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            Decimal128 { precision, scale } => ArrowDataType::Decimal128(*precision, *scale),
            unsupported => return Err(anyhow!("unsupported data type: {unsupported:?}")),
        })
    }

    fn arrow_to_delta_type(&self, arrow: &ArrowDataType) -> Result<DeltaDataType> {
        use PrimitiveType::*;
        Ok(match arrow {
            ArrowDataType::Boolean => DeltaDataType::Primitive(Boolean),
            ArrowDataType::Int8 => DeltaDataType::Primitive(Byte),
            ArrowDataType::Int16 => DeltaDataType::Primitive(Short),
            ArrowDataType::Int32 => DeltaDataType::Primitive(Integer),
            ArrowDataType::Int64 => DeltaDataType::Primitive(Long),
            ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64 => {
                return Err(anyhow!("Delta spec has no unsigned integer types"));
            }
            ArrowDataType::Float32 => DeltaDataType::Primitive(Float),
            ArrowDataType::Float64 => DeltaDataType::Primitive(Double),
            ArrowDataType::Utf8 => DeltaDataType::Primitive(String),
            ArrowDataType::Binary => DeltaDataType::Primitive(Binary),
            ArrowDataType::Date32 => DeltaDataType::Primitive(Date),
            ArrowDataType::Timestamp(_, _) => DeltaDataType::Primitive(Timestamp),
            ArrowDataType::Decimal128(p, s) => DeltaDataType::decimal(*p, *s as u8)?,
            unsupported => {
                return Err(anyhow!(
                    "Arrow type {unsupported:?} is not yet supported in Delta"
                ));
            }
        })
    }
}
