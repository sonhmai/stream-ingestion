use anyhow::{Context, Result};
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::{Client, Config};
use std::collections::HashMap;
use tracing::{debug, info, warn};

use crate::config::S3Config;

pub struct S3Client {
    client: Client,
    config: S3Config,
}

impl S3Client {
    pub async fn new(config: S3Config) -> Result<Self> {
        let region_provider = RegionProviderChain::default_provider()
            .or_else(&config.region);

        let mut aws_config_builder = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider);

        if let Some(access_key) = &config.access_key_id {
            if let Some(secret_key) = &config.secret_access_key {
                aws_config_builder = aws_config_builder.credentials_provider(
                    aws_sdk_s3::config::Credentials::new(
                        access_key,
                        secret_key,
                        config.session_token.clone(),
                        None,
                        "stream-ingest",
                    )
                );
            }
        }

        let aws_config = aws_config_builder.load().await;

        let mut s3_config_builder = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(aws_config.region().cloned())
            .credentials_provider(aws_config.credentials_provider().cloned());

        if let Some(endpoint_url) = &config.endpoint_url {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint_url);
        }

        let s3_config = s3_config_builder.build();
        let client = Client::from_conf(s3_config);

        Ok(Self { client, config })
    }

    pub async fn ensure_bucket_exists(&self) -> Result<()> {
        match self.client.head_bucket()
            .bucket(&self.config.bucket)
            .send()
            .await
        {
            Ok(_) => {
                info!("S3 bucket '{}' exists and is accessible", self.config.bucket);
                Ok(())
            }
            Err(e) => {
                warn!("Cannot access S3 bucket '{}': {}", self.config.bucket, e);
                Err(anyhow::anyhow!("S3 bucket '{}' is not accessible: {}", self.config.bucket, e))
            }
        }
    }

    pub async fn list_delta_tables(&self) -> Result<Vec<String>> {
        let mut table_names = Vec::new();
        let prefix = format!("{}/", self.config.prefix.trim_end_matches('/'));

        let response = self.client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(&prefix)
            .delimiter("/")
            .send()
            .await
            .context("Failed to list objects in S3 bucket")?;

        if let Some(common_prefixes) = response.common_prefixes() {
            for common_prefix in common_prefixes {
                if let Some(prefix) = common_prefix.prefix() {
                    let table_name = prefix
                        .strip_prefix(&format!("{}/", self.config.prefix.trim_end_matches('/')))
                        .unwrap_or(prefix)
                        .trim_end_matches('/');
                    
                    if !table_name.is_empty() {
                        table_names.push(table_name.to_string());
                    }
                }
            }
        }

        debug!("Found {} Delta tables in S3", table_names.len());
        Ok(table_names)
    }

    pub async fn check_delta_table_exists(&self, table_name: &str) -> Result<bool> {
        let table_path = format!("{}/{}", 
            self.config.prefix.trim_end_matches('/'), 
            table_name
        );

        let log_path = format!("{}/_delta_log/", table_path);

        let response = self.client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(&log_path)
            .max_keys(1)
            .send()
            .await
            .context("Failed to check Delta table existence")?;

        let exists = response.contents().map(|c| !c.is_empty()).unwrap_or(false);
        
        debug!("Delta table '{}' exists: {}", table_name, exists);
        Ok(exists)
    }

    pub async fn get_table_metadata(&self, table_name: &str) -> Result<HashMap<String, String>> {
        let table_path = format!("{}/{}", 
            self.config.prefix.trim_end_matches('/'), 
            table_name
        );

        let log_path = format!("{}/_delta_log/", table_path);

        let response = self.client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(&log_path)
            .send()
            .await
            .context("Failed to get table metadata")?;

        let mut metadata = HashMap::new();
        
        if let Some(contents) = response.contents() {
            metadata.insert("file_count".to_string(), contents.len().to_string());
            
            if let Some(latest_file) = contents.iter()
                .filter(|obj| obj.key().map(|k| k.ends_with(".json")).unwrap_or(false))
                .max_by_key(|obj| obj.last_modified())
            {
                if let Some(last_modified) = latest_file.last_modified() {
                    metadata.insert("last_modified".to_string(), last_modified.to_string());
                }
                if let Some(size) = latest_file.size() {
                    metadata.insert("latest_log_size".to_string(), size.to_string());
                }
            }
        }

        metadata.insert("table_path".to_string(), format!("s3://{}/{}", self.config.bucket, table_path));
        
        Ok(metadata)
    }

    pub async fn cleanup_old_files(&self, table_name: &str, retention_days: u32) -> Result<usize> {
        let table_path = format!("{}/{}", 
            self.config.prefix.trim_end_matches('/'), 
            table_name
        );

        let cutoff_time = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);

        let response = self.client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(&table_path)
            .send()
            .await
            .context("Failed to list files for cleanup")?;

        let mut deleted_count = 0;

        if let Some(contents) = response.contents() {
            for object in contents {
                if let (Some(key), Some(last_modified)) = (object.key(), object.last_modified()) {
                    if last_modified < &cutoff_time.into() && 
                       !key.contains("_delta_log/") && 
                       !key.ends_with(".checkpoint.parquet") {
                        
                        match self.client
                            .delete_object()
                            .bucket(&self.config.bucket)
                            .key(key)
                            .send()
                            .await
                        {
                            Ok(_) => {
                                debug!("Deleted old file: {}", key);
                                deleted_count += 1;
                            }
                            Err(e) => {
                                warn!("Failed to delete file {}: {}", key, e);
                            }
                        }
                    }
                }
            }
        }

        if deleted_count > 0 {
            info!("Cleaned up {} old files from table '{}'", deleted_count, table_name);
        }

        Ok(deleted_count)
    }

    pub async fn get_bucket_metrics(&self) -> Result<HashMap<String, String>> {
        let mut metrics = HashMap::new();

        let response = self.client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(&self.config.prefix)
            .send()
            .await
            .context("Failed to get bucket metrics")?;

        if let Some(contents) = response.contents() {
            let total_size: i64 = contents.iter()
                .filter_map(|obj| obj.size())
                .sum();
                
            let total_files = contents.len();
            
            metrics.insert("total_files".to_string(), total_files.to_string());
            metrics.insert("total_size_bytes".to_string(), total_size.to_string());
            metrics.insert("total_size_mb".to_string(), (total_size / 1024 / 1024).to_string());
        }

        metrics.insert("bucket_name".to_string(), self.config.bucket.clone());
        metrics.insert("region".to_string(), self.config.region.clone());
        metrics.insert("prefix".to_string(), self.config.prefix.clone());

        Ok(metrics)
    }

    pub async fn health_check(&self) -> Result<()> {
        self.ensure_bucket_exists().await?;

        let test_key = format!("{}/_health_check/test.txt", 
            self.config.prefix.trim_end_matches('/')
        );
        let test_content = format!("Health check at {}", chrono::Utc::now().to_rfc3339());

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&test_key)
            .body(test_content.into_bytes().into())
            .send()
            .await
            .context("Failed to write health check file")?;

        let response = self.client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&test_key)
            .send()
            .await
            .context("Failed to read health check file")?;

        let _body = response.body.collect().await
            .context("Failed to collect health check response body")?;

        self.client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(&test_key)
            .send()
            .await
            .context("Failed to delete health check file")?;

        info!("S3 health check passed for bucket: {}", self.config.bucket);
        Ok(())
    }

    pub fn get_table_uri(&self, table_name: &str) -> String {
        format!("s3://{}/{}/{}", 
            self.config.bucket, 
            self.config.prefix.trim_end_matches('/'),
            table_name
        )
    }

    pub fn get_config(&self) -> &S3Config {
        &self.config
    }
}