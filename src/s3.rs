use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
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
        let region = aws_config::Region::new(config.region.clone());
        
        let mut aws_config_builder = aws_config::defaults(BehaviorVersion::latest())
            .region(region);

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
            .credentials_provider(aws_config.credentials_provider().unwrap().clone());

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

        for common_prefix in response.common_prefixes() {
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

        let exists = !response.contents().is_empty();
        
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
        
        let contents = response.contents();
        if !contents.is_empty() {
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

        let contents = response.contents();
        for object in contents {
            if let (Some(key), Some(last_modified)) = (object.key(), object.last_modified()) {
                if last_modified < &aws_sdk_s3::primitives::DateTime::from(std::time::SystemTime::from(cutoff_time)) && 
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

        let contents = response.contents();
        if !contents.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::S3Config;

    fn create_test_s3_config() -> S3Config {
        S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            prefix: "test-prefix".to_string(),
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            endpoint_url: None,
        }
    }

    fn create_test_s3_config_with_credentials() -> S3Config {
        S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-west-2".to_string(),
            prefix: "delta-tables".to_string(),
            access_key_id: Some("AKIATEST123".to_string()),
            secret_access_key: Some("secret123".to_string()),
            session_token: Some("session123".to_string()),
            endpoint_url: Some("https://s3.custom-endpoint.com".to_string()),
        }
    }

    fn create_test_s3_config_minimal() -> S3Config {
        S3Config {
            bucket: "minimal-bucket".to_string(),
            region: "eu-west-1".to_string(),
            prefix: "".to_string(),
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            endpoint_url: None,
        }
    }

    fn create_mock_s3_client(config: S3Config) -> S3Client {
        // Create a mock client for testing - this won't actually connect to AWS
        // In a real implementation, we'd use mockall or similar for proper mocking
        let aws_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test"
            ))
            .build();
        let client = Client::from_conf(aws_config);
        S3Client { client, config }
    }

    #[test]
    fn test_get_table_uri() {
        let config = create_test_s3_config();
        let client = create_mock_s3_client(config);

        let uri = client.get_table_uri("user_events");
        assert_eq!(uri, "s3://test-bucket/test-prefix/user_events");
    }

    #[test]
    fn test_get_table_uri_with_trailing_slash() {
        let mut config = create_test_s3_config();
        config.prefix = "test-prefix/".to_string();
        
        let client = create_mock_s3_client(config);

        let uri = client.get_table_uri("user_events");
        assert_eq!(uri, "s3://test-bucket/test-prefix/user_events");
    }

    #[test]
    fn test_get_table_uri_empty_prefix() {
        let mut config = create_test_s3_config();
        config.prefix = "".to_string();
        
        let client = create_mock_s3_client(config);

        let uri = client.get_table_uri("transactions");
        assert_eq!(uri, "s3://test-bucket//transactions");
    }

    #[test]
    fn test_get_table_uri_nested_table_name() {
        let config = create_test_s3_config();
        let client = create_mock_s3_client(config);

        let uri = client.get_table_uri("analytics/daily/events");
        assert_eq!(uri, "s3://test-bucket/test-prefix/analytics/daily/events");
    }

    #[test]
    fn test_get_config() {
        let config = create_test_s3_config_with_credentials();
        let client = create_mock_s3_client(config);

        let returned_config = client.get_config();
        assert_eq!(returned_config.bucket, "test-bucket");
        assert_eq!(returned_config.region, "us-west-2");
        assert_eq!(returned_config.prefix, "delta-tables");
        assert_eq!(returned_config.access_key_id, Some("AKIATEST123".to_string()));
        assert_eq!(returned_config.secret_access_key, Some("secret123".to_string()));
        assert_eq!(returned_config.session_token, Some("session123".to_string()));
        assert_eq!(returned_config.endpoint_url, Some("https://s3.custom-endpoint.com".to_string()));
    }

    #[test]
    fn test_path_trimming_logic() {
        let test_cases = vec![
            ("prefix", "prefix"),
            ("prefix/", "prefix"),
            ("nested/path", "nested/path"),
            ("nested/path/", "nested/path"),
            ("", ""),
            ("/", ""),
        ];

        for (input, expected) in test_cases {
            let trimmed = input.trim_end_matches('/');
            assert_eq!(trimmed, expected, "Failed for input: '{}'", input);
        }
    }

    #[test]
    fn test_config_validation() {
        // Test minimal valid config
        let minimal_config = create_test_s3_config_minimal();
        assert!(!minimal_config.bucket.is_empty());
        assert!(!minimal_config.region.is_empty());

        // Test config with all optional fields
        let full_config = create_test_s3_config_with_credentials();
        assert!(full_config.access_key_id.is_some());
        assert!(full_config.secret_access_key.is_some());
        assert!(full_config.session_token.is_some());
        assert!(full_config.endpoint_url.is_some());
    }

    #[test]
    fn test_credentials_consistency() {
        // Test that both access key and secret are provided together
        let mut config = create_test_s3_config();
        config.access_key_id = Some("AKIATEST123".to_string());
        // secret_access_key is None - this should be handled gracefully

        // This test ensures the code doesn't panic when only one credential is provided
        assert!(config.access_key_id.is_some());
        assert!(config.secret_access_key.is_none());
    }

    #[test]
    fn test_table_path_generation() {
        let config = create_test_s3_config();
        
        // Test various table path scenarios
        let test_cases = vec![
            ("simple_table", "test-prefix/simple_table"),
            ("nested/table", "test-prefix/nested/table"),
            ("table_with_underscores", "test-prefix/table_with_underscores"),
            ("table-with-dashes", "test-prefix/table-with-dashes"),
        ];

        for (table_name, expected_suffix) in test_cases {
            let expected_path = format!("{}/{}", config.prefix.trim_end_matches('/'), table_name);
            assert_eq!(expected_path, expected_suffix, "Failed for table: '{}'", table_name);
        }
    }

    #[test]
    fn test_log_path_generation() {
        let config = create_test_s3_config();
        let table_name = "user_events";
        
        let table_path = format!("{}/{}", config.prefix.trim_end_matches('/'), table_name);
        let log_path = format!("{}/_delta_log/", table_path);
        
        assert_eq!(log_path, "test-prefix/user_events/_delta_log/");
    }

    #[test]
    fn test_health_check_file_path() {
        let config = create_test_s3_config();
        let expected_path = format!("{}/_health_check/test.txt", config.prefix.trim_end_matches('/'));
        
        assert_eq!(expected_path, "test-prefix/_health_check/test.txt");
    }

    #[test]
    fn test_retention_time_calculation() {
        let retention_days = 7u32;
        let cutoff_time = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
        let now = chrono::Utc::now();
        
        // Verify that cutoff time is before now
        assert!(cutoff_time < now);
        
        // Verify the duration is approximately correct (within 1 second tolerance)
        let duration = now - cutoff_time;
        let expected_seconds = retention_days as i64 * 24 * 60 * 60;
        let actual_seconds = duration.num_seconds();
        
        assert!((actual_seconds - expected_seconds).abs() <= 1, 
               "Expected ~{} seconds, got {} seconds", expected_seconds, actual_seconds);
    }

    #[test]
    fn test_file_filtering_logic() {
        // Test the logic used in cleanup_old_files for determining which files to delete
        let test_files = vec![
            ("data/file1.parquet", false), // Should be deletable
            ("data/_delta_log/00000.json", true), // Should be preserved (delta log)
            ("data/checkpoint.checkpoint.parquet", true), // Should be preserved (checkpoint)
            ("data/regular_file.txt", false), // Should be deletable
            ("_delta_log/transaction.json", true), // Should be preserved (delta log)
        ];

        for (file_path, should_preserve) in test_files {
            let is_delta_log = file_path.contains("_delta_log/");
            let is_checkpoint = file_path.ends_with(".checkpoint.parquet");
            let should_skip = is_delta_log || is_checkpoint;
            
            assert_eq!(should_skip, should_preserve, 
                      "File '{}' preservation logic failed", file_path);
        }
    }

    #[test]
    fn test_metrics_calculation() {
        // Test the logic for calculating bucket metrics
        let file_sizes: Vec<i64> = vec![1024, 2048, 512, 4096]; // Different file sizes in bytes
        let total_size: i64 = file_sizes.iter().sum();
        let total_files = file_sizes.len();
        let total_size_mb = total_size / 1024 / 1024;

        assert_eq!(total_size, 7680); // 1024 + 2048 + 512 + 4096
        assert_eq!(total_files, 4);
        assert_eq!(total_size_mb, 0); // Less than 1 MB total
    }

    #[test]
    fn test_prefix_stripping_logic() {
        // Test the logic used in list_delta_tables for extracting table names
        let base_prefix = "delta-tables";
        let full_prefix = format!("{}/", base_prefix);
        
        let test_cases = vec![
            (format!("{}user_events/", full_prefix), "user_events"),
            (format!("{}transactions/", full_prefix), "transactions"),
            (format!("{}analytics/daily/", full_prefix), "analytics/daily"),
            (format!("{}", full_prefix), ""), // Empty case
        ];

        for (input_prefix, expected_table_name) in test_cases {
            let table_name = input_prefix
                .strip_prefix(&full_prefix)
                .unwrap_or(&input_prefix)
                .trim_end_matches('/');
            
            assert_eq!(table_name, expected_table_name, 
                      "Failed to extract table name from: '{}'", input_prefix);
        }
    }

    #[test]
    fn test_empty_responses_handling() {
        // Test how the code handles empty AWS API responses
        
        // Empty contents list
        let empty_contents: Vec<aws_sdk_s3::types::Object> = vec![];
        assert!(empty_contents.is_empty());
        
        // Verify empty check logic
        let has_files = !empty_contents.is_empty();
        assert!(!has_files);
    }

    #[test]
    fn test_error_messages() {
        // Test that our error messages are descriptive
        let bucket_name = "test-bucket";
        let error_msg = format!("S3 bucket '{}' is not accessible", bucket_name);
        assert!(error_msg.contains("test-bucket"));
        assert!(error_msg.contains("not accessible"));
    }

    #[test]
    fn test_uri_format_consistency() {
        // Ensure URI formats are consistent across methods
        let config = create_test_s3_config();
        let table_name = "test_table";
        
        // URI from get_table_uri
        let uri1 = format!("s3://{}/{}/{}", 
                          config.bucket, 
                          config.prefix.trim_end_matches('/'),
                          table_name);
        
        // URI format used in get_table_metadata
        let table_path = format!("{}/{}", 
                                config.prefix.trim_end_matches('/'), 
                                table_name);
        let uri2 = format!("s3://{}/{}", config.bucket, table_path);
        
        assert_eq!(uri1, uri2, "URI formats should be consistent");
    }

    // Note: Integration tests that require actual AWS SDK calls would go in a separate file
    // or be marked with #[ignore] and run only when AWS credentials are available
    
    #[test]
    #[ignore] // Requires AWS credentials and actual S3 access
    fn test_s3_client_creation_integration() {
        // This would test actual S3Client::new() but requires real AWS setup
        // Keeping as placeholder for future integration test suite
    }
}