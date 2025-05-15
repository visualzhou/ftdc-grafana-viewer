use crate::{FtdcDocument, FtdcDocumentTS, FtdcError, MetricValue};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub type VictoriaMetricsResult<T> = Result<T, FtdcError>;

/// Represents metadata about the import process with pre-escaped paths
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportMetadata {
    file_path: Option<String>,
    folder_path: Option<String>,
}

impl ImportMetadata {
    /// Create a new import metadata with pre-escaped paths
    pub fn new(raw_file_path: Option<String>, raw_folder_path: Option<String>) -> Self {
        let escaped_file_path = raw_file_path.as_ref().map(|path| {
            path.replace(' ', "\\ ")
                .replace(',', "\\,")
                .replace('=', "\\=")
        });

        let escaped_folder_path = raw_folder_path.as_ref().map(|path| {
            path.replace(' ', "\\ ")
                .replace(',', "\\,")
                .replace('=', "\\=")
        });

        Self {
            file_path: escaped_file_path,
            folder_path: escaped_folder_path,
        }
    }
}

/// Client for sending metrics to Victoria Metrics
pub struct VictoriaMetricsClient {
    client: Client,
    base_url: String,
    batch_size: usize,
    metadata: ImportMetadata,
}

impl VictoriaMetricsClient {
    /// Create a new Victoria Metrics client
    pub fn new(base_url: String, batch_size: usize, metadata: ImportMetadata) -> Self {
        Self {
            client: Client::new(),
            base_url,
            batch_size,
            metadata,
        }
    }

    /// Set the import metadata
    pub fn set_metadata(&mut self, metadata: ImportMetadata) {
        self.metadata = metadata;
    }

    /// Convert a SystemTime to nanoseconds since UNIX epoch
    fn system_time_to_nanos(time: SystemTime) -> VictoriaMetricsResult<u128> {
        time.duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .map_err(|e| FtdcError::Format(format!("Invalid timestamp: {}", e)))
    }

    /// Convert a metric value to InfluxDB Line Protocol format
    fn metric_to_line_protocol(&self, metric: &MetricValue) -> VictoriaMetricsResult<String> {
        // Use the metric's timestamp
        let timestamp_ns = Self::system_time_to_nanos(metric.timestamp)?;

        // Build the tags section, including file and folder paths if available
        let mut tags = format!("source=mongodb_ftdc");

        // Add file_path tag if available (using pre-escaped path)
        if let Some(escaped_path) = &self.metadata.file_path {
            tags.push_str(&format!(",file_path={}", escaped_path));
        }

        // Add folder_path tag if available (using pre-escaped path)
        if let Some(escaped_path) = &self.metadata.folder_path {
            tags.push_str(&format!(",folder_path={}", escaped_path));
        }

        // Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
        let line: String = format!(
            "{},{} value={} {}",
            metric.name, tags, metric.value, timestamp_ns
        );

        Ok(line)
    }

    /// Convert an FTDC document to InfluxDB Line Protocol format
    pub fn document_to_line_protocol(
        &self,
        doc: &FtdcDocument,
    ) -> VictoriaMetricsResult<Vec<String>> {
        let mut lines = Vec::with_capacity(doc.metrics.len());

        for metric in &doc.metrics {
            let line = self.metric_to_line_protocol(metric)?;
            lines.push(line);
        }

        Ok(lines)
    }

    /// Send metrics to Victoria Metrics
    pub async fn send_metrics(&self, lines: Vec<String>) -> VictoriaMetricsResult<()> {
        // Split lines into batches of batch_size
        for chunk in lines.chunks(self.batch_size) {
            let payload = chunk.join("\n");

            // Debug logging
            println!("\nSending metrics to Victoria Metrics:");
            println!("URL: {}/influx/write", self.base_url);
            println!("Total metrics in this batch: {}", chunk.len());
            println!("First few metrics in line protocol format:");
            for line in chunk.iter().take(3) {
                println!("  {}", line);
            }
            println!("...");

            // Print the exact payload being sent
            //println!("\nExact payload being sent:");
            //println!("{}", payload);

            let response = self
                .client
                .post(&format!("{}/influx/write", self.base_url))
                .header("Content-Type", "text/plain")
                .header("X-Retention-Period", "365d")
                .body(payload.clone())
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status();
                let message = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                println!(
                    "Error response from Victoria Metrics: {} - {}",
                    status, message
                );
                return Err(FtdcError::Server { status, message });
            }

            // Print response details
            println!("\nResponse details:");
            println!("Status: {}", response.status());
            let response_text = response.text().await?;
            println!("Body: {}", response_text);

            // Verify the metrics were received
            let verify_url = format!("{}/api/v1/query?query=mongodb_ftdc_value", self.base_url);
            println!("\nVerifying metrics at: {}", verify_url);
            let verify_response = self.client.get(&verify_url).send().await?;
            if !verify_response.status().is_success() {
                println!("Verification status: {}", verify_response.status());
                let verify_body = verify_response.text().await?;
                println!("Verification response: {}", verify_body);
            }
        }

        Ok(())
    }

    /// Import an FTDC document into Victoria Metrics
    pub async fn import_document(&self, doc: &FtdcDocument) -> VictoriaMetricsResult<()> {
        let lines = self.document_to_line_protocol(doc)?;
        self.send_metrics(lines).await
    }

    pub async fn import_document_ts(&self, _doc: &FtdcDocumentTS) -> VictoriaMetricsResult<()> {
        // TODO(XXX): Implement this
        Ok(())
    }

    /// Clean up all metrics
    ///
    /// This function deletes all metrics with source=mongodb_ftdc.
    pub async fn cleanup_old_metrics(&self) -> VictoriaMetricsResult<()> {
        println!("Cleaning up all metrics...");

        // Delete all metrics with source=mongodb_ftdc
        let delete_url = format!("{}/api/v1/admin/tsdb/delete_series", self.base_url);

        println!("Sending delete request to: {}", delete_url);
        println!("With params: match[]={{source=\"mongodb_ftdc\"}}");

        // Delete all metrics regardless of timestamp
        let response = self
            .client
            .post(&delete_url)
            .query(&[("match[]", "{source=\"mongodb_ftdc\"}")])
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            println!("Error cleaning up metrics: {} - {}", status, message);
            return Err(FtdcError::Server { status, message });
        }

        let response_text = response.text().await?;
        println!(
            "Successfully cleaned up metrics. Response: {}",
            response_text
        );

        Ok(())
    }
}

// Add trait implementation for MetricType to convert to string
impl ToString for crate::MetricType {
    fn to_string(&self) -> String {
        match self {
            crate::MetricType::Double => "Double".to_string(),
            crate::MetricType::Int32 => "Int32".to_string(),
            crate::MetricType::Int64 => "Int64".to_string(),
            crate::MetricType::Boolean => "Boolean".to_string(),
            crate::MetricType::DateTime => "DateTime".to_string(),
            crate::MetricType::Timestamp => "Timestamp".to_string(),
            crate::MetricType::Decimal128 => "Decimal128".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MetricType, MetricValue};
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_metric_to_line_protocol() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(1615000000);

        // Test each metric type
        let test_cases = vec![
            (MetricType::Double, 42.5, "double"),
            (MetricType::Int32, 42.0, "int32"),
            (MetricType::Int64, 42.0, "int64"),
            (MetricType::Boolean, 1.0, "boolean"),
            (MetricType::DateTime, 1615000000000.0, "datetime"),
            (MetricType::Timestamp, 1615000000.0, "timestamp"),
            (MetricType::Decimal128, 42.5, "decimal128"),
        ];
        // Create metadata
        let metadata =
            ImportMetadata::new(Some("test/file.ftdc".to_string()), Some("test".to_string()));
        let client =
            VictoriaMetricsClient::new("http://localhost:8428".to_string(), 1000, metadata);

        for (metric_type, value, _) in test_cases {
            let metric = MetricValue {
                name: "test_metric".to_string(),
                value,
                timestamp,
                metric_type: metric_type.clone(),
            };

            let line = client.metric_to_line_protocol(&metric).unwrap();

            assert!(line.starts_with(&format!("{},source=mongodb_ftdc", "test_metric")));

            // Check for file path and folder path tags
            assert!(line.contains("file_path=test/file.ftdc"));
            assert!(line.contains("folder_path=test"));
        }
    }

    #[test]
    fn test_system_time_to_nanos() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let nanos = VictoriaMetricsClient::system_time_to_nanos(timestamp).unwrap();
        assert_eq!(nanos, 1_000_000_000);
    }
}
