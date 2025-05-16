use crate::prometheus::ImportMetadata;
use crate::{FtdcDocument, FtdcDocumentTS, FtdcError, MetricValue};
use reqwest::Client;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

pub type VictoriaMetricsResult<T> = Result<T, FtdcError>;

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
        let mut tags = "source=mongodb_ftdc".to_string();

        // Add file_path tag if available (using pre-escaped path)
        if let Some(escaped_path) = &self.metadata.file_path {
            tags.push_str(&format!(",file_path={}", escaped_path));
        }

        // Add folder_path tag if available (using pre-escaped path)
        if let Some(escaped_path) = &self.metadata.folder_path {
            tags.push_str(&format!(",folder_path={}", escaped_path));
        }

        // Add extra labels if any
        for (name, value) in &self.metadata.extra_labels {
            tags.push_str(&format!(",{}={}", name, value));
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
    pub async fn send_metrics(
        &self,
        lines: Vec<String>,
        verbose: bool,
    ) -> VictoriaMetricsResult<()> {
        // Split lines into batches of batch_size
        for chunk in lines.chunks(self.batch_size) {
            let payload = chunk.join("\n");

            // Debug logging only in verbose mode
            if verbose {
                println!("\nSending metrics to Victoria Metrics:");
                println!("URL: {}/influx/write", self.base_url);
                println!("Total metrics in this batch: {}", chunk.len());

                println!("First few metrics in line protocol format:");
                for line in chunk.iter().take(3) {
                    println!("  {}", line);
                }
                println!("...");
            }

            let response = self
                .client
                .post(format!("{}/influx/write", self.base_url))
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

            // Print response details only in verbose mode
            if verbose {
                println!("Response details:");
                println!("Status: {}", response.status());
                let response_text = response.text().await?;
                println!("Body: {}", response_text);
            } else {
                // Still need to read the response body to avoid keeping the connection open
                let _ = response.text().await?;
            }
        }

        Ok(())
    }

    /// Import a document into Victoria Metrics
    pub async fn import_document(
        &self,
        doc: &FtdcDocument,
        verbose: bool,
    ) -> VictoriaMetricsResult<()> {
        let lines = self.document_to_line_protocol(doc)?;
        self.send_metrics(lines, verbose).await
    }

    pub async fn import_document_ts(
        &self,
        doc: &FtdcDocumentTS,
        verbose: bool,
    ) -> VictoriaMetricsResult<()> {
        if doc.metrics.is_empty() || doc.timestamps.is_empty() {
            if verbose {
                println!("Empty document or timestamps, skipping");
            }
            return Ok(());
        }

        // Pre-allocate for all metrics and timestamps
        let total_samples = doc.metrics.len() * doc.timestamps.len();
        let mut lines = Vec::with_capacity(total_samples);

        if verbose {
            println!(
                "Converting {} time series with {} samples each to line protocol",
                doc.metrics.len(),
                doc.timestamps.len()
            );
        }

        for metric in &doc.metrics {
            let metric_name = &metric.name;
            if metric.values.len() != doc.timestamps.len() {
                return Err(FtdcError::Format(format!(
                    "Values and timestamps have different lengths for metric {}: values={}, timestamps={}",
                    metric_name, metric.values.len(), doc.timestamps.len()
                )));
            }

            // Use the metric's name but with timestamps from the document
            for (idx, (value, timestamp)) in
                metric.values.iter().zip(doc.timestamps.iter()).enumerate()
            {
                // Build base tags string
                let mut tags = "source=mongodb_ftdc".to_string();

                // Add file_path tag if available
                if let Some(file_path) = &self.metadata.file_path {
                    tags.push_str(&format!(",file_path={}", file_path));
                }

                // Add folder_path tag if available
                if let Some(folder_path) = &self.metadata.folder_path {
                    tags.push_str(&format!(",folder_path={}", folder_path));
                }

                // Add index tag to uniquely identify each sample
                tags.push_str(&format!(",sample_idx={}", idx));

                // Add extra labels if any
                for (name, value) in &self.metadata.extra_labels {
                    tags.push_str(&format!(",{}={}", name, value));
                }

                // Convert the timestamp to nanoseconds
                let timestamp_ns = Self::system_time_to_nanos(*timestamp)?;

                // Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
                let line: String =
                    format!("{},{} value={} {}", metric_name, tags, *value, timestamp_ns);

                lines.push(line);
            }
        }

        // Send the lines to Victoria Metrics
        self.send_metrics(lines, verbose).await
    }

    /// Clean up all metrics
    ///
    /// This function deletes all metrics with source=mongodb_ftdc.
    pub async fn cleanup_old_metrics(&self, verbose: bool) -> VictoriaMetricsResult<()> {
        if verbose {
            println!("Cleaning up metrics for the current file...");
        }

        // Only delete metrics for the current file_path if available
        if let Some(file_path) = &self.metadata.file_path {
            let delete_url = format!("{}/api/v1/admin/tsdb/delete_series", self.base_url);
            let query = format!("{{source=\"mongodb_ftdc\",file_path=\"{}\"}}", file_path);

            if verbose {
                println!("Sending delete request to: {}", delete_url);
                println!("With params: match[]={}", query);
            }

            // Delete metrics for the specific file path
            let response = self
                .client
                .post(&delete_url)
                .query(&[("match[]", query)])
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
            if verbose {
                println!(
                    "Successfully cleaned up metrics for file '{}'. Response: {}",
                    file_path, response_text
                );
            } else {
                println!("Successfully cleaned up metrics for file '{}'", file_path);
            }
        } else {
            // If no file path is available, print a warning
            println!("Warning: No file path available, skipping cleanup");
        }

        Ok(())
    }
}

// Add trait implementation for MetricType to convert to string
impl fmt::Display for crate::MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            crate::MetricType::Double => write!(f, "Double"),
            crate::MetricType::Int32 => write!(f, "Int32"),
            crate::MetricType::Int64 => write!(f, "Int64"),
            crate::MetricType::Boolean => write!(f, "Boolean"),
            crate::MetricType::DateTime => write!(f, "DateTime"),
            crate::MetricType::Timestamp => write!(f, "Timestamp"),
            crate::MetricType::Decimal128 => write!(f, "Decimal128"),
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
        let mut metadata =
            ImportMetadata::new(Some("test/file.ftdc".to_string()), Some("test".to_string()));
        // Add a test label
        metadata.add_extra_label("test_label".to_string(), "test_value".to_string());

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
            // Check for the extra label
            assert!(line.contains("test_label=test_value"));
        }
    }

    #[test]
    fn test_system_time_to_nanos() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let nanos = VictoriaMetricsClient::system_time_to_nanos(timestamp).unwrap();
        assert_eq!(nanos, 1_000_000_000);
    }
}
