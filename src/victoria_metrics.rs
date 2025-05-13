use crate::{FtdcDocument, FtdcError, MetricValue};
use reqwest::Client;
use std::time::{SystemTime, UNIX_EPOCH};

pub type VictoriaMetricsResult<T> = Result<T, FtdcError>;

/// Client for sending metrics to Victoria Metrics
pub struct VictoriaMetricsClient {
    client: Client,
    base_url: String,
    batch_size: usize,
}

impl VictoriaMetricsClient {
    /// Create a new Victoria Metrics client
    pub fn new(base_url: String, batch_size: usize) -> Self {
        Self {
            client: Client::new(),
            base_url,
            batch_size,
        }
    }

    /// Convert a SystemTime to nanoseconds since UNIX epoch
    fn system_time_to_nanos(time: SystemTime) -> VictoriaMetricsResult<u128> {
        time.duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .map_err(|e| FtdcError::Format(format!("Invalid timestamp: {}", e)))
    }

    /// Convert a metric value to InfluxDB Line Protocol format
    fn metric_to_line_protocol(metric: &MetricValue) -> VictoriaMetricsResult<String> {
        // Use current time instead of metric's timestamp
        // TODO: Use metric's timestamp instead of current time
        let timestamp_ns = Self::system_time_to_nanos(SystemTime::now())?;

        // Sanitize metric name (replace spaces and special chars with underscores)
        let sanitized_name = metric.name.replace(' ', "_").replace('.', "_");

        // Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
        // Include the metric name as a tag for better identification in Victoria Metrics
        let line = format!(
            "mongodb_ftdc,metric_type={},metric_name={} value={} {}",
            metric.metric_type.to_string().to_lowercase(),
            sanitized_name,
            metric.value,
            timestamp_ns
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
            let line = Self::metric_to_line_protocol(metric)?;
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
            println!("\nExact payload being sent:");
            println!("{}", payload);

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

    /// Clean up old metrics by deleting them
    pub async fn cleanup_old_metrics(&self) -> VictoriaMetricsResult<()> {
        println!("Cleaning up old metrics...");

        // Delete all metrics with mongodb_ftdc prefix
        let delete_url = format!("{}/api/v1/admin/tsdb/delete_series", self.base_url);

        // Create the request with match[] parameter to match all mongodb_ftdc metrics
        let response = self
            .client
            .post(&delete_url)
            .query(&[("match[]", "{__name__=~\"mongodb_ftdc.*\"}")])
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

        println!("Successfully cleaned up old metrics");
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

        for (metric_type, value, expected_type) in test_cases {
            let metric = MetricValue {
                name: "test_metric".to_string(),
                value,
                timestamp,
                metric_type: metric_type.clone(),
            };

            let line = VictoriaMetricsClient::metric_to_line_protocol(&metric).unwrap();

            // Updated assertion to match lowercase metric_type and correct format
            assert!(line.starts_with(&format!(
                "mongodb_ftdc,metric_type={},metric_name=test_metric value={} ",
                expected_type, value
            )));
        }
    }

    #[test]
    fn test_system_time_to_nanos() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let nanos = VictoriaMetricsClient::system_time_to_nanos(timestamp).unwrap();
        assert_eq!(nanos, 1_000_000_000);
    }
}
