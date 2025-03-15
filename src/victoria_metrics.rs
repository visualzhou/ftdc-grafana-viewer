use crate::{FtdcDocument, MetricValue};
use reqwest::{Client, StatusCode};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Error types for Victoria Metrics operations
#[derive(Error, Debug)]
pub enum VictoriaMetricsError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Server error: status={status}, message={message}")]
    Server { status: StatusCode, message: String },

    #[error("Conversion error: {0}")]
    Conversion(String),

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
}

pub type VictoriaMetricsResult<T> = Result<T, VictoriaMetricsError>;

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
            .map_err(|e| VictoriaMetricsError::InvalidTimestamp(e.to_string()))
    }

    /// Convert a metric value to InfluxDB Line Protocol format
    fn metric_to_line_protocol(metric: &MetricValue) -> VictoriaMetricsResult<String> {
        let timestamp_ns = Self::system_time_to_nanos(metric.timestamp)?;
        
        // Sanitize metric name (replace spaces and special chars with underscores)
        let sanitized_name = metric.name.replace(' ', "_").replace('.', "_");
        
        // Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
        // Include the metric name as a tag for better identification in Victoria Metrics
        let line = format!("mongodb_ftdc,metric_type={},metric_name={} value={} {}", 
            metric.metric_type.to_string().to_lowercase(),
            sanitized_name,
            metric.value, 
            timestamp_ns);
            
        Ok(line)
    }

    /// Convert an FTDC document to InfluxDB Line Protocol format
    pub fn document_to_line_protocol(&self, doc: &FtdcDocument) -> VictoriaMetricsResult<Vec<String>> {
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
            
            let response = self.client
                .post(&format!("{}/write", self.base_url))
                .header("Content-Type", "text/plain")
                .body(payload)
                .send()
                .await?;
                
            if !response.status().is_success() {
                let status = response.status();
                let message = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                return Err(VictoriaMetricsError::Server { status, message });
            }
        }
        
        Ok(())
    }

    /// Import an FTDC document into Victoria Metrics
    pub async fn import_document(&self, doc: &FtdcDocument) -> VictoriaMetricsResult<()> {
        let lines = self.document_to_line_protocol(doc)?;
        self.send_metrics(lines).await
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
                metric_type,
            };

            let line = VictoriaMetricsClient::metric_to_line_protocol(&metric).unwrap();
            assert!(line.starts_with(&format!("mongodb_ftdc,metric_type={},metric_name=test_metric value={} ", expected_type, value)));
        }
    }

    #[test]
    fn test_system_time_to_nanos() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let nanos = VictoriaMetricsClient::system_time_to_nanos(timestamp).unwrap();
        assert_eq!(nanos, 1_000_000_000);
    }
} 