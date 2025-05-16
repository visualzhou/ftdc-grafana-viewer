use crate::{FtdcDocumentTS, FtdcError};
use prometheus_reqwest_remote_write::{Label, Sample, TimeSeries, WriteRequest};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

// Re-using the ImportMetadata from victoria_metrics.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportMetadata {
    pub file_path: Option<String>,
    pub folder_path: Option<String>,
    pub extra_labels: Vec<(String, String)>,
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
            extra_labels: Vec::new(),
        }
    }

    /// Add an extra label to the metadata
    pub fn add_extra_label(&mut self, name: String, value: String) {
        self.extra_labels.push((name, value));
    }
}

pub type PrometheusResult<T> = Result<T, FtdcError>;

/// Client for sending metrics to Prometheus using the remote write protocol
pub struct PrometheusRemoteWriteClient {
    client: Client,
    remote_write_url: String,
    metadata: ImportMetadata,
}

impl PrometheusRemoteWriteClient {
    /// Create a new Prometheus remote write client
    pub fn new(remote_write_url: String, metadata: ImportMetadata) -> Self {
        Self {
            client: Client::new(),
            remote_write_url,
            metadata,
        }
    }

    /// Convert SystemTime to milliseconds since UNIX epoch (required by Prometheus)
    fn system_time_to_millis(time: &SystemTime) -> i64 {
        time.duration_since(UNIX_EPOCH)
            .expect("Time should be after UNIX epoch")
            .as_millis() as i64
    }

    /// Import an FTDC document time series into Prometheus
    pub async fn import_document_ts(&self, doc: &FtdcDocumentTS) -> PrometheusResult<()> {
        // Create the Prometheus remote write request
        let mut timeseries = Vec::with_capacity(doc.metrics.len());

        for metric in &doc.metrics {
            // Skip empty series
            if metric.values.is_empty() || metric.timestamps.is_empty() {
                continue;
            }

            // Create common labels for this metric
            let mut labels = vec![
                // The metric name is stored as a special __name__ label in Prometheus
                Label {
                    name: "__name__".to_string(),
                    value: metric.name.clone(),
                },
                // Add source label
                Label {
                    name: "source".to_string(),
                    value: "mongodb_ftdc".to_string(),
                },
            ];

            // Add file_path label if available
            if let Some(file_path) = &self.metadata.file_path {
                labels.push(Label {
                    name: "file_path".to_string(),
                    value: file_path.clone(),
                });
            }

            // Add folder_path label if available
            if let Some(folder_path) = &self.metadata.folder_path {
                labels.push(Label {
                    name: "folder_path".to_string(),
                    value: folder_path.clone(),
                });
            }

            // Add extra labels if any
            for (name, value) in &self.metadata.extra_labels {
                labels.push(Label {
                    name: name.clone(),
                    value: value.clone(),
                });
            }

            // Create samples from the time series
            let mut samples = Vec::with_capacity(metric.values.len());

            // Use zip to iterate through values and timestamps together
            if metric.values.len() != metric.timestamps.len() {
                return Err(FtdcError::Format(
                    "Values and timestamps have different lengths".to_string(),
                ));
            }

            for (value, timestamp) in metric.values.iter().zip(metric.timestamps.iter()) {
                let timestamp_millis = Self::system_time_to_millis(timestamp);
                samples.push(Sample {
                    value: *value as f64,
                    timestamp: timestamp_millis,
                });
            }

            // Add this time series to the request
            timeseries.push(TimeSeries { labels, samples });
        }

        let write_request = WriteRequest { timeseries };

        // Skip empty requests
        if write_request.timeseries.is_empty() {
            println!("No metrics to send");
            return Ok(());
        }

        println!("\nSending metrics to Prometheus remote write endpoint:");
        println!("URL: {}", self.remote_write_url);
        println!("Total time series: {}", write_request.timeseries.len());
        println!(
            "Total samples: {}",
            write_request
                .timeseries
                .iter()
                .map(|ts| ts.samples.len())
                .sum::<usize>()
        );

        let request = write_request.build_http_request(
            self.client.clone(),
            &self.remote_write_url,
            "ftdc-importer",
        )?;

        // Send the request
        let response = self.client.execute(request).await?;

        if !response.status().is_success() {
            let status = response.status();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            println!(
                "Error sending to Prometheus remote write endpoint: {} - {}",
                status, message
            );
            return Err(FtdcError::Server { status, message });
        }

        println!("\nRequest successfully sent to Prometheus remote write endpoint");
        println!("Status: {}", response.status());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_system_time_to_millis() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let millis = PrometheusRemoteWriteClient::system_time_to_millis(&timestamp);
        assert_eq!(millis, 1_000);
    }
}
