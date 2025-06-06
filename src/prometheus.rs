use crate::{FtdcDocumentTS, FtdcError, Result};
use prometheus_reqwest_remote_write::{Label, Sample, TimeSeries, WriteRequest};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

// Re-using the ImportMetadata from victoria_metrics.rs
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ImportMetadata {
    pub extra_labels: Vec<(String, String)>,
}

impl ImportMetadata {
    /// Add an extra label to the metadata
    pub fn add_extra_label(&mut self, name: String, value: String) {
        self.extra_labels.push((name, value));
    }
}

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
    pub async fn import_document_ts(&self, doc: &FtdcDocumentTS) -> Result<()> {
        // Create the Prometheus remote write request
        let mut timeseries = Vec::with_capacity(doc.metrics.len());

        for metric in &doc.metrics {
            // Skip empty series
            if metric.values.is_empty() || doc.timestamps.is_empty() {
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
            if metric.values.len() != doc.timestamps.len() {
                return Err(FtdcError::Format(
                    "Values and timestamps have different lengths".to_string(),
                ));
            }

            for (value, timestamp) in metric.values.iter().zip(doc.timestamps.iter()) {
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

        // print!("Sending metrics to Prometheus remote write endpoint: ");
        // print!("URL: {} ", self.remote_write_url);
        // print!("Total time series: {} ", write_request.timeseries.len());
        // println!(
        //     "Total samples: {}",
        //     write_request
        //         .timeseries
        //         .iter()
        //         .map(|ts| ts.samples.len())
        //         .sum::<usize>()
        // );

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
