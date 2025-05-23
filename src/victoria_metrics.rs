use crate::prometheus::ImportMetadata;
use crate::FtdcError;
use reqwest::Client;
use std::fmt;

pub type VictoriaMetricsResult<T> = Result<T, FtdcError>;

///
/// This file includes the victoria metrics client.
/// It provides functionality for cleaning up metrics in Victoria Metrics.
///
/// Client for sending metrics to Victoria Metrics
pub struct VictoriaMetricsClient {
    client: Client,
    base_url: String,
    metadata: ImportMetadata,
}

impl VictoriaMetricsClient {
    /// Create a new Victoria Metrics client
    #[must_use]
    pub fn new(base_url: String, metadata: ImportMetadata) -> Self {
        Self {
            client: Client::new(),
            base_url,
            metadata,
        }
    }

    /// Set the import metadata
    pub fn set_metadata(&mut self, metadata: ImportMetadata) {
        self.metadata = metadata;
    }

    /// Clean up all metrics
    ///
    /// This function deletes all metrics with `source=mongodb_ftdc`.
    pub async fn cleanup_old_metrics(&self, verbose: bool) -> VictoriaMetricsResult<()> {
        if verbose {
            println!("Cleaning up metrics for the current file...");
        }

        // Only delete metrics for the current file_path if available
        if let Some(file_path) = &self.metadata.file_path {
            let delete_url = format!("{}/api/v1/admin/tsdb/delete_series", self.base_url);
            let query = format!("{{source=\"mongodb_ftdc\",file_path=\"{file_path}\"}}");

            if verbose {
                println!("Sending delete request to: {delete_url}");
                println!("With params: match[]={query}");
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
                println!("Error cleaning up metrics: {status} - {message}");
                return Err(FtdcError::Server { status, message });
            }

            let response_text = response.text().await?;
            if verbose {
                println!(
                    "Successfully cleaned up metrics for file '{file_path}'. Response: {response_text}"
                );
            } else {
                println!("Successfully cleaned up metrics for file '{file_path}'");
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
