use crate::prometheus::ImportMetadata;
use crate::{FtdcError, Result};
use reqwest::Client;

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
    pub async fn cleanup_old_metrics(&self, verbose: bool) -> Result<()> {
        if verbose {
            println!("Cleaning up all mongodb_ftdc metrics...");
        }

        let delete_url = format!("{}/api/v1/admin/tsdb/delete_series", self.base_url);
        let query = "{source=\"mongodb_ftdc\"}".to_string();

        if verbose {
            println!("Sending delete request to: {delete_url}");
            println!("With params: match[]={query}");
        }

        // Delete all mongodb_ftdc metrics
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
            println!("Successfully cleaned up all mongodb_ftdc metrics. Response: {response_text}");
        } else {
            println!("Successfully cleaned up all mongodb_ftdc metrics");
        }

        Ok(())
    }
}
