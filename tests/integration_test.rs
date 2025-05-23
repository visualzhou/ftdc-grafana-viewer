use ftdc_importer::{FtdcReader, ImportMetadata, PrometheusRemoteWriteClient, ReaderResult};
use std::path::Path;

const EXAMPLE_FTDC_FILE: &str = "tests/fixtures/ftdc-metrics-example";

#[tokio::test]
async fn test_real_ftdc_file_time_series() -> ReaderResult<()> {
    assert!(Path::new(EXAMPLE_FTDC_FILE).exists());

    let expected_metrics_count = 3479;
    let expected_sample_count = 300;
    let mut reader = FtdcReader::new(EXAMPLE_FTDC_FILE).await?;

    let mut time_series_collection = Vec::new();

    while let Some(doc) = reader.read_next_time_series().await? {
        time_series_collection.push(doc);
    }

    assert_eq!(time_series_collection.len(), 2);
    assert_eq!(
        time_series_collection[0].metrics.len(),
        expected_metrics_count,
    );
    assert_eq!(
        time_series_collection[0].timestamps.len(),
        expected_sample_count
    );
    for metric in time_series_collection[0].metrics.iter() {
        assert_eq!(metric.values.len(), expected_sample_count);
    }

    Ok(())
}

#[tokio::test]
// Don't run this test unless the Victoria Metrics server is running
// command to run:
// cargo test -- --ignored
#[ignore]
async fn test_prometheus_remote_write_client() -> ReaderResult<()> {
    let prometheus_url = "http://localhost:8428/api/v1/write".to_string();

    println!("Using Prometheus server at: {}", prometheus_url);

    let client = PrometheusRemoteWriteClient::new(
        prometheus_url.clone(),
        ImportMetadata::new(
            Some("tests/fixtures/ftdc-metrics-example".to_string()),
            Some("tests/fixtures".to_string()),
        ),
    );

    // Create a reader for the example FTDC file
    assert!(Path::new(EXAMPLE_FTDC_FILE).exists());
    let mut reader: FtdcReader = FtdcReader::new(EXAMPLE_FTDC_FILE).await?;

    let mut doc_count = 0;
    let mut total_series = 0;
    let mut total_samples = 0;

    // Process all time series documents
    while let Some(doc) = reader.read_next_time_series().await? {
        doc_count += 1;

        // Verify we have data
        assert!(!doc.metrics.is_empty(), "Document should contain metrics");

        let doc_series_count = doc.metrics.len();
        let doc_samples = doc.metrics.iter().map(|m| m.values.len()).sum::<usize>();

        total_series += doc_series_count;
        total_samples += doc_samples;

        println!(
            "Document #{}: {} time series with {} samples",
            doc_count, doc_series_count, doc_samples
        );

        if !doc.metrics.is_empty() {
            println!(
                "Sample metric: {} (samples: {})",
                doc.metrics[0].name,
                doc.metrics[0].values.len()
            );
        }

        // Log what we're about to do
        println!(
            "Sending document #{} to Prometheus server at {}",
            doc_count, prometheus_url
        );

        // Send the metrics to our Prometheus server
        let result = client.import_document_ts(&doc).await;

        match &result {
            Ok(_) => {
                println!(
                    "Successfully sent document #{} with {} time series and {} samples to Prometheus server",
                    doc_count, doc_series_count, doc_samples
                );
            }
            Err(e) => {
                println!(
                    "Error sending document #{} to Prometheus: {:?}",
                    doc_count, e
                );
            }
        }
    }

    // Print summary
    println!(
        "\nSummary: Processed {} documents, Total time series: {}, Total samples: {}",
        doc_count, total_series, total_samples
    );
    Ok(())
}
