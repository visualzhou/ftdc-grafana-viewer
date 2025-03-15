use std::path::Path;
use futures::StreamExt;
use ftdc_importer::{FtdcReader, ReaderResult};

const EXAMPLE_FTDC_FILE: &str = "tests/fixtures/ftdc-metrics-example";

#[tokio::test]
async fn test_real_ftdc_file() -> ReaderResult<()> {
    // Ensure the example file exists
    assert!(Path::new(EXAMPLE_FTDC_FILE).exists());

    // Create reader
    let mut reader = FtdcReader::new(EXAMPLE_FTDC_FILE).await?;

    // Test reading first document
    let first_doc = reader.read_next().await?.expect("Should have at least one document");
    assert!(!first_doc.metrics.is_empty(), "First document should contain metrics");

    // Print some info about the first document
    println!("First document timestamp: {:?}", first_doc.timestamp);
    println!("Number of metrics: {}", first_doc.metrics.len());
    println!("Sample metrics:");
    for metric in first_doc.metrics.iter().take(5) {
        println!("  {} = {} ({:?})", metric.name, metric.value, metric.metric_type);
    }

    // Test streaming all documents
    let reader = FtdcReader::new(EXAMPLE_FTDC_FILE).await?;
    let mut stream = reader.stream_documents();

    let mut doc_count = 0;
    let mut total_metrics = 0;
    let mut metric_names = std::collections::HashSet::new();

    while let Some(doc) = stream.next().await {
        let doc = doc?;
        doc_count += 1;
        total_metrics += doc.metrics.len();
        
        // Collect unique metric names
        for metric in &doc.metrics {
            metric_names.insert(metric.name.clone());
        }
    }

    println!("\nProcessed {} documents", doc_count);
    println!("Total metrics: {}", total_metrics);
    println!("Average metrics per document: {:.2}", total_metrics as f64 / doc_count as f64);
    println!("Unique metric names: {}", metric_names.len());
    println!("\nSample metric names:");
    for name in metric_names.iter().take(10) {
        println!("  {}", name);
    }

    Ok(())
} 