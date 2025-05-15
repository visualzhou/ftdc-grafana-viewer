use anyhow::{Context, Result};
use ftdc_importer::{
    reader::FtdcReader, victoria_metrics::VictoriaMetricsClient, FtdcDocument, ImportMetadata,
};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "ftdc-importer",
    about = "Import FTDC files into Victoria Metrics"
)]
struct Opt {
    /// Input FTDC file path
    #[structopt(parse(from_os_str))]
    input: PathBuf,

    /// Victoria Metrics URL (e.g., http://localhost:8428)
    #[structopt(long, default_value = "http://localhost:8428")]
    vm_url: String,

    /// Batch size for sending metrics
    #[structopt(long, default_value = "1000")]
    batch_size: usize,

    /// Verbose output
    #[structopt(short, long)]
    verbose: bool,

    /// Check mode - analyze all metrics in the file without sending
    #[structopt(long)]
    check: bool,
}

/// Run the check mode to analyze FTDC file contents without sending to Victoria Metrics
async fn run_check_mode(reader: &mut FtdcReader, client: &VictoriaMetricsClient) -> Result<()> {
    println!("CHECK MODE: Analyzing all metrics in the file");

    let mut document_count = 0;
    let mut total_metric_count = 0;
    let mut unique_metric_names = HashSet::new();
    let mut metric_type_counts = HashMap::new();
    let mut metric_examples = HashMap::new();

    // Process all documents
    while let Some(doc) = reader
        .read_next()
        .await
        .context("Failed to read FTDC document")?
    {
        document_count += 1;
        total_metric_count += doc.metrics.len();

        // Collect statistics
        for metric in &doc.metrics {
            unique_metric_names.insert(metric.name.clone());

            // Count metric types
            *metric_type_counts
                .entry(format!("{:?}", metric.metric_type))
                .or_insert(0) += 1;

            // Store an example of each unique metric name (first occurrence)
            if !metric_examples.contains_key(&metric.name) {
                metric_examples.insert(metric.name.clone(), metric.clone());
            }
        }

        // Print progress every 100 documents
        if document_count % 100 == 0 {
            println!(
                "Processed {} documents ({} metrics)",
                document_count, total_metric_count
            );
        }
    }

    // Print statistics
    println!("\n=== FTDC File Analysis ===");
    println!("Total documents: {}", document_count);
    println!("Total metrics: {}", total_metric_count);
    println!("Unique metric names: {}", unique_metric_names.len());

    println!("\nMetric types distribution:");
    for (metric_type, count) in &metric_type_counts {
        let percentage = (*count as f64 / total_metric_count as f64) * 100.0;
        println!("  {}: {} ({:.2}%)", metric_type, count, percentage);
    }

    // Print examples of each metric type
    println!("\nExample metrics (one per type):");
    let mut examples_by_type = HashMap::new();
    for metric in metric_examples.values() {
        let type_str = format!("{:?}", metric.metric_type);
        if !examples_by_type.contains_key(&type_str) {
            examples_by_type.insert(type_str, metric);
        }
    }

    for (type_str, metric) in &examples_by_type {
        println!("\n{} example:", type_str);
        println!("  Name: {}", metric.name);
        println!("  Value: {}", metric.value);
        println!("  Timestamp: {:?}", metric.timestamp);

        // Show line protocol format
        let line = client.document_to_line_protocol(&FtdcDocument {
            timestamp: metric.timestamp,
            metrics: vec![(*metric).clone()],
        })?;
        println!("  Line Protocol: {}", line[0]);
    }

    // Print all unique metric names
    println!(
        "\nAll unique metric names ({} total):",
        unique_metric_names.len()
    );
    let mut sorted_metrics: Vec<_> = unique_metric_names.iter().collect();
    sorted_metrics.sort(); // Sort alphabetically for easier reading

    for (i, name) in sorted_metrics.iter().enumerate() {
        println!("{}. {}", i + 1, name);
    }

    Ok(())
}

/// Import FTDC metrics to Victoria Metrics
async fn run_import_mode(
    reader: &mut FtdcReader,
    client: &VictoriaMetricsClient,
    verbose: bool,
) -> Result<(usize, usize)> {
    let mut document_count = 0;
    let mut metric_count = 0;
    let mut last_progress = Instant::now();
    let mut total_metrics_processed = 0;

    // Process all documents
    while let Some(doc) = reader
        .read_next()
        .await
        .context("Failed to read FTDC document")?
    {
        document_count += 1;
        let doc_metric_count = doc.metrics.len();
        metric_count += doc_metric_count;
        total_metrics_processed += doc_metric_count;

        // Print progress every 100 documents or every 5 seconds
        if verbose
            && (document_count % 100 == 0 || last_progress.elapsed() >= Duration::from_secs(5))
        {
            let elapsed = last_progress.elapsed();
            let metrics_per_second = total_metrics_processed as f64 / elapsed.as_secs_f64();

            println!(
                "Processed {} documents ({} metrics) - {:.2} metrics/sec",
                document_count, metric_count, metrics_per_second
            );

            last_progress = Instant::now();
            total_metrics_processed = 0;
        }

        // Convert and import the document
        client.import_document(&doc).await.with_context(|| {
            format!(
                "Failed to import document {} with {} metrics",
                document_count, doc_metric_count
            )
        })?;
    }

    Ok((document_count, metric_count))
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    if opt.verbose {
        println!("Importing FTDC file: {:?}", opt.input);
        println!("Victoria Metrics URL: {}", opt.vm_url);
        println!("Batch size: {}", opt.batch_size);
    }

    let start = Instant::now();
    let file_path = opt.input.to_string_lossy().to_string();
    let folder_path = opt
        .input
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| ".".to_string());

    if opt.verbose {
        println!("File path: {}", file_path);
        println!("Folder path: {}", folder_path);
    }

    let mut reader = FtdcReader::new(&opt.input)
        .await
        .context("Failed to create FTDC reader")?;

    // Set file and folder paths for FTDC documents
    reader.set_paths(file_path.clone(), folder_path.clone());

    // Clone vm_url before using it
    let vm_url = opt.vm_url.clone();
    let metadata = ImportMetadata::new(Some(file_path.clone()), Some(folder_path.clone()));
    let client = VictoriaMetricsClient::new(opt.vm_url, opt.batch_size, metadata);

    if opt.check {
        // Run in check mode
        run_check_mode(&mut reader, &client).await?;
        println!("\nAnalysis completed in {:.2?}", start.elapsed());
    } else {
        // Clean up old metrics before importing new ones
        client.cleanup_old_metrics().await?;

        // Run in import mode
        let (document_count, metric_count) =
            run_import_mode(&mut reader, &client, opt.verbose).await?;
        let elapsed = start.elapsed();

        println!("Import completed successfully!");
        println!(
            "Processed {} documents with {} metrics in {:.2?}",
            document_count, metric_count, elapsed
        );
        println!(
            "Average processing speed: {:.2} documents/sec",
            document_count as f64 / elapsed.as_secs_f64()
        );

        // Verify metrics in Victoria Metrics
        println!("\nVerifying metrics in Victoria Metrics...");
        let verify_url = format!("{}/api/v1/query?query=mongodb_ftdc_value", vm_url);
        println!("Query URL: {}", verify_url);

        let response = reqwest::get(&verify_url)
            .await
            .context("Failed to query Victoria Metrics")?;

        if response.status().is_success() {
            let body = response.text().await?;
            println!("Victoria Metrics response: {}", body);
        } else {
            println!(
                "Warning: Failed to verify metrics in Victoria Metrics. Status: {}",
                response.status()
            );
        }
    }

    Ok(())
}
