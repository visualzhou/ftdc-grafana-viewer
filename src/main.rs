use anyhow::{Context, Result};
use ftdc_importer::{
    prometheus::PrometheusRemoteWriteClient, reader::FtdcReader,
    victoria_metrics::VictoriaMetricsClient, ImportMetadata,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
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

    /// Verbose output
    #[structopt(short, long)]
    verbose: bool,

    /// Check mode - analyze all metrics in the file without sending
    #[structopt(long)]
    check: bool,

    /// Clean up existing metrics for this specific file before importing
    #[structopt(long)]
    clean: bool,

    /// Extra label to add to all metrics (format: name=value)
    #[structopt(long, number_of_values = 1, multiple = true)]
    extra_label: Vec<String>,
}

/// Run the check mode to analyze FTDC file contents without sending to Victoria Metrics
async fn run_check_mode(reader: &mut FtdcReader) -> Result<()> {
    println!("CHECK MODE: Analyzing all metrics in the file");

    let mut document_count = 0;
    let mut total_metric_count = 0;
    let mut metric_examples = HashMap::new();

    // Process all documents using time series format
    while let Some(doc) = reader
        .read_next_time_series()
        .await
        .context("Failed to read FTDC document in time series format")?
    {
        document_count += 1;
        total_metric_count += doc.metrics.len();

        // Collect statistics
        for metric in &doc.metrics {
            // Store metric example (metrics are guaranteed to be unique)
            metric_examples.insert(metric.name.clone(), metric.clone());
        }

        // Print progress every 10 documents
        if document_count % 10 == 0 {
            println!(
                "Processed {} documents ({} metrics)",
                document_count, total_metric_count
            );
        }
    }

    // Print statistics
    println!("\n=== FTDC File Analysis (Time Series Format) ===");
    println!("Total documents: {}", document_count);
    println!("Total metrics: {}", total_metric_count);
    println!("Unique metric names: {}", metric_examples.len());

    // Print examples of each metric
    println!("\nExample metrics:");
    let mut sorted_examples: Vec<_> = metric_examples.values().collect();
    sorted_examples.sort_by(|a, b| a.name.cmp(&b.name));

    for (i, metric) in sorted_examples.iter().enumerate() {
        if i < 10 {
            // Limit to first 10 examples to avoid verbose output
            println!("\nMetric example {}:", i + 1);
            println!("  Name: {}", metric.name);
            println!("  Sample count: {}", metric.values.len());
            if !metric.values.is_empty() {
                // FtdcTimeSeries appears to only have name and values fields
                println!("  First value: {:?}", metric.values.first());
            }
        }
    }

    // Print all metric names
    println!("\nAll metric names ({} total):", metric_examples.len());
    let mut sorted_metrics: Vec<_> = metric_examples.keys().collect();
    sorted_metrics.sort(); // Sort alphabetically for easier reading

    for (i, name) in sorted_metrics.iter().enumerate() {
        println!("{}. {}", i + 1, name);
    }

    Ok(())
}

/// Import FTDC metrics to a Prometheus compatible endpoint via remote write API
async fn run_import_mode_prometheus(
    reader: &mut FtdcReader,
    client: &PrometheusRemoteWriteClient,
) -> Result<(usize, usize)> {
    let mut document_count = 0;
    let mut metric_count = 0;

    // Process all documents in time series format
    while let Some(doc) = reader.read_next_time_series().await? {
        document_count += 1;
        metric_count += doc.metrics.len();

        let sample_count = doc.metrics.first().map(|m| m.values.len()).unwrap_or(0);

        // print progress
        println!(
            "Sent total {} documents ({} metrics); last chunk had {} samples",
            document_count, metric_count, sample_count
        );

        // Import the document via Prometheus remote write
        client.import_document_ts(&doc).await?;
    }

    Ok((document_count, metric_count))
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    if opt.verbose {
        println!("Importing FTDC file: {:?}", opt.input);
        println!("Victoria Metrics URL: {}", opt.vm_url);
    }

    let start = Instant::now();

    let mut reader = FtdcReader::new(&opt.input)
        .await
        .context("Failed to create FTDC reader")?;

    // Clone vm_url before using it
    let vm_url = opt.vm_url.clone();
    let mut metadata = ImportMetadata::new(None, None);

    // Parse the extra label strings and add them to metadata
    for label_str in &opt.extra_label {
        if let Some(pos) = label_str.find('=') {
            let name = label_str[..pos].trim().to_string();
            let value = label_str[pos + 1..].trim().to_string();
            if !name.is_empty() {
                if opt.verbose {
                    println!("Adding extra label: {}={}", name, value);
                }
                metadata.add_extra_label(name, value);
            } else {
                println!("Warning: Invalid label format (empty name): {}", label_str);
            }
        } else {
            println!("Warning: Invalid label format (missing '='): {}", label_str);
        }
    }

    let client = VictoriaMetricsClient::new(opt.vm_url, metadata.clone());

    if opt.check {
        // Run in check mode without sending metrics
        return run_check_mode(&mut reader).await;
    }

    // Clean up old metrics only if clean flag is specified
    if opt.clean {
        client.cleanup_old_metrics(opt.verbose).await?;
    }

    // Create the Prometheus Remote Write client
    let prometheus_url = format!("{}/api/v1/write", vm_url);
    let prom_client = PrometheusRemoteWriteClient::new(prometheus_url, metadata);
    // Use time series format for Victoria Metrics
    println!("Using time series format for Victoria Metrics");
    let result = run_import_mode_prometheus(&mut reader, &prom_client).await?;

    // Use regular format for Victoria Metrics
    // run_import_mode(&mut reader, &client, opt.verbose).await?

    let (document_count, metric_count) = result;
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

    Ok(())
}
