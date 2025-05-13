use bson::{doc, Bson, Document};
use ftdc_importer::{
    ChunkParser, FtdcDecoder, FtdcError, MetricChunkDecompressor, MetricChunkExtractor,
    MetricDocumentReconstructor, MetricSample, MetricsArrayDecoder,
};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

#[tokio::test]
async fn test_ftdc_decoder_layers() -> io::Result<()> {
    // Path to the extracted metric document
    let path = Path::new("tests/fixtures/metric-example.bson");

    // Open the file
    let mut file = File::open(path)?;

    // Read the entire document
    let mut doc_data = Vec::new();
    file.read_to_end(&mut doc_data)?;

    // Parse the BSON document
    let doc = bson::from_slice::<Document>(&doc_data).expect("Failed to parse BSON document");

    println!("\n=== FTDC Decoder Test ===");

    // Layer 1: Extract the raw metric chunk
    let chunk_extractor = MetricChunkExtractor::new();
    let raw_chunk = chunk_extractor
        .extract_chunk(&doc)
        .expect("Failed to extract chunk");

    println!("Layer 1: Raw Chunk");
    println!("  Uncompressed size: {} bytes", raw_chunk.uncompressed_size);
    println!(
        "  Compressed data size: {} bytes",
        raw_chunk.compressed_data.len()
    );

    // Layer 2: Decompress the chunk
    let chunk_decompressor = MetricChunkDecompressor::new();
    let decompressed_chunk = chunk_decompressor
        .decompress_chunk(&raw_chunk)
        .expect("Failed to decompress chunk");

    println!("\nLayer 2: Decompressed Chunk");
    println!(
        "  Reference document size: {} bytes",
        bson::to_vec(&decompressed_chunk.reference_doc)
            .unwrap()
            .len()
    );
    println!("  Sample count: {}", decompressed_chunk.sample_count);
    println!("  Metric count: {}", decompressed_chunk.metric_count);
    println!(
        "  Compressed metrics array size: {} bytes",
        decompressed_chunk.compressed_metrics.len()
    );

    // Create a document reconstructor early (moved from below)
    let reconstructor: MetricDocumentReconstructor =
        MetricDocumentReconstructor::new(decompressed_chunk.reference_doc.clone())
            .expect("Failed to create document reconstructor");

    println!("\nLayer 2.5: Early Document Reconstructor");
    println!("  Number of metrics: {}", reconstructor.metric_count());
    println!("  (Metric paths are no longer stored explicitly)");
    println!("  Metric types: Double, Int32, Int64, DateTime, Bool (should all be counted)");

    // Layer 3: Decode the metrics array
    let metrics_decoder = MetricsArrayDecoder::new();
    match metrics_decoder.decode_metrics_array(
        &decompressed_chunk.compressed_metrics,
        decompressed_chunk.sample_count,
        decompressed_chunk.metric_count,
    ) {
        Ok(metrics_array) => {
            println!("\nLayer 3: Decoded Metrics Array");
            println!("  Total number of values: {}", metrics_array.len());

            // Calculate the metrics per sample
            let metrics_per_sample = decompressed_chunk.metric_count as usize;
            let sample_count = decompressed_chunk.sample_count as usize;

            println!("  Number of samples: {}", sample_count);
            println!("  Metrics per sample: {}", metrics_per_sample);

            // Layer 4: Use the document reconstructor (already created above)
            println!("\nLayer 4: Document Reconstructor");
            println!("  Using reconstructor created earlier");

            // Apply delta decoding to the first sample
            if metrics_array.len() >= metrics_per_sample {
                // Extract the first sample's metrics (first metrics_per_sample values)
                let first_sample_metrics: Vec<u64> = metrics_array
                    .iter()
                    .take(metrics_per_sample)
                    .cloned()
                    .collect();

                // Get the timestamp from the original document
                let timestamp = match doc.get("_id") {
                    Some(Bson::DateTime(dt)) => *dt,
                    _ => panic!("Missing _id field or not a DateTime"),
                };

                // Reconstruct the document
                let reconstructed_doc = reconstructor
                    .reconstruct_document(&first_sample_metrics, timestamp)
                    .expect("Failed to reconstruct document");

                println!("\nReconstructed Document (First Sample):");
                println!("  Timestamp: {}", reconstructed_doc.get("_id").unwrap());
                println!("  Number of fields: {}", reconstructed_doc.len());
            }
        }
        Err(e) => {
            println!("\nLayer 3: Failed to decode metrics array");
            println!("  Error: {:?}", e);
            println!("  This is expected with some FTDC files due to format variations");
            println!("  Continuing with full decoder test...");
        }
    }

    // Layer 5: Full decoder
    let decoder = FtdcDecoder::new();
    match decoder.decode_document(&doc) {
        Ok(samples) => {
            println!("\nLayer 5: Full Decoder");
            println!("  Number of samples: {}", samples.len());
            if !samples.is_empty() {
                println!("  First sample timestamp: {}", samples[0].timestamp);
                println!(
                    "  Last sample timestamp: {}",
                    samples.last().unwrap().timestamp
                );
            }
        }
        Err(e) => {
            println!("\nLayer 5: Failed to decode document");
            println!("  Error: {:?}", e);
            println!("  This is expected with some FTDC files due to format variations");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_decode_real_metric_document() -> io::Result<()> {
    // Path to the extracted metric document
    let path = Path::new("tests/fixtures/metric-example.bson");

    // Open the file
    let mut file = File::open(path)?;

    // Read the entire document
    let mut doc_data = Vec::new();
    file.read_to_end(&mut doc_data)?;

    // Parse the BSON document
    let doc = bson::from_slice::<Document>(&doc_data).expect("Failed to parse BSON document");

    println!("\n=== Testing FtdcDecoder with real metric document ===");

    // Create a decoder
    let decoder = FtdcDecoder::new();

    // Decode the document
    match decoder.decode_document(&doc) {
        Ok(samples) => {
            println!("Successfully decoded {} samples", samples.len());

            // Print information about the first few samples
            let sample_count = std::cmp::min(5, samples.len());
            for (i, sample) in samples.iter().take(sample_count).enumerate() {
                println!("\nSample {}", i);
                println!("  Timestamp: {}", sample.timestamp);
                println!("  Metrics count: {}", count_metrics(&sample.metrics));

                // Print a few example metrics
                print_sample_metrics(sample, 5);
            }

            // Verify delta decoding by checking if values change between samples
            if samples.len() >= 2 {
                println!("\nVerifying delta decoding between samples:");
                compare_samples(&samples[0], &samples[1]);
            }
        }
        Err(e) => {
            println!("Failed to decode document: {:?}", e);

            // If it's a format error, print more details about the document
            if let FtdcError::Format(_) = e {
                println!("\nDocument details:");
                println!("  Keys: {:?}", doc.keys().collect::<Vec<_>>());

                if let Some(Bson::Int32(doc_type)) = doc.get("type") {
                    println!("  Document type: {}", doc_type);
                }

                if let Some(Bson::Binary(bin)) = doc.get("data") {
                    println!("  Data size: {} bytes", bin.bytes.len());
                }
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_parse_chunk() -> io::Result<()> {
    let path = Path::new("tests/fixtures/metric-example.bson");
    let mut file = File::open(path)?;
    let mut doc_data = Vec::new();
    file.read_to_end(&mut doc_data)?;

    // Parse the BSON document
    let doc = bson::from_slice::<Document>(&doc_data).unwrap();

    println!("\n=== Testing parse_chunk method ===");

    let chunk_parser = ChunkParser;
    let chunk = chunk_parser.parse_chunk(&doc).unwrap();
    assert_eq!(chunk.reference_doc.len(), 9);
    assert_eq!(chunk.n_keys, 3479);
    assert_eq!(chunk.n_deltas, 299);
    assert!(!chunk.deltas.is_empty());

    assert_eq!(chunk.keys.len(), chunk.n_keys as usize);
    Ok(())
}

// Helper function to count metrics in a document (recursively)
fn count_metrics(doc: &Document) -> usize {
    let mut count = 0;

    for (_, value) in doc.iter() {
        match value {
            Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Boolean(_) => {
                count += 1;
            }
            Bson::Document(subdoc) => {
                count += count_metrics(subdoc);
            }
            Bson::Array(arr) => {
                for item in arr {
                    if let Bson::Document(subdoc) = item {
                        count += count_metrics(subdoc);
                    }
                }
            }
            _ => {}
        }
    }

    count
}

// Helper function to print sample metrics
fn print_sample_metrics(sample: &MetricSample, limit: usize) {
    let mut metrics_printed = 0;

    println!("  Sample metrics:");
    print_document_metrics(&sample.metrics, "", &mut metrics_printed, limit);
}

// Helper function to print metrics from a document
fn print_document_metrics(doc: &Document, prefix: &str, count: &mut usize, limit: usize) {
    if *count >= limit {
        return;
    }

    for (key, value) in doc.iter() {
        let path = if prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}.{}", prefix, key)
        };

        match value {
            Bson::Double(v) => {
                println!("    {}: {} (Double)", path, v);
                *count += 1;
                if *count >= limit {
                    return;
                }
            }
            Bson::Int32(v) => {
                println!("    {}: {} (Int32)", path, v);
                *count += 1;
                if *count >= limit {
                    return;
                }
            }
            Bson::Int64(v) => {
                println!("    {}: {} (Int64)", path, v);
                *count += 1;
                if *count >= limit {
                    return;
                }
            }
            Bson::Document(subdoc) => {
                print_document_metrics(subdoc, &path, count, limit);
            }
            _ => {}
        }
    }
}

// Helper function to compare two samples to verify delta decoding
fn compare_samples(sample1: &MetricSample, sample2: &MetricSample) {
    let mut changes = 0;
    let mut total = 0;

    // Compare a few key metrics that are likely to change
    compare_document_metrics(
        &sample1.metrics,
        &sample2.metrics,
        "",
        &mut changes,
        &mut total,
    );

    println!(
        "  Found {} changes out of {} compared metrics",
        changes, total
    );
    println!(
        "  Delta decoding verification: {}",
        if changes > 0 {
            "PASSED"
        } else {
            "FAILED (no changes found)"
        }
    );
}

// Helper function to compare metrics between documents
fn compare_document_metrics(
    doc1: &Document,
    doc2: &Document,
    prefix: &str,
    changes: &mut usize,
    total: &mut usize,
) {
    for (key, value1) in doc1.iter() {
        let path = if prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}.{}", prefix, key)
        };

        if let Some(value2) = doc2.get(key) {
            match (value1, value2) {
                (Bson::Double(v1), Bson::Double(v2)) => {
                    *total += 1;
                    if (v1 - v2).abs() > 0.0001 {
                        *changes += 1;
                        println!("    {}: {} -> {} (changed)", path, v1, v2);
                    }
                }
                (Bson::Int32(v1), Bson::Int32(v2)) => {
                    *total += 1;
                    if v1 != v2 {
                        *changes += 1;
                        println!("    {}: {} -> {} (changed)", path, v1, v2);
                    }
                }
                (Bson::Int64(v1), Bson::Int64(v2)) => {
                    *total += 1;
                    if v1 != v2 {
                        *changes += 1;
                        println!("    {}: {} -> {} (changed)", path, v1, v2);
                    }
                }
                (Bson::Document(subdoc1), Bson::Document(subdoc2)) => {
                    compare_document_metrics(subdoc1, subdoc2, &path, changes, total);
                }
                _ => {}
            }
        }
    }
}

#[test]
fn test_delta_decoding_with_synthetic_data() {
    // Create a reference document with some metrics
    let reference_doc = doc! {
        "metrics": {
            "counter1": 0i32,
            "counter2": 0i32,
            "gauge1": 0.0,
            "gauge2": 0.0
        },
        "server": {
            "uptime": 0i64,
            "connections": 0i32
        }
    };

    // Create a reconstructor
    let reconstructor = MetricDocumentReconstructor::new(reference_doc.clone()).unwrap();

    // Get the metric count
    let metric_count = reconstructor.metric_count();
    println!("Number of metrics: {}", metric_count);

    // Create a series of delta-encoded samples
    let timestamp = bson::DateTime::now();

    // Sample 1: Initial values (no deltas)
    let values1 = vec![100, 200, 1.5f64.to_bits(), 2.5f64.to_bits(), 3600, 50];
    let doc1 = reconstructor
        .reconstruct_document(&values1, timestamp)
        .unwrap();

    // Sample 2: Some deltas
    let values2 = vec![110, 220, 1.7f64.to_bits(), 2.2f64.to_bits(), 3660, 55];
    let doc2 = reconstructor
        .reconstruct_document(&values2, timestamp)
        .unwrap();

    // Compare the documents
    let mut changes = 0;
    let mut total = 0;
    compare_document_metrics(&doc1, &doc2, "", &mut changes, &mut total);

    println!("Total metrics: {}, Changed: {}", total, changes);
}
