use bson::{ Bson, Document};
use ftdc_importer::{
    ChunkParser, FtdcError, MetricSample, MetricsArrayDecoder,
};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;


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
    let chunk = chunk_parser.parse_chunk_header(&doc).unwrap();
    assert_eq!(chunk.reference_doc.len(), 9);
    assert_eq!(chunk.n_keys, 3479);
    assert_eq!(chunk.n_deltas, 299);
    assert!(!chunk.deltas.is_empty());

    // TODO(XXX): fix this: assertion failed. left: 3476 right: 3479
    assert_eq!(chunk.key_names.len(), chunk.n_keys as usize);
    Ok(())
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
