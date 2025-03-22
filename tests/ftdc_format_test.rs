use bson::{Bson, Document};
use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::path::Path;

#[tokio::test]
async fn test_ftdc_document_types() -> io::Result<()> {
    // Path to the example FTDC file
    let path = Path::new("tests/fixtures/ftdc-metrics-example");

    // Open the file
    let mut file = File::open(path)?;

    // Buffer to read BSON document size
    let mut size_buf = [0u8; 4];

    // Counter for each document type
    let mut metadata_count = 0;
    let mut metric_count = 0;
    let mut metadata_delta_count = 0;
    let mut unknown_count = 0;

    // For storing one metric document
    let mut metric_example: Option<Vec<u8>> = None;

    println!("\n=== FTDC Document Analysis ===");

    // Read documents until EOF
    let mut doc_index = 0;
    while file.read_exact(&mut size_buf).is_ok() {
        // Get document size (little endian)
        let doc_size = u32::from_le_bytes(size_buf);

        // Read the entire document
        let mut doc_data = vec![0u8; doc_size as usize];
        doc_data[0..4].copy_from_slice(&size_buf);
        file.read_exact(&mut doc_data[4..])?;

        // Parse the BSON document
        match bson::from_slice::<Document>(&doc_data) {
            Ok(doc) => {
                // Determine document type
                let doc_type = match doc.get("type") {
                    Some(Bson::Int32(0)) => {
                        metadata_count += 1;
                        "metadata"
                    }
                    Some(Bson::Int32(1)) => {
                        metric_count += 1;
                        // Store the first metric document as an example
                        if metric_example.is_none() {
                            metric_example = Some(doc_data.clone());
                        }
                        "metric"
                    }
                    Some(Bson::Int32(2)) => {
                        metadata_delta_count += 1;
                        "metadata_delta"
                    }
                    _ => {
                        unknown_count += 1;
                        "unknown"
                    }
                };

                println!(
                    "Document #{}: Type: {}, Size: {} bytes",
                    doc_index, doc_type, doc_size
                );
                doc_index += 1;
            }
            Err(e) => {
                println!("Error parsing document #{}: {}", doc_index, e);
                break;
            }
        }
    }

    // Print summary
    println!("\n=== Summary ===");
    println!("Total documents: {}", doc_index);
    println!("Metadata documents: {}", metadata_count);
    println!("Metric documents: {}", metric_count);
    println!("Metadata delta documents: {}", metadata_delta_count);
    println!("Unknown documents: {}", unknown_count);

    // Save one metric document as an example if found
    if let Some(metric_data) = metric_example {
        let example_path = Path::new("tests/fixtures/metric-example.bson");
        let mut file = BufWriter::new(File::create(example_path)?);
        file.write_all(&metric_data)?;
        println!("\nSaved one metric document to: {:?}", example_path);
    }

    Ok(())
}
