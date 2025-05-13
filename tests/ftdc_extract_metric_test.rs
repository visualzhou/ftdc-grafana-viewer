use bson::{Bson, Document};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

//
// This file prepares the example BSON metrics for the tests.
//
/// Reads a single BSON document from a file
fn read_bson_document(file: &mut File) -> io::Result<Option<(Document, Vec<u8>)>> {
    // Read document size (first 4 bytes)
    let mut size_buf = [0u8; 4];
    if file.read_exact(&mut size_buf).is_err() {
        return Ok(None); // End of file
    }

    // Get document size (little endian)
    let doc_size = u32::from_le_bytes(size_buf);

    // Read the entire document
    let mut doc_data = vec![0u8; doc_size as usize];
    doc_data[0..4].copy_from_slice(&size_buf);
    file.read_exact(&mut doc_data[4..])?;

    // Parse the BSON document
    match bson::from_slice::<Document>(&doc_data) {
        Ok(doc) => Ok(Some((doc, doc_data))),
        Err(e) => {
            eprintln!("Error parsing BSON document: {}", e);
            Err(io::Error::new(io::ErrorKind::InvalidData, e))
        }
    }
}

#[tokio::test]
async fn test_extract_metric_document() -> io::Result<()> {
    // Path to the example FTDC file
    let path = Path::new("tests/fixtures/ftdc-metrics-example");

    // Open the file
    let mut file = File::open(path)?;

    println!("\n=== Extracting Metric Document ===");

    // Read documents until we find a metric document
    let mut doc_index = 0;
    let mut metric_index = 0;

    while let Some((doc, raw_data)) = read_bson_document(&mut file)? {
        // Check if this is a metric document
        if let Some(Bson::Int32(1)) = doc.get("type") {
            println!(
                "Found metric document #{} at index {}",
                metric_index, doc_index
            );

            // Extract the timestamp
            if let Some(Bson::DateTime(timestamp)) = doc.get("_id") {
                println!("Timestamp: {}", timestamp);

                // Create a filename with the timestamp
                let timestamp_str = timestamp.to_string().replace(":", "-").replace(" ", "_");
                let output_path = format!("tests/fixtures/metric-{}.bson", timestamp_str);

                // Save the raw document
                let mut output_file = File::create(&output_path)?;
                output_file.write_all(&raw_data)?;

                println!("Saved metric document to: {}", output_path);

                // Only extract the first metric document
                if metric_index == 0 {
                    // Also save a pretty-printed JSON version for easier inspection
                    let json_path = format!("tests/fixtures/metric-{}.json", timestamp_str);
                    let json = serde_json::to_string_pretty(&doc)?;
                    let mut json_file = File::create(&json_path)?;
                    json_file.write_all(json.as_bytes())?;

                    println!("Saved JSON representation to: {}", json_path);

                    // Break after finding the first metric document
                    break;
                }
            }

            metric_index += 1;
        }

        doc_index += 1;
    }

    println!("Extraction complete!");

    Ok(())
}
