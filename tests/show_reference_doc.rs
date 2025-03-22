use bson::{Bson, Document};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

#[tokio::test]
async fn test_show_reference_document() -> io::Result<()> {
    // Path to the decompressed metric data
    let path = Path::new("tests/fixtures/decompressed-metric.bin");

    // Open the file
    let mut file = File::open(path)?;

    // Read the entire file
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    println!("\n=== Reference Document Analysis ===");

    // Get the size of the reference document
    if data.len() < 4 {
        println!("File too small to contain a BSON document");
        return Ok(());
    }

    let doc_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    println!("Reference document size: {} bytes", doc_size);

    if doc_size > data.len() {
        println!("Document size exceeds file size");
        return Ok(());
    }

    // Parse the reference document
    match bson::from_slice::<Document>(&data[..doc_size]) {
        Ok(ref_doc) => {
            println!("Successfully parsed reference document");

            // Print all top-level keys
            println!("\nTop-level keys: {:?}", ref_doc.keys().collect::<Vec<_>>());

            // Print the document in a more readable format
            println!("\n=== Reference Document Structure ===");
            print_document_structure(&ref_doc, 0);

            // Extract sample count and metric count
            let offset = doc_size;
            if data.len() >= offset + 8 {
                let sample_count = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);

                let metric_count = u32::from_le_bytes([
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);

                println!("\nSample count: {}", sample_count);
                println!("Metric count: {}", metric_count);
            }
        }
        Err(e) => {
            println!("Failed to parse reference document: {}", e);
        }
    }

    Ok(())
}

// Helper function to print document structure with indentation
fn print_document_structure(doc: &Document, indent: usize) {
    let indent_str = " ".repeat(indent * 2);

    for (key, value) in doc.iter() {
        match value {
            Bson::Document(subdoc) => {
                println!("{}{}:", indent_str, key);
                print_document_structure(subdoc, indent + 1);
            }
            Bson::Array(arr) => {
                println!("{}{}: [Array with {} items]", indent_str, key, arr.len());
                if !arr.is_empty() {
                    // Print first item as example
                    match &arr[0] {
                        Bson::Document(subdoc) => {
                            println!("{}  First item:", indent_str);
                            print_document_structure(subdoc, indent + 2);
                        }
                        _ => {
                            println!("{}  First item: {:?}", indent_str, arr[0]);
                        }
                    }
                }
            }
            _ => {
                // For other types, just print the key and a short representation of the value
                let value_str = match value {
                    Bson::String(s) => {
                        if s.len() > 50 {
                            format!("String({} chars): \"{}...\"", s.len(), &s[..47])
                        } else {
                            format!("\"{}\"", s)
                        }
                    }
                    Bson::Int32(i) => format!("{}", i),
                    Bson::Int64(i) => format!("{}", i),
                    Bson::Double(d) => format!("{}", d),
                    Bson::Boolean(b) => format!("{}", b),
                    Bson::DateTime(dt) => format!("{}", dt),
                    _ => format!("{:?}", value),
                };
                println!("{}{}: {}", indent_str, key, value_str);
            }
        }
    }
}
