use bson::{Bson, Document};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use zstd::decode_all;

#[tokio::test]
async fn test_parse_metric_document() -> io::Result<()> {
    // Path to the extracted metric document
    let path = Path::new("tests/fixtures/metric-example.bson");

    // Open the file
    let mut file = File::open(path)?;

    // Read the entire document
    let mut doc_data = Vec::new();
    file.read_to_end(&mut doc_data)?;

    // Parse the BSON document
    let doc = bson::from_slice::<Document>(&doc_data).expect("Failed to parse BSON document");

    println!("\n=== Metric Document Analysis ===");

    // Verify this is a metric document
    match doc.get("type") {
        Some(Bson::Int32(1)) => println!("Confirmed: This is a metric document (type: 1)"),
        _ => {
            println!("Error: This is not a metric document");
            return Ok(());
        }
    }

    // Extract the _id (timestamp)
    if let Some(Bson::DateTime(timestamp)) = doc.get("_id") {
        println!("Timestamp: {}", timestamp);
    }

    // Print all keys in the document for debugging
    println!("Document keys: {:?}", doc.keys().collect::<Vec<_>>());

    // Extract the compressed metrics data (looking for 'data' field instead of 'doc')
    if let Some(Bson::Binary(bin)) = doc.get("data") {
        println!("Compressed data size: {} bytes", bin.bytes.len());

        // According to the FTDC format, the first 4 bytes represent the uncompressed size
        if bin.bytes.len() >= 4 {
            let uncompressed_size =
                u32::from_le_bytes([bin.bytes[0], bin.bytes[1], bin.bytes[2], bin.bytes[3]]);
            println!("Uncompressed size: {} bytes", uncompressed_size);

            // The rest is zlib compressed data
            let compressed_data = &bin.bytes[4..];
            println!("Compressed chunk size: {} bytes", compressed_data.len());

            // Try to decompress using zlib
            let mut decoder = flate2::read::ZlibDecoder::new(compressed_data);
            let mut decompressed = Vec::new();
            match decoder.read_to_end(&mut decompressed) {
                Ok(_) => {
                    println!(
                        "Successfully decompressed with zlib: {} bytes",
                        decompressed.len()
                    );

                    // Try to parse the reference document (first part of decompressed data)
                    if let Ok(ref_doc) = bson::from_slice::<Document>(&decompressed) {
                        // Convert document to bytes to get its size
                        let ref_doc_bytes = bson::to_vec(&ref_doc).unwrap_or_default();
                        println!("Reference document size: {} bytes", ref_doc_bytes.len());

                        // Print the first few keys of the reference document
                        println!(
                            "Reference document keys: {:?}",
                            ref_doc.keys().take(10).collect::<Vec<_>>()
                        );

                        // Extract sample count and metric count
                        let remaining = &decompressed[ref_doc_bytes.len()..];
                        if remaining.len() >= 8 {
                            let sample_count = u32::from_le_bytes([
                                remaining[0],
                                remaining[1],
                                remaining[2],
                                remaining[3],
                            ]);
                            let metric_count = u32::from_le_bytes([
                                remaining[4],
                                remaining[5],
                                remaining[6],
                                remaining[7],
                            ]);

                            println!("Sample count: {}", sample_count);
                            println!("Metric count: {}", metric_count);

                            // The rest is the compressed metrics array
                            let compressed_metrics = &remaining[8..];
                            println!(
                                "Compressed metrics array size: {} bytes",
                                compressed_metrics.len()
                            );
                        }
                    } else {
                        println!("Failed to parse reference document from decompressed data");

                        // Print the first few bytes of the decompressed data for debugging
                        let preview_size = std::cmp::min(100, decompressed.len());
                        println!(
                            "First {} bytes of decompressed data: {:?}",
                            preview_size,
                            &decompressed[..preview_size]
                        );
                    }
                }
                Err(e) => {
                    println!("Failed to decompress with zlib: {}", e);

                    // Try with zstd instead
                    match decode_all(compressed_data) {
                        Ok(decompressed) => {
                            println!(
                                "Successfully decompressed with zstd: {} bytes",
                                decompressed.len()
                            );

                            // Print the first few bytes of the decompressed data for debugging
                            let preview_size = std::cmp::min(100, decompressed.len());
                            println!(
                                "First {} bytes of decompressed data: {:?}",
                                preview_size,
                                &decompressed[..preview_size]
                            );
                        }
                        Err(e) => println!("Failed to decompress with zstd: {}", e),
                    }
                }
            }
        }
    } else {
        println!("No 'data' field found in the document");
    }

    Ok(())
}
