use bson::{Bson, Document};
use flate2::read::ZlibDecoder;
use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::path::Path;

/// Represents the type of FTDC document
#[derive(Debug, PartialEq)]
enum FtdcDocumentType {
    Metadata,
    Metric,
    MetadataDelta,
    Unknown,
}

/// Extracts the FTDC document type from a BSON document
fn get_document_type(doc: &Document) -> FtdcDocumentType {
    match doc.get("type") {
        Some(Bson::Int32(0)) => FtdcDocumentType::Metadata,
        Some(Bson::Int32(1)) => FtdcDocumentType::Metric,
        Some(Bson::Int32(2)) => FtdcDocumentType::MetadataDelta,
        _ => FtdcDocumentType::Unknown,
    }
}

/// Reads a single BSON document from a file
fn read_bson_document(file: &mut File) -> io::Result<Option<(Document, Vec<u8>, usize)>> {
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
        Ok(doc) => Ok(Some((doc, doc_data, doc_size as usize))),
        Err(e) => {
            eprintln!("Error parsing BSON document: {}", e);
            Err(io::Error::new(io::ErrorKind::InvalidData, e))
        }
    }
}

/// Extracts and processes a metric document
fn process_metric_document(doc: &Document) -> io::Result<()> {
    println!("Processing metric document...");

    // Extract the _id (timestamp)
    if let Some(Bson::DateTime(timestamp)) = doc.get("_id") {
        println!("Timestamp: {}", timestamp);
    }

    // Extract the compressed metrics data
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

            // Decompress the data
            let mut decoder = ZlibDecoder::new(compressed_data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;

            println!("Decompressed size: {} bytes", decompressed.len());

            // Save the decompressed data to a file for further analysis
            let output_path = Path::new("tests/fixtures/decompressed-metric.bin");
            let mut file = File::create(output_path)?;
            file.write_all(&decompressed)?;
            println!("Saved decompressed data to: {:?}", output_path);

            // Try to extract the reference document and metrics
            extract_metrics_from_decompressed_data(&decompressed)?;
        }
    } else {
        println!("No 'data' field found in the document");
    }

    Ok(())
}

/// Extracts metrics from decompressed data
fn extract_metrics_from_decompressed_data(data: &[u8]) -> io::Result<()> {
    // The format of the decompressed data according to the FTDC documentation:
    // 1. Reference document (BSON)
    // 2. Sample count (uint32)
    // 3. Metric count (uint32)
    // 4. Compressed metrics array

    // Try to parse the reference document
    let mut offset = 0;

    // First, check if we have a valid BSON document at the beginning
    if data.len() < 5 {
        println!("Decompressed data too small to contain a BSON document");
        return Ok(());
    }

    // Get the size of the reference document
    let doc_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    println!("Reference document size from header: {} bytes", doc_size);

    if doc_size > data.len() {
        println!("Document size exceeds decompressed data size");
        return Ok(());
    }

    // Try to parse the reference document
    match bson::from_slice::<Document>(&data[..doc_size]) {
        Ok(ref_doc) => {
            println!("Successfully parsed reference document");
            println!(
                "Reference document keys: {:?}",
                ref_doc.keys().collect::<Vec<_>>()
            );

            // Move offset past the reference document
            offset += doc_size;

            // Extract sample count and metric count
            if data.len() >= offset + 8 {
                let sample_count = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;

                let metric_count = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;

                println!("Sample count: {}", sample_count);
                println!("Metric count: {}", metric_count);

                // The rest is the compressed metrics array
                let compressed_metrics = &data[offset..];
                println!(
                    "Compressed metrics array size: {} bytes",
                    compressed_metrics.len()
                );

                // Save the compressed metrics array to a file for further analysis
                let output_path = Path::new("tests/fixtures/compressed-metrics-array.bin");
                let mut file = File::create(output_path)?;
                file.write_all(compressed_metrics)?;
                println!("Saved compressed metrics array to: {:?}", output_path);
            }
        }
        Err(e) => {
            println!("Failed to parse reference document: {}", e);

            // Try a different approach - look for BSON document markers
            println!("Trying to find BSON document markers...");

            // Print the first few bytes for debugging
            let preview_size = std::cmp::min(100, data.len());
            println!(
                "First {} bytes of decompressed data: {:?}",
                preview_size,
                &data[..preview_size]
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_ftdc_format_and_chunk_analysis() -> io::Result<()> {
    // Path to the example FTDC file
    let path = Path::new("tests/fixtures/ftdc-metrics-example");

    // Open the file
    let mut file = File::open(path)?;

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
    while let Some((doc, doc_data, size)) = read_bson_document(&mut file)? {
        // Determine document type
        let doc_type = get_document_type(&doc);

        match doc_type {
            FtdcDocumentType::Metadata => {
                println!(
                    "Document #{}: Type: Metadata, Size: {} bytes",
                    doc_index, size
                );
                metadata_count += 1;
            }
            FtdcDocumentType::Metric => {
                println!(
                    "Document #{}: Type: Metric, Size: {} bytes",
                    doc_index, size
                );
                metric_count += 1;

                // Store the first metric document as an example
                if metric_example.is_none() {
                    metric_example = Some(doc_data);
                }

                // Process the first metric document in detail
                if metric_count == 1 {
                    process_metric_document(&doc)?;
                }
            }
            FtdcDocumentType::MetadataDelta => {
                println!(
                    "Document #{}: Type: MetadataDelta, Size: {} bytes",
                    doc_index, size
                );
                metadata_delta_count += 1;
            }
            FtdcDocumentType::Unknown => {
                println!(
                    "Document #{}: Type: Unknown, Size: {} bytes",
                    doc_index, size
                );
                unknown_count += 1;
            }
        }

        doc_index += 1;
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
