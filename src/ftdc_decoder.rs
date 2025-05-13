use crate::varint::decode_varint_ftdc;
use crate::{FtdcError, MetricType, MetricValue};
use bson::{Bson, Document};
use std::io::Read;
use std::time::{Duration, SystemTime};

pub type Result<T> = std::result::Result<T, FtdcError>;

pub struct Chunk {
    // Original
    pub reference_doc: Document,
    pub n_keys: u32,
    pub n_deltas: u32,
    pub deltas: Vec<u8>,
    // Decoded
    pub keys: Vec<(String, MetricType)>,
    pub timestamp: SystemTime,
    //pub next_keys_idx: u32,
    // Each metric (key) is a vector.
    // Each sample (delta) is a sub-vector of n_deltas + 1, including the reference doc.
}

/// Parses one BSON document containing a type 1 FTDC metric chunk
pub struct ChunkParser;

impl ChunkParser {
    pub fn parse_chunk_header(&self, doc: &Document) -> Result<Chunk> {
        // metric =
        //     _id : DateTime
        //     type: 1
        //     data : BinData(0) // metrics_chunk

        // Verify this is a metric document (type: 1)
        match doc.get("type") {
            Some(Bson::Int32(1)) => {}
            _ => return Err(FtdcError::Format("Not a metric document".to_string())),
        }

        // Get timestamp from _id
        let timestamp = match doc.get("_id") {
            Some(Bson::DateTime(dt)) => *dt,
            _ => {
                return Err(FtdcError::Format(
                    "Missing _id field or not a DateTime".to_string(),
                ))
            }
        };

        // Extract the binary data
        let metrics_chunk = match doc.get("data") {
            Some(Bson::Binary(bin)) => bin,
            _ => {
                return Err(FtdcError::Format(
                    "No 'data' field found in the document".to_string(),
                ))
            }
        };

        // metrics_chunk = // Not a BSON document, raw bytes
        //     uncompressed_size : uint32_t
        //     compressed_chunk: uint8_t[] // zlib compressed

        // Extract the uncompressed size (first 4 bytes)
        if metrics_chunk.bytes.len() < 4 {
            return Err(FtdcError::Format("Data field too small".to_string()));
        }

        // Create a new chunk
        let mut chunk = Chunk {
            reference_doc: Document::new(),
            n_keys: 0,
            n_deltas: 0,
            deltas: Vec::new(),
            keys: Vec::new(),
            timestamp: timestamp.to_system_time(),
        };

        // Extract the compressed data
        let compressed_chunk = metrics_chunk.bytes.to_vec();

        // Decompress the data
        let mut decoder = flate2::read::ZlibDecoder::new(&compressed_chunk[4..]);
        let mut decompressed_chunk = Vec::new();
        decoder
            .read_to_end(&mut decompressed_chunk)
            .map_err(|e| FtdcError::Compression(format!("ZLIB decompression error: {}", e)))?;

        // decompressed_chunk = // Not a BSON document, raw bytes
        //     reference_document uint8_t[] // a BSON Document -see role_based_collectors_doc
        //     metric_count uint32_t
        //     sample_count uint32_t
        //     compressed_metrics_array uint8_t[]

        let ref_doc_size =
            u32::from_le_bytes(decompressed_chunk[0..4].try_into().unwrap()) as usize;

        if ref_doc_size > decompressed_chunk.len() {
            return Err(FtdcError::Format(
                "Reference document size exceeds decompressed data size".to_string(),
            ));
        }

        // Parse the reference document
        chunk.reference_doc = bson::from_slice::<Document>(&decompressed_chunk[..ref_doc_size])
            .map_err(|e| FtdcError::Format(format!("Failed to parse reference document: {}", e)))?;

        let offset = ref_doc_size;
        if decompressed_chunk.len() < offset + 8 {
            return Err(FtdcError::Format(
                "Decompressed data too small to contain sample and metric counts".to_string(),
            ));
        }

        // Read n_keys (metric count) - first 4 bytes
        chunk.n_keys =
            u32::from_le_bytes(decompressed_chunk[offset..offset + 4].try_into().unwrap());

        // Read n_deltas (sample count) - next 4 bytes
        chunk.n_deltas = u32::from_le_bytes(
            decompressed_chunk[offset + 4..offset + 8]
                .try_into()
                .unwrap(),
        );

        // Extract the deltas array
        chunk.deltas = decompressed_chunk[offset + 8..].to_vec();

        // Extract keys directly
        chunk.keys.clear();
        self.extract_keys_recursive(&chunk.reference_doc, "", &mut chunk.keys)?;
        println!(
            "new chunk refdoc n_keys {} n_deltas {} actual keys {}",
            chunk.n_keys,
            chunk.n_deltas,
            chunk.keys.len()
        );
        Ok(chunk)
    }

    // Use null byte as path separator.
    // This avoids conflicts with field names that might contain dots
    const PATH_SEP: &str = "\x00";

    fn extract_keys_recursive(
        &self,
        doc: &Document,
        prefix: &str,
        keys: &mut Vec<(String, MetricType)>,
    ) -> Result<()> {
        for (key, value) in doc.iter() {
            let current_path = if prefix.is_empty() {
                key.to_string()
            } else {
                format!("{}{}{}", prefix, Self::PATH_SEP, key)
            };

            self.extract_keys_from_value(value, &current_path, keys)?;
        }
        Ok(())
    }

    // Helper method to extract keys from a single BSON value
    fn extract_keys_from_value(
        &self,
        value: &Bson,
        path: &str,
        keys: &mut Vec<(String, MetricType)>,
    ) -> Result<()> {
        let keylen = keys.len();
        match value {
            Bson::String(_) | Bson::ObjectId(_) => {
                // log
                println!("Skipping key: {} {}", path, value);
                Ok(())
            }
            // For numeric types, add the key to the list
            Bson::Double(_) => {
                keys.push((path.to_string(), MetricType::Double));
                Ok(())
            }
            Bson::Int32(_) => {
                keys.push((path.to_string(), MetricType::Int32));
                Ok(())
            }
            Bson::Int64(_) => {
                keys.push((path.to_string(), MetricType::Int64));
                Ok(())
            }
            Bson::Decimal128(_) => {
                keys.push((path.to_string(), MetricType::Decimal128));
                Ok(())
            }
            Bson::Boolean(_) => {
                keys.push((path.to_string(), MetricType::Boolean));
                Ok(())
            }
            Bson::DateTime(_) => {
                keys.push((path.to_string(), MetricType::DateTime));
                Ok(())
            }
            // Timestamp counts as two fields
            Bson::Timestamp(_) => {
                keys.push((
                    format!("{}{}{}", path, Self::PATH_SEP, "t"),
                    MetricType::Timestamp,
                )); // time component
                keys.push((
                    format!("{}{}{}", path, Self::PATH_SEP, "i"),
                    MetricType::Timestamp,
                )); // increment component
                    //println!("Adding 2 keys: {} {}", path, value);
                if keys.len() == keylen {
                    return Err(FtdcError::Format(format!("Key already exists: {}", path)));
                }
                Ok(())
            }
            // Recursively process nested documents
            Bson::Document(subdoc) => self.extract_keys_recursive(subdoc, path, keys),
            // Handle arrays
            Bson::Array(arr) => {
                for (i, item) in arr.iter().enumerate() {
                    let array_path = format!("{}{}{}", path, Self::PATH_SEP, i);
                    self.extract_keys_from_value(item, &array_path, keys)?;
                }
                Ok(())
            }
            // Return error for other unhandled BSON types
            _ => Err(FtdcError::Format(format!(
                "Unhandled BSON type for key: {} {}",
                path, value
            ))),
        }
    }

    // Decodes a chunk into a vector of "metric values" (timestamp / value pairs)
    // Each sub-vector contains the metrics for a single sample.
    pub fn decode_chunk_values(&self, chunk: &Chunk) -> Result<Vec<MetricValue>> {
        let mut delta_index = 0;
        let mut final_values: Vec<MetricValue> = Vec::new();
        // For each metric vector
        for key in chunk.keys.iter() {
            println!("Working on metric {}", key.0);

            // Varint decompression
            let mut decoded_values = Vec::new();
            let mut value_count = 0;

            while value_count < chunk.n_deltas {
                let (value, bytes_read) = decode_varint_ftdc(&chunk.deltas[delta_index..])?;
                decoded_values.push(value);
                delta_index += bytes_read;
                value_count += 1;

                // Safety check to prevent excessive memory usage
                if value_count > 10_000_000 {
                    // 10 million values max
                    return Err(FtdcError::Compression(format!(
                        "Too many values decoded: {}",
                        value_count
                    )));
                }
            }
            println!(
                "\t\tDecoded {} varint values for {}",
                decoded_values.len(),
                key.0
            );

            // 3. Run-length decoding of zeros
            let mut expanded_values = Vec::new();
            let mut j = 0;
            while j < decoded_values.len() {
                let value = decoded_values[j];
                if value == 0 && j + 1 < decoded_values.len() {
                    // Found a zero, next value is the count
                    let count = decoded_values[j + 1] as usize;

                    // Safety check
                    if count > 1_000_000 {
                        // Max 1 million consecutive zeros
                        println!("WARNING: Unreasonable zero count: {}. This may indicate corrupt FTDC data.", count);
                        // Instead of failing, just push a single zero and continue
                        expanded_values.push(0);
                        j += 2;
                        continue;
                    }

                    expanded_values.extend(std::iter::repeat(0u64).take(count));
                    j += 2;
                } else {
                    expanded_values.push(value);
                    j += 1;
                }

                // Safety check
                if expanded_values.len() > 100_000_000 {
                    // 100 million expanded values max
                    return Err(FtdcError::Compression(format!(
                        "Too many expanded values: {}",
                        expanded_values.len()
                    )));
                }
            }

            println!(
                "\t\tExpanded to {} values after RLE decoding",
                expanded_values.len()
            );

            // 4. Delta decoding
            let mut prev_value = 0u64;

            /*
                    // If we have reference values, use them as baseline for the first sample
                    let ref_vals = reference_values;
                    if !ref_vals.is_empty() {
                        prev_value = ref_vals[0];
                        final_values.push(prev_value);
                    }
            */
            let current_timestamp = chunk.timestamp;
            for delta in expanded_values {
                let value = prev_value.checked_add(delta);
                let metric_value = MetricValue {
                    name: key.0.clone(),
                    timestamp: current_timestamp,
                    value: value.unwrap_or_default() as f64,
                    metric_type: key.1.clone(),
                };
                final_values.push(metric_value);
                prev_value = value.unwrap_or_default();
                current_timestamp.checked_add(Duration::from_secs(1));
            }

            println!("\t\tFinal decompressed values: {}", final_values.len());
        }

        Ok(final_values)
    }
}
