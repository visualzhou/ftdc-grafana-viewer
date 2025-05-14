use crate::varint::{decode_varint_ftdc, VarintReader};
use crate::{FtdcError, FtdcTimeSeries, MetricType, MetricValue};
use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use bson::{Bson, Document};
use std::io::Read;
use std::time::{Duration, SystemTime};

pub type Result<T> = std::result::Result<T, FtdcError>;

pub struct Chunk {
    // Original
    pub reference_doc: RawDocumentBuf,
    pub n_keys: u32,
    pub n_deltas: u32,
    pub deltas: Vec<u8>,
    // Decoded
    pub keys: Vec<(String, MetricType, Bson)>,
    pub timestamp: SystemTime,
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
            reference_doc: RawDocumentBuf::new(),
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
        chunk.reference_doc =
            match RawDocumentBuf::from_bytes(decompressed_chunk[..ref_doc_size].to_vec()) {
                Ok(doc) => doc,
                Err(e) => {
                    return Err(FtdcError::Format(format!(
                        "Failed to parse reference document: {}",
                        e
                    )))
                }
            };

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
        doc: &RawDocument,
        prefix: &str,
        keys: &mut Vec<(String, MetricType, Bson)>,
    ) -> Result<()> {
        for element_result in doc.iter() {
            let (key, value) = element_result.map_err(|e| {
                FtdcError::Format(format!("Failed to read document element: {}", e))
            })?;

            // Check if the key is empty, print it
            if key.is_empty() {
                return Err(FtdcError::Format(format!("Empty key: prefix={}", prefix)));
            }

            let current_path = if prefix.is_empty() {
                key.to_string()
            } else {
                format!("{}{}{}", prefix, Self::PATH_SEP, key)
            };

            self.extract_keys_from_raw_value(&value, &current_path, keys)?;
        }
        Ok(())
    }

    fn to_bson(&self, value: &RawBsonRef) -> Result<Bson> {
        Bson::try_from(value.clone())
            .map_err(|e| FtdcError::Format(format!("Failed to convert RawBsonRef to Bson: {}", e)))
    }

    // Helper method to extract keys from a RawBsonRef value
    fn extract_keys_from_raw_value(
        &self,
        value: &RawBsonRef,
        path: &str,
        keys: &mut Vec<(String, MetricType, Bson)>,
    ) -> Result<()> {
        match value {
            RawBsonRef::String(_) | RawBsonRef::ObjectId(_) => {
                println!("Skipping key: {}", path);
                Ok(())
            }
            // For numeric types, add the key to the list
            RawBsonRef::Double(_) => {
                keys.push((path.to_string(), MetricType::Double, self.to_bson(value)?));
                Ok(())
            }
            RawBsonRef::Int32(_) => {
                keys.push((path.to_string(), MetricType::Int32, self.to_bson(value)?));
                Ok(())
            }
            RawBsonRef::Int64(_) => {
                keys.push((path.to_string(), MetricType::Int64, self.to_bson(value)?));
                Ok(())
            }
            RawBsonRef::Decimal128(_) => {
                keys.push((
                    path.to_string(),
                    MetricType::Decimal128,
                    self.to_bson(value)?,
                ));
                Ok(())
            }
            RawBsonRef::Boolean(_) => {
                keys.push((path.to_string(), MetricType::Boolean, self.to_bson(value)?));
                Ok(())
            }
            RawBsonRef::DateTime(_) => {
                keys.push((path.to_string(), MetricType::DateTime, self.to_bson(value)?));
                Ok(())
            }
            // Timestamp counts as two fields
            RawBsonRef::Timestamp(_) => {
                keys.push((
                    format!("{}{}{}", path, Self::PATH_SEP, "t"),
                    MetricType::Timestamp,
                    self.to_bson(value)?, // todo: fix timestamp
                )); // time component
                keys.push((
                    format!("{}{}{}", path, Self::PATH_SEP, "i"),
                    MetricType::Timestamp,
                    self.to_bson(value)?,
                )); // increment component
                Ok(())
            }
            // Recursively process nested documents
            RawBsonRef::Document(subdoc) => self.extract_keys_recursive(subdoc, path, keys),
            // Handle arrays
            RawBsonRef::Array(arr) => {
                for (i, item_result) in arr.into_iter().enumerate() {
                    let item = item_result.map_err(|e| {
                        FtdcError::Format(format!("Failed to read array item: {}", e))
                    })?;
                    let array_path = format!("{}{}{}", path, Self::PATH_SEP, i);
                    self.extract_keys_from_raw_value(&item, &array_path, keys)?;
                }
                Ok(())
            }
            // Return error for other unhandled BSON types
            _ => Err(FtdcError::Format(format!(
                "Unhandled BSON type for key: {} {:?}",
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
            let mut current_timestamp = chunk.timestamp;
            let mut prev_value = key.2.as_i64().unwrap_or_default();

            println!(
                "Working on metric {} with reference value {}",
                key.0, prev_value
            );
            // First, insert the first sample from the reference doc
            // 'key' is tuple of (string name, metric type, bson value)
            let metric_value = MetricValue {
                name: key.0.clone(),
                timestamp: current_timestamp,
                value: prev_value as f64,
                metric_type: key.1.clone(),
            };
            final_values.push(metric_value);
            let starting_final_values_len = final_values.len(); // this should be number of metrics * samples so far
                                                                // Keep going until we have decoded all the samples for this metric.
            while final_values.len() - starting_final_values_len
                < chunk.n_deltas.try_into().unwrap()
            {
                let (mut value, bytes_read) = decode_varint_ftdc(&chunk.deltas[delta_index..])?;
                delta_index += bytes_read;

                if let MetricType::Timestamp = key.1 {
                    // already got the 't', now parse the 'i'
                    let (i_value, bytes_read) = decode_varint_ftdc(&chunk.deltas[delta_index..])?;
                    delta_index += bytes_read;
                    println!("Parsed timestamp: t {} i {}", value, i_value);
                    value += i_value; // todo: this is clearly wrong
                }
                let mut expanded_values: Vec<u64> = Vec::new();
                // Run-length decoding of zeros
                if value == 0 {
                    // Found a zero, next value is the count
                    let (mut zero_count, bytes_read) =
                        decode_varint_ftdc(&chunk.deltas[delta_index..])?;
                    delta_index += bytes_read;

                    // Safety check
                    if final_values.len() - starting_final_values_len + zero_count as usize
                        > chunk.n_deltas.try_into().unwrap()
                    {
                        /*
                        return Err(FtdcError::Compression(format!(
                            "Invalid zero count: {}",
                            zero_count
                        )));
                        */
                        println!("\t\tInvalid zero count: {} with final_values.len {} starting_final_values_len {} chunk.n_deltas {} Skipping.", zero_count,
                            final_values.len(),
                            starting_final_values_len,
                            chunk.n_deltas
                        );
                        zero_count = 1;
                    }
                    // Expand the zeros
                    for _ in 0..zero_count {
                        expanded_values.push(0);
                    }
                    println!("\tFound {} zeros", zero_count);
                } else {
                    expanded_values.push(value);
                    println!("\tFound value {}", value);
                }

                // Delta decoding
                for delta in expanded_values {
                    let value = prev_value.checked_add(delta as i64);
                    current_timestamp = current_timestamp
                        .checked_add(Duration::from_secs(1))
                        .unwrap();
                    let metric_value = MetricValue {
                        name: key.0.clone(),
                        timestamp: current_timestamp,
                        value: value.unwrap_or_default() as f64,
                        metric_type: key.1.clone(),
                    };
                    println!("\t\tAdding metric value: {:?}", metric_value);
                    final_values.push(metric_value);
                    prev_value = value.unwrap_or_default();
                }
            }

            println!("\t\tFinal decompressed values: {}", final_values.len());
        }

        Ok(final_values)
    }

    // Decodes a chunk into a vector of FtdcTimeSeries (a single metric's values over time)
    pub fn decode_time_series(&self, chunk: &Chunk) -> Result<Vec<FtdcTimeSeries>> {
        let mut metrics_time_series: Vec<FtdcTimeSeries> = Vec::new();
        let mut timestamps: Vec<SystemTime> = Vec::new();
        let mut current_timestamp = chunk.timestamp;
        // Construct the timestamp vector; we will clone a copy of this into each FtdcTimeSeries.
        for _ in 0..chunk.n_deltas {
            current_timestamp = current_timestamp
                .checked_add(Duration::from_secs(1))
                .unwrap();
            timestamps.push(current_timestamp);
        }

        let mut reader = VarintReader::new(&chunk.deltas);
        let mut zero_count = 0u64;

        // For each metric, construct its values vector.
        for key in chunk.keys.iter() {
            let mut current_value = key.2.as_i64().unwrap_or_default();
            let mut values: Vec<i64> = Vec::new();
            // The initial value is the reference value.
            values.push(current_value);

            for _ in 0..chunk.n_deltas {
                if zero_count == 0 {
                    let delta = reader.read()?;
                    if delta == 0 {
                        // Consume the zero count
                        zero_count = reader.read()?;
                    }
                    current_value += delta as i64;
                } else {
                    zero_count -= 1;
                }
                values.push(current_value);
            }
            metrics_time_series.push(FtdcTimeSeries {
                name: key.0.clone(),
                values,
                timestamps: timestamps.clone(),
            });
        }
        // Check zero count is exhausted
        if zero_count != 0 {
            return Err(FtdcError::Format(
                "Zero count not exhausted at end of parsing".to_string(),
            ));
        }
        // Deltas should be exhausted by now.
        if !reader.read().is_err() {
            return Err(FtdcError::Format(
                "Not all deltas were consumed".to_string(),
            ));
        }

        return Ok(metrics_time_series);
    }
}
