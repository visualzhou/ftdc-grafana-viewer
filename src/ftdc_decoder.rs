use crate::varint::VarintReader;
use crate::{FtdcDocumentTS, FtdcError, FtdcTimeSeries, MetricValue};
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
    pub keys: Vec<(String, Bson)>,
    pub timestamp: SystemTime,
    // Each metric (key) is a vector.
    // Each sample (delta) is a sub-vector of n_deltas + 1, including the reference doc.
}

/// Parses one BSON document containing a type 1 FTDC metric chunk
pub struct ChunkParser;

impl ChunkParser {
    // Helper function to convert Bson values to i64 more accurately
    fn bson_to_i64(value: &Bson) -> i64 {
        match value {
            Bson::DateTime(dt) => dt.timestamp_millis(),
            Bson::Int32(v) => i64::from(*v),
            Bson::Int64(v) => *v,
            Bson::Double(v) => *v as i64,
            Bson::Boolean(v) => i64::from(*v),
            // For other types where direct casting doesn't make sense, return 0
            _ => 0,
        }
    }

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
        let Some(Bson::Binary(metrics_chunk)) = doc.get("data") else {
            return Err(FtdcError::Format(
                "No 'data' field found in the document".to_string(),
            ));
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
        let compressed_chunk = metrics_chunk.bytes.clone();

        // Decompress the data
        let mut decoder = flate2::read::ZlibDecoder::new(&compressed_chunk[4..]);
        let mut decompressed_chunk = Vec::new();
        decoder
            .read_to_end(&mut decompressed_chunk)
            .map_err(|e| FtdcError::Compression(format!("ZLIB decompression error: {e}")))?;

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
                        "Failed to parse reference document: {e}"
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
        Ok(chunk)
    }

    // Use underscore as path separator.
    // This avoids conflicts with field names that might contain dots
    const PATH_SEP: &str = "_";

    fn extract_keys_recursive(
        &self,
        doc: &RawDocument,
        prefix: &str,
        keys: &mut Vec<(String, Bson)>,
    ) -> Result<()> {
        for element_result in doc {
            let (key, value) = element_result
                .map_err(|e| FtdcError::Format(format!("Failed to read document element: {e}")))?;

            // Check if the key is empty, print it
            if key.is_empty() {
                return Err(FtdcError::Format(format!("Empty key: prefix={prefix}")));
            }

            let escaped_key = key
                .to_string()
                .trim()
                .replace([' ', '.', '-', ',', '(', ')'], "_");

            let current_path = if prefix.is_empty() {
                escaped_key
            } else {
                format!("{}{}{}", prefix, Self::PATH_SEP, escaped_key)
            };

            self.extract_keys_from_raw_value(&value, &current_path, keys)?;
        }
        Ok(())
    }

    fn to_bson(value: &RawBsonRef) -> Result<Bson> {
        Bson::try_from(*value)
            .map_err(|e| FtdcError::Format(format!("Failed to convert RawBsonRef to Bson: {e}")))
    }

    // Helper method to extract keys from a RawBsonRef value
    fn extract_keys_from_raw_value(
        &self,
        value: &RawBsonRef,
        path: &str,
        keys: &mut Vec<(String, Bson)>,
    ) -> Result<()> {
        match value {
            RawBsonRef::String(_) | RawBsonRef::ObjectId(_) => Ok(()),
            // For numeric types, add the key to the list
            RawBsonRef::Double(_) => {
                keys.push((path.to_string(), Self::to_bson(value)?));
                Ok(())
            }
            RawBsonRef::Int32(_) => {
                keys.push((path.to_string(), Self::to_bson(value)?));
                Ok(())
            }
            RawBsonRef::Int64(_) => {
                keys.push((path.to_string(), Self::to_bson(value)?));
                Ok(())
            }
            RawBsonRef::Decimal128(_) => {
                keys.push((path.to_string(), Self::to_bson(value)?));
                Ok(())
            }
            RawBsonRef::Boolean(_) => {
                keys.push((path.to_string(), Self::to_bson(value)?));
                Ok(())
            }
            RawBsonRef::DateTime(_) => {
                keys.push((path.to_string(), Self::to_bson(value)?));
                Ok(())
            }
            // Timestamp counts as two fields
            RawBsonRef::Timestamp(_) => {
                keys.push((
                    format!("{}{}{}", path, Self::PATH_SEP, "t"),
                    Self::to_bson(value)?,
                )); // time component
                keys.push((
                    format!("{}{}{}", path, Self::PATH_SEP, "i"),
                    Self::to_bson(value)?,
                )); // increment component
                Ok(())
            }
            // Recursively process nested documents
            RawBsonRef::Document(subdoc) => self.extract_keys_recursive(subdoc, path, keys),
            // Handle arrays
            RawBsonRef::Array(arr) => {
                for (i, item_result) in arr.into_iter().enumerate() {
                    let item = item_result.map_err(|e| {
                        FtdcError::Format(format!("Failed to read array item: {e}"))
                    })?;
                    let array_path = format!("{}{}{}", path, Self::PATH_SEP, i);
                    self.extract_keys_from_raw_value(&item, &array_path, keys)?;
                }
                Ok(())
            }
            // Return error for other unhandled BSON types
            _ => Err(FtdcError::Format(format!(
                "Unhandled BSON type for key: {path} {value:?}"
            ))),
        }
    }

    // Decodes a chunk into a vector of "metric values" (timestamp / value pairs)
    // Each sub-vector contains the metrics for a single sample.
    pub fn decode_chunk_values(&self, chunk: &Chunk) -> Result<Vec<MetricValue>> {
        let mut reader = VarintReader::new(&chunk.deltas);
        let mut final_values: Vec<MetricValue> = Vec::new();
        // For each metric vector
        let mut zero_count = 0u64;

        for key in &chunk.keys {
            let mut current_timestamp = chunk.timestamp;
            let mut prev_value = Self::bson_to_i64(&key.1);

            // First, insert the first sample from the reference doc
            // 'key' is tuple of (string name, bson value)
            let metric_value = MetricValue {
                name: key.0.clone(),
                timestamp: current_timestamp,
                value: prev_value as f64,
            };
            final_values.push(metric_value);

            let mut expanded_values: Vec<u64> = Vec::new();
            let mut value = 0u64;

            // Now generate the rest of the samples for this metric.
            // First, create a vector of deltas.
            for _ in 0..chunk.n_deltas {
                // RLE for zeros.
                if zero_count == 0 {
                    value = reader.read()?;
                    if value == 0 {
                        // Found a zero, next value is the count
                        zero_count = reader.read()?;
                    }
                } else {
                    // We're filling in zero's.
                    zero_count -= 1;
                    assert_eq!(value, 0);
                }

                expanded_values.push(value);
            }

            // Now apply those deltas and generate all the MetricValues.
            for delta in expanded_values {
                let value = prev_value.checked_add(delta as i64);
                current_timestamp = current_timestamp
                    .checked_add(Duration::from_secs(1))
                    .unwrap();
                let metric_value = MetricValue {
                    name: key.0.clone(),
                    timestamp: current_timestamp,
                    value: value.unwrap_or_default() as f64,
                };
                final_values.push(metric_value);
                prev_value = value.unwrap_or_default();
            }
        } // for each metric

        Ok(final_values)
    }

    // Decodes a chunk into a vector of FtdcTimeSeries (a single metric's values over time)
    pub fn decode_time_series(&self, chunk: &Chunk) -> Result<FtdcDocumentTS> {
        let mut metrics_time_series: Vec<FtdcTimeSeries> = Vec::new();
        let mut reader = VarintReader::new(&chunk.deltas);
        let mut zero_count = 0u64;

        // First pass: collect all metrics and find the "start" time series
        let mut start_time_series_index = None;

        for (i, key) in chunk.keys.iter().enumerate() {
            // Convert value to i64 using our helper function (handles all types including DateTime)
            let mut current_value = Self::bson_to_i64(&key.1);

            let mut values: Vec<i64> = Vec::new();
            // The initial value is the reference value.
            values.push(current_value);

            // Check if this is the "start" time series
            if key.0 == "start" {
                start_time_series_index = Some(i);
            }

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
            });
        }

        // Check zero count is exhausted
        if zero_count != 0 {
            return Err(FtdcError::Format(
                "Zero count not exhausted at end of parsing".to_string(),
            ));
        }

        // Deltas should be exhausted by now.
        if reader.read().is_ok() {
            return Err(FtdcError::Format(
                "Not all deltas were consumed".to_string(),
            ));
        }

        // Get the index of the "start" time series - it should always exist
        let start_idx = start_time_series_index
            .ok_or_else(|| FtdcError::Format("Missing required 'start' time series".to_string()))?;

        // Convert "start" time series values to SystemTime timestamps
        let timestamps = metrics_time_series[start_idx]
            .values
            .iter()
            .map(|&ts_millis| {
                // Convert milliseconds to SystemTime
                std::time::UNIX_EPOCH + std::time::Duration::from_millis(ts_millis as u64)
            })
            .collect();

        Ok(FtdcDocumentTS {
            metrics: metrics_time_series,
            timestamps,
        })
    }
}
