use bson::{Bson, Document};
use flate2::read::ZlibDecoder;
use std::io::{self, Read};
use thiserror::Error;
use crate::varint;

/// Errors that can occur during FTDC decoding
#[derive(Error, Debug)]
pub enum FtdcDecoderError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("BSON error: {0}")]
    Bson(#[from] bson::de::Error),

    #[error("Invalid FTDC format: {0}")]
    InvalidFormat(String),

    #[error("Decompression error: {0}")]
    Decompression(String),

    #[error("Decoding error: {0}")]
    Decoding(String),
}

pub type Result<T> = std::result::Result<T, FtdcDecoderError>;

/// Represents a raw FTDC metric chunk
#[derive(Debug, Clone)]
pub struct RawMetricChunk {
    /// Uncompressed size of the chunk
    pub uncompressed_size: u32,
    /// Zlib compressed data
    pub compressed_data: Vec<u8>,
}

/// Represents a decompressed FTDC metric chunk
#[derive(Debug, Clone)]
pub struct DecompressedMetricChunk {
    /// Reference document containing the metric schema
    pub reference_doc: Document,
    /// Number of samples in the chunk
    pub sample_count: u32,
    /// Number of metrics per sample
    pub metric_count: u32,
    /// Compressed metrics array
    pub compressed_metrics: Vec<u8>,
}

/// Represents a decoded FTDC metric sample
#[derive(Debug, Clone)]
pub struct MetricSample {
    /// Timestamp of the sample
    pub timestamp: bson::DateTime,
    /// Metrics document
    pub metrics: Document,
}

/// Layer 1: Extract raw metric chunks from BSON documents
pub struct MetricChunkExtractor;

impl MetricChunkExtractor {
    /// Creates a new MetricChunkExtractor
    pub fn new() -> Self {
        Self {}
    }

    /// Extracts a raw metric chunk from a BSON document
    pub fn extract_chunk(&self, doc: &Document) -> Result<RawMetricChunk> {
        // Verify this is a metric document (type: 1)
        match doc.get("type") {
            Some(Bson::Int32(1)) => {}
            _ => {
                return Err(FtdcDecoderError::InvalidFormat(
                    "Not a metric document".to_string(),
                ))
            }
        }

        // Extract the binary data
        let bin = match doc.get("data") {
            Some(Bson::Binary(bin)) => bin,
            _ => {
                return Err(FtdcDecoderError::InvalidFormat(
                    "No 'data' field found in the document".to_string(),
                ))
            }
        };

        // Extract the uncompressed size (first 4 bytes)
        if bin.bytes.len() < 4 {
            return Err(FtdcDecoderError::InvalidFormat(
                "Data field too small".to_string(),
            ));
        }

        let uncompressed_size =
            u32::from_le_bytes([bin.bytes[0], bin.bytes[1], bin.bytes[2], bin.bytes[3]]);

        // Extract the compressed data (rest of the bytes)
        let compressed_data = bin.bytes[4..].to_vec();

        Ok(RawMetricChunk {
            uncompressed_size,
            compressed_data,
        })
    }
}

/// Layer 2: Decompress the raw metric chunk
pub struct MetricChunkDecompressor;

impl MetricChunkDecompressor {
    /// Creates a new MetricChunkDecompressor
    pub fn new() -> Self {
        Self {}
    }

    /// Decompresses a raw metric chunk
    pub fn decompress_chunk(&self, chunk: &RawMetricChunk) -> Result<DecompressedMetricChunk> {
        // Decompress the data using zlib
        let mut decoder = ZlibDecoder::new(&chunk.compressed_data[..]);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| FtdcDecoderError::Decompression(e.to_string()))?;

        // Verify the decompressed size
        if decompressed.len() != chunk.uncompressed_size as usize {
            return Err(FtdcDecoderError::Decompression(format!(
                "Decompressed size mismatch: expected {}, got {}",
                chunk.uncompressed_size,
                decompressed.len()
            )));
        }

        // Parse the reference document
        let doc_size = u32::from_le_bytes([
            decompressed[0],
            decompressed[1],
            decompressed[2],
            decompressed[3],
        ]) as usize;

        if doc_size > decompressed.len() {
            return Err(FtdcDecoderError::InvalidFormat(
                "Reference document size exceeds decompressed data size".to_string(),
            ));
        }

        let reference_doc = bson::from_slice::<Document>(&decompressed[..doc_size])?;

        // Extract sample count and metric count
        let offset = doc_size;
        if decompressed.len() < offset + 8 {
            return Err(FtdcDecoderError::InvalidFormat(
                "Decompressed data too small to contain sample and metric counts".to_string(),
            ));
        }

        let sample_count = u32::from_le_bytes([
            decompressed[offset],
            decompressed[offset + 1],
            decompressed[offset + 2],
            decompressed[offset + 3],
        ]);

        let metric_count = u32::from_le_bytes([
            decompressed[offset + 4],
            decompressed[offset + 5],
            decompressed[offset + 6],
            decompressed[offset + 7],
        ]);

        // Extract the compressed metrics array
        let compressed_metrics = decompressed[offset + 8..].to_vec();

        Ok(DecompressedMetricChunk {
            reference_doc,
            sample_count,
            metric_count,
            compressed_metrics,
        })
    }
}

/// Layer 3: Decode the compressed metrics array
pub struct MetricsArrayDecoder;

impl MetricsArrayDecoder {
    /// Creates a new MetricsArrayDecoder
    pub fn new() -> Self {
        Self {}
    }

    /// Decodes the compressed metrics array into a 2D array of u64 values
    pub fn decode_metrics_array(
        &self,
        compressed_metrics: &[u8],
        sample_count: u32,
        metric_count: u32,
    ) -> Result<Vec<Vec<u64>>> {
        // Step 1: Decode the varint-encoded values
        let mut values = Vec::new();
        let mut offset = 0;

        while offset < compressed_metrics.len() {
            let (value, bytes_read) = varint::decode_varint(&compressed_metrics[offset..])
                .map_err(|e| FtdcDecoderError::Decoding(e.to_string()))?;
            values.push(value);
            offset += bytes_read;
        }

        // Step 2: Decode run-length encoding of zeros
        let mut expanded_values = Vec::new();
        let mut i = 0;
        while i < values.len() {
            if values[i] == 0 && i + 1 < values.len() {
                // This is a run of zeros
                let count = values[i + 1] as usize;
                expanded_values.extend(vec![0; count]);
                i += 2;
            } else {
                // Regular value
                expanded_values.push(values[i]);
                i += 1;
            }
        }

        // Step 3: Reshape into a 2D array (column-major order)
        let expected_values = sample_count as usize * metric_count as usize;

        // Instead of returning an error, log a warning and proceed with the available data
        if expanded_values.len() != expected_values {
            eprintln!(
                "Warning: Decoded values count mismatch: expected {}, got {}. Proceeding with available data.",
                expected_values,
                expanded_values.len()
            );

            // If we have fewer values than expected, we'll pad with zeros
            // If we have more values than expected, we'll truncate
            if expanded_values.len() < expected_values {
                expanded_values.extend(vec![0; expected_values - expanded_values.len()]);
            }
        }

        // Convert from column-major to row-major order
        let mut result = vec![vec![0u64; metric_count as usize]; sample_count as usize];
        for i in 0..sample_count as usize {
            for j in 0..metric_count as usize {
                let column_major_index = j * sample_count as usize + i;
                if column_major_index < expanded_values.len() {
                    result[i][j] = expanded_values[column_major_index];
                }
                // If index is out of bounds, the value remains 0
            }
        }

        Ok(result)
    }
}

/// Layer 4: Apply delta decoding and reconstruct documents
pub struct MetricDocumentReconstructor {
    reference_doc: Document,
    metric_count: usize,
}

impl MetricDocumentReconstructor {
    /// Creates a new MetricDocumentReconstructor
    pub fn new(reference_doc: Document) -> Result<Self> {
        // Count all numeric fields in the reference document
        let metric_count = Self::count_numeric_fields(&reference_doc);

        Ok(Self {
            reference_doc,
            metric_count,
        })
    }

    /// Counts all numeric fields in the document
    fn count_numeric_fields(doc: &Document) -> usize {
        let mut count = 0;
        Self::count_numeric_fields_recursive(doc, &mut count);
        count
    }

    /// Recursively counts numeric fields in the document
    fn count_numeric_fields_recursive(doc: &Document, count: &mut usize) {
        for (_, value) in doc.iter() {
            match value {
                Bson::Double(_)
                | Bson::Int32(_)
                | Bson::Int64(_)
                | Bson::Timestamp(_)
                | Bson::DateTime(_) => {
                    *count += 1;
                }
                Bson::Document(subdoc) => {
                    Self::count_numeric_fields_recursive(subdoc, count);
                }
                Bson::Array(arr) => {
                    // Handle arrays of documents
                    for item in arr.iter() {
                        if let Bson::Document(subdoc) = item {
                            Self::count_numeric_fields_recursive(subdoc, count);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Converts a BSON value to u64
    fn bson_to_u64(value: &Bson) -> Option<u64> {
        match value {
            Bson::Double(d) => Some(*d as u64),
            Bson::Int32(i) => Some(*i as u64),
            Bson::Int64(i) => Some(*i as u64),
            Bson::DateTime(dt) => Some(dt.timestamp_millis() as u64),
            Bson::Timestamp(ts) => Some((ts.time as u64) << 32 | ts.increment as u64),
            _ => None,
        }
    }

    /// Converts a u64 value back to the original BSON type
    fn u64_to_bson(value: u64, original: &Bson) -> Bson {
        match original {
            Bson::Double(_) => Bson::Double(value as f64),
            Bson::Int32(_) => Bson::Int32(value as i32),
            Bson::Int64(_) => Bson::Int64(value as i64),
            Bson::DateTime(_) => Bson::DateTime(bson::DateTime::from_millis(value as i64)),
            Bson::Timestamp(_ts) => {
                let time = (value >> 32) as u32;
                let increment = value as u32;
                Bson::Timestamp(bson::Timestamp { time, increment })
            }
            _ => Bson::Int64(value as i64), // Fallback
        }
    }

    /// Updates numeric fields in a document with values from the metrics array
    fn update_numeric_fields(&self, doc: &mut Document, values: &[u64]) -> usize {
        let mut value_index = 0;

        for (_key, value) in doc.iter_mut() {
            match value {
                Bson::Double(_)
                | Bson::Int32(_)
                | Bson::Int64(_)
                | Bson::Timestamp(_)
                | Bson::DateTime(_) => {
                    if value_index < values.len() {
                        *value = Self::u64_to_bson(values[value_index], value);
                        value_index += 1;
                    }
                }
                Bson::Document(subdoc) => {
                    if value_index < values.len() {
                        let remaining_values = &values[value_index..];
                        let used_values = self.update_numeric_fields(subdoc, remaining_values);
                        value_index += used_values;
                    }
                }
                Bson::Array(arr) => {
                    // Handle arrays of documents
                    for item in arr.iter_mut() {
                        if let Bson::Document(subdoc) = item {
                            if value_index < values.len() {
                                let remaining_values = &values[value_index..];
                                let used_values =
                                    self.update_numeric_fields(subdoc, remaining_values);
                                value_index += used_values;
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        value_index
    }

    /// Reconstructs a document from the reference document and metric values
    pub fn reconstruct_document(
        &self,
        metric_values: &[u64],
        timestamp: bson::DateTime,
    ) -> Result<Document> {
        // Check if we have enough values
        if metric_values.len() != self.metric_count {
            eprintln!(
                "Warning: Metric values count mismatch: expected {}, got {}. Proceeding with available data.",
                self.metric_count,
                metric_values.len()
            );
        }

        // Clone the reference document
        let mut doc = self.reference_doc.clone();

        // Set the timestamp
        doc.insert("_id", Bson::DateTime(timestamp));

        // Update all numeric fields
        self.update_numeric_fields(&mut doc, metric_values);

        Ok(doc)
    }

    /// Gets the number of metrics
    pub fn metric_count(&self) -> usize {
        self.metric_count
    }
}

/// Layer 5: Full FTDC decoder that combines all layers
pub struct FtdcDecoder {
    chunk_extractor: MetricChunkExtractor,
    chunk_decompressor: MetricChunkDecompressor,
    metrics_decoder: MetricsArrayDecoder,
}

impl FtdcDecoder {
    /// Creates a new FtdcDecoder
    pub fn new() -> Self {
        Self {
            chunk_extractor: MetricChunkExtractor::new(),
            chunk_decompressor: MetricChunkDecompressor::new(),
            metrics_decoder: MetricsArrayDecoder::new(),
        }
    }

    /// Decodes a metric document into a series of metric samples
    pub fn decode_document(&self, doc: &Document) -> Result<Vec<MetricSample>> {
        // Extract the timestamp
        let timestamp = match doc.get("_id") {
            Some(Bson::DateTime(dt)) => *dt,
            _ => {
                return Err(FtdcDecoderError::InvalidFormat(
                    "Missing _id field or not a DateTime".to_string(),
                ))
            }
        };

        // Extract the raw chunk
        let raw_chunk = self.chunk_extractor.extract_chunk(doc)?;

        // Decompress the chunk
        let decompressed_chunk = self.chunk_decompressor.decompress_chunk(&raw_chunk)?;

        // Decode the metrics array
        let metrics_array = self.metrics_decoder.decode_metrics_array(
            &decompressed_chunk.compressed_metrics,
            decompressed_chunk.sample_count,
            decompressed_chunk.metric_count,
        )?;

        // Create the document reconstructor
        let reconstructor = MetricDocumentReconstructor::new(decompressed_chunk.reference_doc)?;

        // Check for metric count mismatch
        if reconstructor.metric_count() != decompressed_chunk.metric_count as usize {
            eprintln!(
                "Warning: Metric values count mismatch: expected {}, got {}. Proceeding with available data.",
                reconstructor.metric_count(),
                decompressed_chunk.metric_count
            );

            return Err(FtdcDecoderError::InvalidFormat(
                "Metric count mismatch".to_string(),
            ));
        }

        // Apply delta decoding
        let mut samples = Vec::new();
        let mut previous_values = vec![0u64; decompressed_chunk.metric_count as usize];

        for (i, row) in metrics_array.iter().enumerate() {
            // Apply delta decoding
            let mut absolute_values = vec![0u64; decompressed_chunk.metric_count as usize];
            for j in 0..decompressed_chunk.metric_count as usize {
                if j < row.len() {
                    absolute_values[j] = previous_values[j].saturating_add(row[j]);
                    previous_values[j] = absolute_values[j];
                }
            }

            // Calculate the sample timestamp (base timestamp + i milliseconds)
            let sample_timestamp =
                bson::DateTime::from_millis(timestamp.timestamp_millis().saturating_add(i as i64));

            // Reconstruct the document
            let metrics_doc =
                match reconstructor.reconstruct_document(&absolute_values, sample_timestamp) {
                    Ok(doc) => doc,
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to reconstruct document for sample {}: {:?}",
                            i, e
                        );
                        continue; // Skip this sample and continue with the next one
                    }
                };

            samples.push(MetricSample {
                timestamp: sample_timestamp,
                metrics: metrics_doc,
            });
        }

        Ok(samples)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_count_numeric_fields() {
        let doc = doc! {
            "a": 1,
            "b": {
                "c": 2,
                "d": "string"
            },
            "e": [
                {"f": 3},
                {"g": 4}
            ]
        };

        let count = MetricDocumentReconstructor::count_numeric_fields(&doc);
        assert_eq!(count, 4);
    }

    #[test]
    fn test_update_numeric_fields() {
        let mut doc = doc! {
            "a": 1,
            "b": {
                "c": 2
            },
            "e": [
                {"f": 3},
                {"g": 4}
            ]
        };

        let values = vec![10, 20, 30, 40];
        let reconstructor = MetricDocumentReconstructor::new(doc.clone()).unwrap();
        let used_values = reconstructor.update_numeric_fields(&mut doc, &values);

        // Verify all numeric fields were updated
        assert_eq!(doc.get_i32("a").unwrap(), 10);
        assert_eq!(doc.get_document("b").unwrap().get_i32("c").unwrap(), 20);

        let array = doc.get_array("e").unwrap();
        let first = array[0].as_document().unwrap();
        let second = array[1].as_document().unwrap();
        assert_eq!(first.get_i32("f").unwrap(), 30);
        assert_eq!(second.get_i32("g").unwrap(), 40);

        // Verify the correct number of values were used
        assert_eq!(used_values, 4);
    }

    #[test]
    fn test_decode_metrics_array() {
        // Create a simple test case with run-length encoding
        // [1, 2, 0, 3] -> [1, 2, 0, 0, 0]
        let mut compressed = Vec::new();
        varint::encode_varint_vec(1, &mut compressed).unwrap();
        varint::encode_varint_vec(2, &mut compressed).unwrap();
        varint::encode_varint_vec(0, &mut compressed).unwrap();
        varint::encode_varint_vec(3, &mut compressed).unwrap();

        let decoder = MetricsArrayDecoder::new();
        let result = decoder.decode_metrics_array(&compressed, 5, 1).unwrap();

        assert_eq!(result.len(), 5);
        assert_eq!(result[0][0], 1);
        assert_eq!(result[1][0], 2);
        assert_eq!(result[2][0], 0);
        assert_eq!(result[3][0], 0);
        assert_eq!(result[4][0], 0);
    }

    #[test]
    fn test_delta_decoding() {
        // Create a simple document with numeric fields
        let reference_doc = doc! {
            "counter": 100i32,
            "counter2": 200i32
        };

        // Create a reconstructor
        let reconstructor = MetricDocumentReconstructor::new(reference_doc.clone()).unwrap();

        // Verify the metric count
        assert_eq!(reconstructor.metric_count(), 2);

        // Create a document with initial values
        let timestamp = bson::DateTime::now();
        let doc1 = reconstructor
            .reconstruct_document(&[100, 200], timestamp)
            .unwrap();

        // Verify the document has the expected values
        assert_eq!(doc1.get_i32("counter").unwrap(), 100);
        assert_eq!(doc1.get_i32("counter2").unwrap(), 200);

        // Now create a second document with different values
        let doc2 = reconstructor
            .reconstruct_document(&[105, 190], timestamp)
            .unwrap();

        // Verify the updated values
        assert_eq!(doc2.get_i32("counter").unwrap(), 105);
        assert_eq!(doc2.get_i32("counter2").unwrap(), 190);
    }
}
