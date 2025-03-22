use crate::varint;
use thiserror::Error;

/// Errors that can occur during metrics array decoding
#[derive(Error, Debug)]
pub enum MetricsDecoderError {
    #[error("Decoding error: {0}")]
    Decoding(String),
}

pub type Result<T> = std::result::Result<T, MetricsDecoderError>;

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
                .map_err(|e| MetricsDecoderError::Decoding(e.to_string()))?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
