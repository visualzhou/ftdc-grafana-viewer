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

    /// Decodes the compressed metrics array into a 1D array of u64 values
    pub fn decode_metrics_array(
        &self,
        compressed_metrics: &[u8],
        sample_count: u32,
        metric_count: u32,
    ) -> Result<Vec<u64>> {
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

        // Ensure we have the expected number of values
        let expected_values = sample_count as usize * metric_count as usize;

        // Return an error if the number of decoded values doesn't match the expected count
        if expanded_values.len() != expected_values {
            return Err(MetricsDecoderError::Decoding(format!(
                "Decoded values count mismatch: expected {}, got {}",
                expected_values,
                expanded_values.len()
            )));
        }

        Ok(expanded_values)
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
        assert_eq!(result[0], 1);
        assert_eq!(result[1], 2);
        assert_eq!(result[2], 0);
        assert_eq!(result[3], 0);
        assert_eq!(result[4], 0);
    }

    #[test]
    fn test_values_count_mismatch_error() {
        // Create a simple test case with run-length encoding
        // [1, 2, 0, 3] -> [1, 2, 0, 0, 0]
        let mut compressed = Vec::new();
        varint::encode_varint_vec(1, &mut compressed).unwrap();
        varint::encode_varint_vec(2, &mut compressed).unwrap();
        varint::encode_varint_vec(0, &mut compressed).unwrap();
        varint::encode_varint_vec(3, &mut compressed).unwrap();

        let decoder = MetricsArrayDecoder::new();

        // Request 6 values (5x1 matrix) but we'll only get 5
        let result = decoder.decode_metrics_array(&compressed, 6, 1);

        // Verify that we get an error
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.to_string().contains("Decoded values count mismatch"));
        }

        // Request 3 values (3x1 matrix) but we'll get 5
        let result = decoder.decode_metrics_array(&compressed, 3, 1);

        // Verify that we get an error
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.to_string().contains("Decoded values count mismatch"));
        }
    }
}
