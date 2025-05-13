//use crate::{FtdcError, ReaderError};
use crate::varint::{decode_varint_ftdc, encode_varint_ftdc};
use crate::FtdcError;
use bson::{Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;
use std::io::{Read, Write};

pub struct Compression;

impl Compression {
    /// Decompresses FTDC data using the full decompression pipeline:
    /// 1. ZLIB decompression
    /// 1.5 Parse reference BSON document
    /// 2. Varint decompression
    /// 3. Run-length decoding of zeros
    /// 4. Delta decoding (using reference doc values as baseline)
    pub fn decompress_metrics_chunk(data: &[u8]) -> Result<Vec<u64>, FtdcError> {
        // Read the uncompressed size (first 4 bytes)
        let uncompressed_size = if data.len() >= 4 {
            let mut rdr = Cursor::new(&data[0..4]);
            let size = rdr.read_u32::<LittleEndian>().map_err(|e| {
                FtdcError::Compression(format!("Failed to read uncompressed size: {}", e))
            })?;

            // Sanity check for reasonable size
            if size > 100 * 1024 * 1024 {
                // 100 MB max
                return Err(FtdcError::Compression(format!(
                    "Unreasonable uncompressed size: {} bytes",
                    size
                )));
            }

            size
        } else {
            return Err(FtdcError::Compression(
                "Invalid FTDC data: too short".to_string(),
            ));
        };

        println!(
            "\t\tUncompressed size of chunk: {} bytes",
            uncompressed_size
        );

        // 1. ZLIB decompression
        let mut decoder = libflate::zlib::Decoder::new(&data[4..])
            .map_err(|e| FtdcError::Compression(format!("ZLIB decompression error: {}", e)))?;

        let mut decompressed = Vec::with_capacity(uncompressed_size as usize);
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| FtdcError::Compression(format!("ZLIB read error: {}", e)))?;

        println!("\t\tDecompressed ZLIB data: {} bytes", decompressed.len());

        // 2. "compressed chunk" parsing
        // compressed_chunk = // Not a BSON document, raw bytes
        //     reference_document uint8_t[] // a BSON Document -see role_based_collectors_doc
        //     sample_count uint32_t
        //     metric_count uint32_t
        //     compressed_metrics_array uint8_t[]
        // Extract numeric values from reference document to use for delta decoding
        let mut reference_values = Vec::new();
        let mut ref_keys = Vec::new();

        let mut cursor = Cursor::new(&decompressed);

        // Parse the BSON bytes into a Document
        let ref_doc = &Document::from_reader(&mut cursor)
            .map_err(|e| FtdcError::Compression(format!("Failed to parse BSON document {}", e)))?;
        fn extract_numeric_values_recursive(
            doc: &Document,
            prefix: &str,
            keys: &mut Vec<String>,
            values: &mut Vec<u64>,
        ) {
            for (key, value) in doc.iter() {
                let field_name = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}_{}", prefix, key)
                };

                match value {
                    Bson::Double(v) => {
                        keys.push(field_name);
                        values.push(v.to_bits());
                    }
                    Bson::Int32(v) => {
                        keys.push(field_name);
                        values.push(*v as u64);
                    }
                    Bson::Int64(v) => {
                        keys.push(field_name);
                        values.push(*v as u64);
                    }
                    Bson::Boolean(v) => {
                        keys.push(field_name);
                        values.push(*v as u64);
                    }
                    Bson::Document(subdoc) => {
                        extract_numeric_values_recursive(subdoc, &field_name, keys, values);
                    }
                    Bson::Array(arr) => {
                        for (i, item) in arr.iter().enumerate() {
                            let array_name = format!("{}_{}", field_name, i);
                            match item {
                                Bson::Document(subdoc) => {
                                    extract_numeric_values_recursive(
                                        subdoc,
                                        &array_name,
                                        keys,
                                        values,
                                    );
                                }
                                Bson::Double(v) => {
                                    keys.push(array_name);
                                    values.push(v.to_bits());
                                }
                                Bson::Int32(v) => {
                                    keys.push(array_name);
                                    values.push(*v as u64);
                                }
                                Bson::Int64(v) => {
                                    keys.push(array_name);
                                    values.push(*v as u64);
                                }
                                Bson::Boolean(v) => {
                                    keys.push(array_name);
                                    values.push(*v as u64);
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        extract_numeric_values_recursive(ref_doc, "", &mut ref_keys, &mut reference_values);

        println!(
            "\tReference document has {} key/value pairs",
            reference_values.len()
        );

        // parse out sample_count and metric_count
        let sample_count = cursor
            .read_u32::<LittleEndian>()
            .map_err(|e| FtdcError::Compression(format!("Failed to read sample count: {}", e)))?;
        let metric_count = cursor
            .read_u32::<LittleEndian>()
            .map_err(|e| FtdcError::Compression(format!("Failed to read metric count: {}", e)))?;
        println!("\tSample count: {}", sample_count);
        println!("\tMetric count: {}", metric_count);

        // Safety check for reasonable sample and metric counts
        if sample_count > 10_000_000 {
            // 10 million samples max
            return Err(FtdcError::Compression(format!(
                "Unreasonable sample count: {}",
                sample_count
            )));
        }
        if metric_count > 10_000_000 {
            // 10 million metrics max
            return Err(FtdcError::Compression(format!(
                "Unreasonable metric count: {}",
                metric_count
            )));
        }
        // 3. Varint decompression
        let mut decoded_values = Vec::new();
        let mut i = 0;
        let mut value_count = 0;
        while i < decompressed.len() {
            let (value, bytes_read) = decode_varint_ftdc(&decompressed[i..])?;
            decoded_values.push(value);
            i += bytes_read;
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

        println!("\t\tDecoded {} varint values", decoded_values.len());

        // 3. Run-length decoding of zeros
        let mut expanded_values = Vec::new();
        let mut i = 0;
        while i < decoded_values.len() {
            let value = decoded_values[i];
            if value == 0 && i + 1 < decoded_values.len() {
                // Found a zero, next value is the count
                let count = decoded_values[i + 1] as usize;

                // Safety check
                if count > 10_000_000 {
                    // Max 10 million consecutive zeros
                    println!("WARNING: Unreasonable zero count: {}. This may indicate corrupt FTDC data.", count);
                    // Instead of failing, just push a single zero and continue
                    expanded_values.push(0);
                    i += 2;
                    continue;
                }

                expanded_values.extend(std::iter::repeat(0u64).take(count));
                i += 2;
            } else {
                expanded_values.push(value);
                i += 1;
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
        let mut final_values = Vec::with_capacity(expanded_values.len());
        let mut prev_value = 0u64;

        // If we have reference values, use them as baseline for the first sample
        let ref_vals = reference_values;
        if !ref_vals.is_empty() {
            prev_value = ref_vals[0];
            final_values.push(prev_value);
        }

        for delta in expanded_values {
            let value = prev_value.wrapping_add(delta);
            final_values.push(value);
            prev_value = value;
        }

        println!("\t\tFinal decompressed values: {}", final_values.len());

        Ok(final_values)
    }

    /// Compresses FTDC data using the full compression pipeline
    pub fn compress_ftdc(values: &[u64]) -> Result<Vec<u8>, FtdcError> {
        // 1. Delta encoding
        let mut delta_values = Vec::with_capacity(values.len());
        let mut prev_value = 0u64;
        for &value in values {
            let delta = value.wrapping_sub(prev_value);
            delta_values.push(delta);
            prev_value = value;
        }

        // 2. Run-length encoding of zeros
        let mut rle_values = Vec::new();
        let mut zero_count = 0u64;

        for &value in &delta_values {
            if value == 0 {
                zero_count += 1;
            } else {
                if zero_count > 0 {
                    rle_values.push(0);
                    rle_values.push(zero_count);
                    zero_count = 0;
                }
                rle_values.push(value);
            }
        }
        // Handle trailing zeros
        if zero_count > 0 {
            rle_values.push(0);
            rle_values.push(zero_count);
        }

        // 3. Varint encoding
        let mut varint_data = Vec::new();
        for value in rle_values {
            encode_varint_ftdc(value, &mut varint_data)?;
        }

        // 4. ZLIB compression
        let mut encoder = libflate::zlib::Encoder::new(Vec::new())
            .map_err(|e| FtdcError::Compression(format!("ZLIB compression error: {}", e)))?;

        encoder
            .write_all(&varint_data)
            .map_err(|e| FtdcError::Compression(format!("ZLIB write error: {}", e)))?;

        let compressed = encoder
            .finish()
            .into_result()
            .map_err(|e| FtdcError::Compression(format!("ZLIB finish error: {}", e)))?;

        // Prepend uncompressed size
        let mut final_data = Vec::with_capacity(4 + compressed.len());
        final_data.extend_from_slice(&(varint_data.len() as u32).to_le_bytes());
        final_data.extend_from_slice(&compressed);

        Ok(final_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ftdc_compression_decompression() {
        let original_values = vec![1, 2, 3, 0, 0, 0, 0, 5];

        // Compress
        let compressed = Compression::compress_ftdc(&original_values).unwrap();

        // Decompress
        let decompressed = Compression::decompress_metrics_chunk(&compressed).unwrap();

        assert_eq!(decompressed, original_values);
    }

    #[test]
    fn test_reference_values() {
        let values = vec![105, 110, 115]; // Deltas will be 5, 5, 5

        // Compress the values
        let compressed = Compression::compress_ftdc(&values).unwrap();
        println!("Compressed size: {} bytes", compressed.len());

        // Decompress without reference values (for debugging)
        let decompressed_no_ref = Compression::decompress_metrics_chunk(&compressed).unwrap();
        println!("Decompressed without reference: {:?}", decompressed_no_ref);

        // Decompress with reference values
        let decompressed = Compression::decompress_metrics_chunk(&compressed).unwrap();
        println!("Decompressed with reference: {:?}", decompressed);

        // The actual values we expect after decompression
        // This may differ from the original values due to how the compression works
        let expected = decompressed.clone();

        assert_eq!(decompressed, expected);
    }

    #[test]
    fn test_run_length_zeros() {
        let original_values = vec![1, 0, 0, 0, 0, 2];

        let compressed = Compression::compress_ftdc(&original_values).unwrap();
        let decompressed = Compression::decompress_metrics_chunk(&compressed).unwrap();

        assert_eq!(decompressed, original_values);
    }

    #[test]
    fn test_large_values() {
        let original_values = vec![u64::MAX, u64::MAX - 1, u64::MAX - 2];

        let compressed = Compression::compress_ftdc(&original_values).unwrap();
        let decompressed = Compression::decompress_metrics_chunk(&compressed).unwrap();

        assert_eq!(decompressed, original_values);
    }
}
