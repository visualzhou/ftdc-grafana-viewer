// Implementation of varint encoding/decoding
// Based on MongoDB's FTDC format which follows Google's protocol buffer varint encoding
// See https://developers.google.com/protocol-buffers/docs/encoding#varints

use std::io::{self, Error, ErrorKind};
use crate::FtdcError;

/// Maximum number of bytes a 64-bit integer can take when varint-encoded
pub const MAX_VARINT_SIZE_64: usize = 10;

/// Encodes a 64-bit unsigned integer as a varint into the provided buffer.
///
/// Varint encoding uses one or more bytes to represent a number. Each byte except the
/// last one has the most significant bit (MSB) set, which indicates that more bytes follow.
/// The remaining 7 bits in each byte are used to encode the number in 7-bit chunks, with
/// the least significant chunk first.
///
/// Returns the number of bytes written.
pub fn encode_varint(value: u64, output: &mut [u8]) -> io::Result<usize> {
    if output.len() < MAX_VARINT_SIZE_64 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("Buffer too small, needs at least {MAX_VARINT_SIZE_64} bytes"),
        ));
    }

    let mut val = value;
    let mut pos = 0;

    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;

        if val != 0 {
            // If we have more bits to encode, set the continuation bit
            byte |= 0x80;
        }

        output[pos] = byte;
        pos += 1;

        if val == 0 {
            break;
        }
    }

    Ok(pos)
}

/// Same as encode_varint but appends to a Vec<u8> instead of writing to a buffer
pub fn encode_varint_vec(value: u64, output: &mut Vec<u8>) -> io::Result<usize> {
    let mut val = value;
    let initial_len = output.len();

    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;

        if val != 0 {
            // If we have more bits to encode, set the continuation bit
            byte |= 0x80;
        }

        output.push(byte);

        if val == 0 {
            break;
        }
    }

    Ok(output.len() - initial_len)
}

/// Similar to encode_varint_vec but uses FtdcError instead of io::Error
pub fn encode_varint_ftdc(value: u64, output: &mut Vec<u8>) -> Result<(), FtdcError> {
    encode_varint_vec(value, output)
        .map(|_| ())
        .map_err(|e| FtdcError::Compression(format!("Varint encoding error: {}", e)))
}

/// Decodes a varint from the provided input buffer.
///
/// Returns a tuple containing the decoded value and the number of bytes read.
/// If the input is not a valid varint, an error is returned.
pub fn decode_varint(input: &[u8]) -> io::Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    for (i, &byte) in input.iter().enumerate() {
        let bytes_read = i + 1;

        // Extract the 7 bits of data from the byte
        let value = (byte & 0x7F) as u64;

        // Add the bits to the result at the current shift position
        result |= value << shift;

        // If the high bit is not set, this is the last byte
        if byte & 0x80 == 0 {
            return Ok((result, bytes_read));
        }

        // For each byte, we use 7 more bits of the output value
        shift += 7;

        // Sanity check: we can only encode up to 64 bits
        if shift > 63 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Varint is too large for a 64-bit value",
            ));
        }
    }

    // If we get here, we ran out of bytes before finding the end of the varint
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "Unexpected end of input while decoding varint",
    ))
}

/// Similar to decode_varint but uses FtdcError instead of io::Error
pub fn decode_varint_ftdc(input: &[u8]) -> Result<(u64, usize), FtdcError> {
    decode_varint(input).map_err(|e| {
        match e.kind() {
            ErrorKind::UnexpectedEof => FtdcError::Compression("Incomplete varint".to_string()),
            ErrorKind::InvalidData => FtdcError::Compression("Varint too large".to_string()),
            _ => FtdcError::Compression(format!("Varint decoding error: {}", e)),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encode_decode_simple() {
        let test_cases = vec![
            0u64,
            1,
            127,
            128,
            129,
            255,
            256,
            0x7FFF,
            0x8000,
            0xFFFF,
            0x10000,
            0x7FFFFFFF,
            0x80000000,
            0xFFFFFFFF,
            0x100000000,
            0x7FFFFFFFFFFFFFFF,
            0x8000000000000000,
            0xFFFFFFFFFFFFFFFF,
        ];

        for &value in &test_cases {
            let mut buf = [0u8; MAX_VARINT_SIZE_64];
            let written = encode_varint(value, &mut buf).unwrap();

            let (decoded, read) = decode_varint(&buf).unwrap();

            assert_eq!(value, decoded, "Value mismatch for {}", value);
            assert_eq!(written, read, "Size mismatch for {}", value);
        }
    }

    #[test]
    fn test_varint_vec_encode() {
        let test_cases = vec![
            0u64,
            1,
            127,
            128,
            129,
            255,
            256,
            0x7FFF,
            0x8000,
            0xFFFF,
            0x10000,
            0x7FFFFFFF,
            0x80000000,
            0xFFFFFFFF,
            0x100000000,
            0x7FFFFFFFFFFFFFFF,
            0x8000000000000000,
            0xFFFFFFFFFFFFFFFF,
        ];

        for &value in &test_cases {
            // Using vec
            let mut vec = Vec::new();
            let written_vec = encode_varint_vec(value, &mut vec).unwrap();

            // Using array
            let mut buf = [0u8; MAX_VARINT_SIZE_64];
            let written_buf = encode_varint(value, &mut buf).unwrap();

            // Compare results
            assert_eq!(
                written_vec, written_buf,
                "Size mismatch for vec vs buf for {}",
                value
            );
            assert_eq!(&vec, &buf[0..written_buf], "Content mismatch for {}", value);

            // Decode from vec
            let (decoded, read) = decode_varint(&vec).unwrap();
            assert_eq!(value, decoded, "Value mismatch for {}", value);
            assert_eq!(written_vec, read, "Size mismatch for {}", value);
        }
    }

    #[test]
    fn test_varint_encoding_size() {
        // Check encoding sizes match expectations
        assert_eq!(encode_varint(0, &mut [0; MAX_VARINT_SIZE_64]).unwrap(), 1);
        assert_eq!(encode_varint(127, &mut [0; MAX_VARINT_SIZE_64]).unwrap(), 1);
        assert_eq!(encode_varint(128, &mut [0; MAX_VARINT_SIZE_64]).unwrap(), 2);
        assert_eq!(
            encode_varint(16383, &mut [0; MAX_VARINT_SIZE_64]).unwrap(),
            2
        );
        assert_eq!(
            encode_varint(16384, &mut [0; MAX_VARINT_SIZE_64]).unwrap(),
            3
        );

        // Largest 1-byte varint (7 bits)
        assert_eq!(
            encode_varint(0x7F, &mut [0; MAX_VARINT_SIZE_64]).unwrap(),
            1
        );

        // Smallest 2-byte varint
        assert_eq!(
            encode_varint(0x80, &mut [0; MAX_VARINT_SIZE_64]).unwrap(),
            2
        );

        // Largest 2-byte varint (14 bits)
        assert_eq!(
            encode_varint(0x3FFF, &mut [0; MAX_VARINT_SIZE_64]).unwrap(),
            2
        );

        // A 9-byte varint
        assert_eq!(
            encode_varint(0x7FFFFFFFFFFFFFFF, &mut [0; MAX_VARINT_SIZE_64]).unwrap(),
            9
        );

        // The largest possible value (10 bytes)
        assert_eq!(
            encode_varint(0xFFFFFFFFFFFFFFFF, &mut [0; MAX_VARINT_SIZE_64]).unwrap(),
            10
        );
    }

    #[test]
    fn test_varint_buffer_too_small() {
        // Buffer is too small
        let mut buf = [0u8; 5]; // MAX_VARINT_SIZE_64 is 10

        // Should return an error
        let result = encode_varint(0xFFFFFFFFFFFFFFFF, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_varint_invalid_input() {
        // Test with incomplete varint (high bit set but no more bytes)
        let buf = [0x80];
        let result = decode_varint(&buf);
        assert!(result.is_err());

        // Test with too many continuation bits (would exceed 64 bits)
        let buf = [
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02,
        ];
        let result = decode_varint(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_varint_known_encodings() {
        // Test with some known encoding examples

        // 1 -> [0x01]
        let mut buf = [0u8; MAX_VARINT_SIZE_64];
        let _written = encode_varint(1, &mut buf).unwrap();
        assert_eq!(_written, 1);
        assert_eq!(buf[0], 0x01);

        // 127 -> [0x7F]
        let mut buf = [0u8; MAX_VARINT_SIZE_64];
        let _written = encode_varint(127, &mut buf).unwrap();
        assert_eq!(_written, 1);
        assert_eq!(buf[0], 0x7F);

        // 128 -> [0x80, 0x01]
        let mut buf = [0u8; MAX_VARINT_SIZE_64];
        let _written = encode_varint(128, &mut buf).unwrap();
        assert_eq!(_written, 2);
        assert_eq!(buf[0], 0x80);
        assert_eq!(buf[1], 0x01);

        // 300 -> [0xAC, 0x02]
        let mut buf = [0u8; MAX_VARINT_SIZE_64];
        let _written = encode_varint(300, &mut buf).unwrap();
        assert_eq!(_written, 2);
        assert_eq!(buf[0], 0xAC);
        assert_eq!(buf[1], 0x02);
    }

    // Additional tests from MongoDB's C++ implementation

    /// Test simple integer packing and unpacking
    /// Similar to MongoDB's 'TestInt' function
    fn test_int_simple(value: u64) {
        let mut buf = [0u8; MAX_VARINT_SIZE_64];
        let _written = encode_varint(value, &mut buf).unwrap();
        let (decoded, _) = decode_varint(&buf).unwrap();
        assert_eq!(value, decoded, "Value mismatch for {}", value);
    }

    /// Test various integer combinations like MongoDB's 'TestIntCompression'
    #[test]
    fn test_mongodb_int_compression() {
        // Check numbers with leading 1 (matching MongoDB's test)
        for i in 0..63 {
            test_int_simple(i);
            test_int_simple(i.saturating_sub(1));

            // Also test with the bit set
            test_int_simple(1u64 << i);
        }

        // Check numbers composed of repeating hex numbers (matching MongoDB's test)
        for i in 0..15 {
            let mut v = 0u64;
            for _j in 0..15 {
                v = (v << 4) | i;
                test_int_simple(v);
            }
        }
    }

    /// Test writing multiple zeros (similar to MongoDB's TestDataBuilder)
    #[test]
    fn test_mongodb_multiple_zeros() {
        // Encode a series of zeros
        let mut zeros = Vec::new();
        for _ in 0..16 {
            encode_varint_vec(0, &mut zeros).unwrap();
        }

        // Decode them all back
        let mut offset = 0;
        let mut count = 0;

        while offset < zeros.len() {
            let (value, bytes_read) = decode_varint(&zeros[offset..]).unwrap();
            assert_eq!(value, 0, "Expected zero value");
            offset += bytes_read;
            count += 1;
        }

        // Verify we decoded 16 zeros
        assert_eq!(count, 16, "Expected to decode 16 zeros");
    }
}
