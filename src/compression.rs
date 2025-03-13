use std::io::{Read, Write};
use crate::{Result, FtdcError};
use zstd::Decoder;

pub struct Compression;

impl Compression {
    /// Decompresses ZSTD-compressed data
    pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = Decoder::new(data)
            .map_err(|e| FtdcError::Zstd(e.to_string()))?;
        
        let mut buffer = Vec::new();
        decoder.read_to_end(&mut buffer)
            .map_err(|e| FtdcError::Zstd(e.to_string()))?;
        
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zstd::Encoder;

    #[test]
    fn test_decompress() {
        let original = b"Hello, World!";
        let mut encoder = Encoder::new(Vec::new(), 1).unwrap();
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let decompressed = Compression::decompress(&compressed).unwrap();
        assert_eq!(&decompressed, original);
    }

    #[test]
    fn test_invalid_data() {
        let invalid_data = b"Not a valid ZSTD stream";
        assert!(Compression::decompress(invalid_data).is_err());
    }
} 