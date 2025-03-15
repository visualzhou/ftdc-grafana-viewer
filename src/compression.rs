use crate::FtdcError;
use std::io::Write;

pub struct Compression;

impl Compression {
    /// Decompresses ZSTD-compressed data
    pub fn decompress(data: &[u8]) -> Result<Vec<u8>, FtdcError> {
        let mut decoder = zstd::Decoder::new(data).map_err(|e| FtdcError::Zstd(e.to_string()))?;

        let mut decompressed = Vec::new();
        std::io::copy(&mut decoder, &mut decompressed)
            .map_err(|e| FtdcError::Zstd(e.to_string()))?;

        Ok(decompressed)
    }

    pub fn compress(data: &[u8]) -> Result<Vec<u8>, FtdcError> {
        let mut encoder =
            zstd::Encoder::new(Vec::new(), 1).map_err(|e| FtdcError::Zstd(e.to_string()))?;

        encoder
            .write_all(data)
            .map_err(|e| FtdcError::Zstd(e.to_string()))?;

        encoder.finish().map_err(|e| FtdcError::Zstd(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
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
