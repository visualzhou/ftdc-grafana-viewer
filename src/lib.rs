use std::time::SystemTime;
use thiserror::Error;
use serde::{Serialize, Deserialize};
use bson::{doc, Document, Bson};

mod compression;
use compression::Compression;

#[derive(Error, Debug)]
pub enum FtdcError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("ZSTD decompression error: {0}")]
    Zstd(String),
    
    #[error("BSON parsing error: {0}")]
    Bson(#[from] bson::de::Error),
    
    #[error("Invalid FTDC format: {0}")]
    InvalidFormat(String),
    
    #[error("Missing required field: {0}")]
    MissingField(String),
    
    #[error("Unsupported metric type: {0}")]
    UnsupportedType(String),
}

pub type Result<T> = std::result::Result<T, FtdcError>;

/// Represents a single FTDC metric value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricValue {
    pub name: String,
    pub value: f64,
    pub timestamp: SystemTime,
    pub metric_type: MetricType,
}

/// Represents the type of metric value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricType {
    Double,
    Int32,
    Int64,
}

/// Represents a single FTDC document containing multiple metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtdcDocument {
    pub timestamp: SystemTime,
    pub metrics: Vec<MetricValue>,
}

/// Main parser for FTDC files
pub struct FtdcParser {
    // Will be implemented in the next step
}

impl FtdcParser {
    /// Creates a new FTDC parser
    pub fn new() -> Self {
        Self {}
    }

    /// Parses a single FTDC document from a byte buffer
    pub async fn parse_document(&self, data: &[u8]) -> Result<FtdcDocument> {
        // First decompress the data
        let decompressed = Compression::decompress(data)?;
        
        // Parse the BSON document
        let doc: Document = bson::from_slice(&decompressed)?;
        
        // Extract timestamp
        let timestamp = match doc.get("ts") {
            Some(Bson::DateTime(dt)) => dt.to_system_time(),
            _ => return Err(FtdcError::MissingField("ts".to_string())),
        };
        
        // Extract metrics
        let metrics = self.extract_metrics(&doc)?;
        
        Ok(FtdcDocument {
            timestamp,
            metrics,
        })
    }

    /// Extracts metrics from a BSON document
    fn extract_metrics(&self, doc: &Document) -> Result<Vec<MetricValue>> {
        let mut metrics = Vec::new();
        
        // Get the metrics object
        let metrics_doc = match doc.get("metrics") {
            Some(Bson::Document(doc)) => doc,
            _ => return Err(FtdcError::MissingField("metrics".to_string())),
        };
        
        // Iterate over all fields in the metrics document
        for (name, value) in metrics_doc.iter() {
            let (value, metric_type) = match value {
                Bson::Double(v) => (*v, MetricType::Double),
                Bson::Int32(v) => (*v as f64, MetricType::Int32),
                Bson::Int64(v) => (*v as f64, MetricType::Int64),
                _ => return Err(FtdcError::UnsupportedType(format!("{:?}", value))),
            };
            
            metrics.push(MetricValue {
                name: name.clone(),
                value,
                timestamp: SystemTime::now(), // This will be updated with the document timestamp
                metric_type,
            });
        }
        
        Ok(metrics)
    }
}

impl Default for FtdcParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;
    use bson::DateTime;

    #[tokio::test]
    async fn test_parser_creation() {
        let parser = FtdcParser::new();
        assert!(parser.parse_document(&[]).await.is_err());
    }

    #[tokio::test]
    async fn test_parse_valid_document() {
        let parser = FtdcParser::new();
        
        // Create a test BSON document with different metric types
        let doc = doc! {
            "ts": DateTime::now(),
            "metrics": {
                "cpu": 42.5,
                "memory": 1024,
                "connections": 123456789012345i64
            }
        };
        
        // Convert to bytes
        let mut bytes = Vec::new();
        doc.to_writer(&mut bytes).unwrap();
        
        // Compress the bytes
        let mut encoder = zstd::Encoder::new(Vec::new(), 1).unwrap();
        encoder.write_all(&bytes).unwrap();
        let compressed = encoder.finish().unwrap();
        
        // Parse the document
        let result = parser.parse_document(&compressed).await;
        assert!(result.is_ok());
        
        let doc = result.unwrap();
        assert_eq!(doc.metrics.len(), 3);
        
        // Verify metrics
        let cpu_metric = doc.metrics.iter().find(|m| m.name == "cpu").unwrap();
        assert_eq!(cpu_metric.value, 42.5);
        assert!(matches!(cpu_metric.metric_type, MetricType::Double));
        
        let memory_metric = doc.metrics.iter().find(|m| m.name == "memory").unwrap();
        assert_eq!(memory_metric.value, 1024.0);
        assert!(matches!(memory_metric.metric_type, MetricType::Int32));
        
        let connections_metric = doc.metrics.iter().find(|m| m.name == "connections").unwrap();
        assert_eq!(connections_metric.value, 123456789012345.0);
        assert!(matches!(connections_metric.metric_type, MetricType::Int64));
    }
}
