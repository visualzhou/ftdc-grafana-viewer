use bson::{doc, };
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use thiserror::Error;

mod compression;
pub mod ftdc_decoder;
mod metrics_array_decoder;
mod reader;
mod varint;
mod victoria_metrics;

pub use compression::Compression;
pub use ftdc_decoder::{
    Chunk,
    ChunkParser,
    DecompressedMetricChunk,
    
    
};
pub use metrics_array_decoder::MetricsArrayDecoder;
pub use reader::{FtdcReader, ReaderResult};
pub use varint::{decode_varint, encode_varint, encode_varint_vec, MAX_VARINT_SIZE_64};
pub use victoria_metrics::VictoriaMetricsClient;

#[derive(Error, Debug)]
pub enum FtdcError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("BSON error: {0}")]
    Bson(#[from] bson::de::Error),

    #[error("BSON serialization error: {0}")]
    BsonSer(#[from] bson::ser::Error),

    #[error("Format error: {0}")]
    Format(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("Server error: status={status}, message={message}")]
    Server { status: reqwest::StatusCode, message: String },
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
    Boolean,
    DateTime,
    Timestamp,
    Decimal128,
}

/// Represents a single FTDC document containing multiple metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtdcDocument {
    pub timestamp: SystemTime,
    pub metrics: Vec<MetricValue>,
}

/*
/// Main parser for FTDC files
pub struct FtdcParser {
    decoder: FtdcDecoder,
}

impl FtdcParser {
    /// Creates a new FTDC parser
    pub fn new() -> Self {
        Self {
            decoder: FtdcDecoder::new(),
        }
    }

    /// Parses a single FTDC document from a byte buffer
    pub async fn parse_document(&self, data: &[u8]) -> Result<FtdcDocument> {
        // Parse the BSON document directly (no decompression needed)
        let doc: Document = bson::from_slice(data)?;

        // Check if this is a metric document (type: 1)
        if let Some(Bson::Int32(1)) = doc.get("type") {
            // Use the decoder to extract metrics
            let samples = self.decoder.decode_document(&doc)?;

            // For simplicity, just return the first sample
            if let Some(first_sample) = samples.first() {
                return self.convert_sample_to_ftdc_document(first_sample);
            }
        }

        // Extract timestamp
        let timestamp = match doc.get("_id") {
            Some(Bson::DateTime(dt)) => dt.to_system_time(),
            _ => return Err(FtdcError::Format("_id".to_string())),
        };

        // Extract metrics
        let metrics = self.extract_metrics(&doc)?;

        Ok(FtdcDocument { timestamp, metrics })
    }

    /// Converts a MetricSample to an FtdcDocument
    fn convert_sample_to_ftdc_document(&self, sample: &MetricSample) -> Result<FtdcDocument> {
        let timestamp = sample.timestamp.to_system_time();
        let mut metrics = Vec::new();

        // Extract metrics from the sample document
        self.extract_metrics_from_doc(&sample.metrics, timestamp, "", &mut metrics)?;

        Ok(FtdcDocument { timestamp, metrics })
    }

    /// Extracts metrics from a BSON document
    fn extract_metrics(&self, doc: &Document) -> Result<Vec<MetricValue>> {
        let mut metrics = Vec::new();

        // Check if this is a metadata document with a "doc" field
        if let Some(Bson::Document(doc_field)) = doc.get("doc") {
            // Process the doc field
            self.extract_metrics_from_doc(doc_field, SystemTime::now(), "", &mut metrics)?;
        } else {
            // Process the document directly
            self.extract_metrics_from_doc(doc, SystemTime::now(), "", &mut metrics)?;
        }

        Ok(metrics)
    }

    /// Recursively extracts metrics from a document
    fn extract_metrics_from_doc(
        &self,
        doc: &Document,
        timestamp: SystemTime,
        prefix: &str,
        metrics: &mut Vec<MetricValue>,
    ) -> Result<()> {
        for (key, value) in doc.iter() {
            // Skip special fields
            if key == "_id" || key == "type" || key == "start" || key == "end" {
                continue;
            }

            let metric_name = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{}_{}", prefix, key)
            };

            match value {
                Bson::Double(v) => {
                    metrics.push(MetricValue {
                        name: metric_name,
                        value: *v,
                        timestamp,
                        metric_type: MetricType::Double,
                    });
                }
                Bson::Int32(v) => {
                    metrics.push(MetricValue {
                        name: metric_name,
                        value: *v as f64,
                        timestamp,
                        metric_type: MetricType::Int32,
                    });
                }
                Bson::Int64(v) => {
                    metrics.push(MetricValue {
                        name: metric_name,
                        value: *v as f64,
                        timestamp,
                        metric_type: MetricType::Int64,
                    });
                }
                Bson::Boolean(v) => {
                    metrics.push(MetricValue {
                        name: metric_name,
                        value: *v as i32 as f64,
                        timestamp,
                        metric_type: MetricType::Boolean,
                    });
                }
                Bson::DateTime(dt) => {
                    metrics.push(MetricValue {
                        name: metric_name,
                        value: dt.timestamp_millis() as f64,
                        timestamp,
                        metric_type: MetricType::DateTime,
                    });
                }
                Bson::Timestamp(ts) => {
                    metrics.push(MetricValue {
                        name: format!("{}_time", metric_name),
                        value: ts.time as f64,
                        timestamp,
                        metric_type: MetricType::Timestamp,
                    });
                    metrics.push(MetricValue {
                        name: format!("{}_increment", metric_name),
                        value: ts.increment as f64,
                        timestamp,
                        metric_type: MetricType::Timestamp,
                    });
                }
                Bson::Array(arr) => {
                    for (i, item) in arr.iter().enumerate() {
                        let array_metric_name = format!("{}_{}", metric_name, i);
                        match item {
                            Bson::Document(subdoc) => {
                                self.extract_metrics_from_doc(
                                    subdoc,
                                    timestamp,
                                    &array_metric_name,
                                    metrics,
                                )?;
                            }
                            _ => self.extract_metric_value(
                                item,
                                timestamp,
                                &array_metric_name,
                                metrics,
                            )?,
                        }
                    }
                }
                Bson::Document(subdoc) => {
                    self.extract_metrics_from_doc(subdoc, timestamp, &metric_name, metrics)?;
                }
                _ => continue,
            }
        }

        Ok(())
    }

    /// Helper function to extract a single metric value from a BSON value
    fn extract_metric_value(
        &self,
        value: &Bson,
        timestamp: SystemTime,
        name: &str,
        metrics: &mut Vec<MetricValue>,
    ) -> Result<()> {
        match value {
            Bson::Double(v) => {
                metrics.push(MetricValue {
                    name: name.to_string(),
                    value: *v,
                    timestamp,
                    metric_type: MetricType::Double,
                });
            }
            Bson::Int32(v) => {
                metrics.push(MetricValue {
                    name: name.to_string(),
                    value: *v as f64,
                    timestamp,
                    metric_type: MetricType::Int32,
                });
            }
            Bson::Int64(v) => {
                metrics.push(MetricValue {
                    name: name.to_string(),
                    value: *v as f64,
                    timestamp,
                    metric_type: MetricType::Int64,
                });
            }
            Bson::Boolean(v) => {
                metrics.push(MetricValue {
                    name: name.to_string(),
                    value: *v as i32 as f64,
                    timestamp,
                    metric_type: MetricType::Boolean,
                });
            }
            Bson::DateTime(dt) => {
                metrics.push(MetricValue {
                    name: name.to_string(),
                    value: dt.timestamp_millis() as f64,
                    timestamp,
                    metric_type: MetricType::DateTime,
                });
            }
            Bson::Timestamp(ts) => {
                metrics.push(MetricValue {
                    name: format!("{}_time", name),
                    value: ts.time as f64,
                    timestamp,
                    metric_type: MetricType::Timestamp,
                });
                metrics.push(MetricValue {
                    name: format!("{}_increment", name),
                    value: ts.increment as f64,
                    timestamp,
                    metric_type: MetricType::Timestamp,
                });
            }
            _ => {
                return Err(FtdcError::Format(format!("Unsupported BSON type: {:?}", value)))
            }
        }
        Ok(())
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
    use bson::DateTime;
    use futures::StreamExt;
    use std::io::Write;
    use tempfile::NamedTempFile;

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
            "_id": DateTime::now(),
            "type": 0i32,
            "doc": {
                "sysMaxOpenFiles": {
                    "sys_max_file_handles": 42.5,
                },
                "memory": 1024,
                "connections": 123456789012345i64
            }
        };

        // Convert to bytes
        let bytes = bson::to_vec(&doc).unwrap();

        // Parse the document
        let result = parser.parse_document(&bytes).await;
        assert!(result.is_ok());

        let doc = result.unwrap();
        assert!(!doc.metrics.is_empty());

        // Verify metrics
        let sys_max_file_handles = doc
            .metrics
            .iter()
            .find(|m| m.name == "sysMaxOpenFiles_sys_max_file_handles")
            .unwrap();
        assert_eq!(sys_max_file_handles.value, 42.5);
        assert!(matches!(
            sys_max_file_handles.metric_type,
            MetricType::Double
        ));

        let memory_metric = doc.metrics.iter().find(|m| m.name == "memory").unwrap();
        assert_eq!(memory_metric.value, 1024.0);
        assert!(matches!(memory_metric.metric_type, MetricType::Int32));

        let connections_metric = doc
            .metrics
            .iter()
            .find(|m| m.name == "connections")
            .unwrap();
        assert_eq!(connections_metric.value, 123456789012345.0);
        assert!(matches!(connections_metric.metric_type, MetricType::Int64));
    }

    #[tokio::test]
    async fn test_file_reading() -> ReaderResult<()> {
        // Create a test file with FTDC data
        let mut temp_file = NamedTempFile::new()?;

        // Create a metadata document
        let metadata_doc = doc! {
            "_id": DateTime::now(),
            "type": 0i32,
            "doc": {
                "start": DateTime::now(),
                "sysMaxOpenFiles": {
                    "start": DateTime::now(),
                    "sys_max_file_handles": 1024i64,
                    "end": DateTime::now()
                },
                "ulimits": {
                    "start": DateTime::now(),
                    "cpuTime_secs": {
                        "soft": 123456789i64,
                        "hard": 123456789i64
                    },
                    "end": DateTime::now()
                },
                "end": DateTime::now()
            }
        };

        // Write metadata document
        let doc_bytes = bson::to_vec(&metadata_doc)?;
        temp_file.write_all(&doc_bytes)?;

        // Create a metric document
        let metric_doc = doc! {
            "_id": DateTime::now(),
            "type": 1i32
        };

        // Write metric document
        let doc_bytes = bson::to_vec(&metric_doc)?;
        temp_file.write_all(&doc_bytes)?;
        temp_file.flush()?;

        // Create reader and read document
        let mut reader = FtdcReader::new(temp_file.path()).await?;

        let doc = reader.read_next().await?.unwrap();
        assert!(!doc.metrics.is_empty(), "Document should contain metrics");

        // Test streaming
        let reader = FtdcReader::new(temp_file.path()).await?;

        let mut stream = reader.stream_documents();
        let mut count = 0;
        while let Some(doc) = stream.next().await {
            let _doc = doc?;
            count += 1;
        }
        assert!(count > 0, "Should have processed at least one document");

        Ok(())
    }
}
*/