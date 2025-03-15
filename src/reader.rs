use std::path::Path;
use tokio::{fs::File, io::{BufReader, AsyncReadExt}};
use thiserror::Error;
use futures::Stream;
use std::pin::Pin;
use bson::{ser, Document, Bson};
use std::time::SystemTime;

use crate::{FtdcDocument, FtdcError, MetricValue, MetricType};

const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer

#[derive(Debug)]
enum FtdcDocType {
    Metadata = 0,
    Metric = 1,
    MetadataDelta = 2,
}

impl TryFrom<i32> for FtdcDocType {
    type Error = ReaderError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FtdcDocType::Metadata),
            1 => Ok(FtdcDocType::Metric),
            2 => Ok(FtdcDocType::MetadataDelta),
            _ => Err(ReaderError::InvalidFile(format!("Invalid FTDC document type: {}", value))),
        }
    }
}

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("Invalid FTDC file: {0}")]
    InvalidFile(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("FTDC error: {0}")]
    Ftdc(#[from] FtdcError),

    #[error("BSON serialization error: {0}")]
    BsonSer(#[from] ser::Error),

    #[error("BSON deserialization error: {0}")]
    BsonDe(#[from] bson::de::Error),
}

pub type ReaderResult<T> = std::result::Result<T, ReaderError>;

/// Reader for FTDC files that supports async streaming
pub struct FtdcReader {
    reader: BufReader<File>,
    reference_doc: Option<Document>,
}

impl FtdcReader {
    /// Creates a new FTDC reader from a file path
    pub async fn new<P: AsRef<Path>>(path: P) -> ReaderResult<Self> {
        let file = File::open(path).await?;
        let reader = BufReader::with_capacity(BUFFER_SIZE, file);
        
        Ok(Self {
            reader,
            reference_doc: None,
        })
    }
    
    /// Reads the next BSON document from the file
    async fn read_bson_document(&mut self) -> ReaderResult<Option<Document>> {
        // Read document size (4 bytes)
        let mut size_buf = [0u8; 4];
        match self.reader.read_exact(&mut size_buf).await {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        
        let doc_size = u32::from_le_bytes(size_buf) as usize;
        if doc_size < 5 {
            return Ok(None);
        }
        
        // Create a buffer that includes the size we already read
        let mut doc_data = vec![0u8; doc_size];
        doc_data[0..4].copy_from_slice(&size_buf);
        
        // Read the rest of the document
        match self.reader.read_exact(&mut doc_data[4..]).await {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(ReaderError::InvalidFile("Unexpected EOF while reading document".to_string()));
            },
            Err(e) => return Err(e.into()),
        }
        
        // Parse BSON document
        match bson::from_slice(&doc_data) {
            Ok(doc) => Ok(Some(doc)),
            Err(e) => Err(ReaderError::BsonDe(e)),
        }
    }

    /// Extracts metrics from a BSON document
    fn extract_metrics(&self, doc: &Document, timestamp: SystemTime, prefix: &str) -> ReaderResult<Vec<MetricValue>> {
        let mut metrics = Vec::new();
        
        for (key, value) in doc.iter() {
            if key == "_id" || key == "type" || key == "doc" || key == "start" || key == "end" {
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
                Bson::Document(subdoc) => {
                    // Recursively process nested documents
                    let nested_metrics = self.extract_metrics(subdoc, timestamp, &metric_name)?;
                    metrics.extend(nested_metrics);
                }
                _ => continue,
            }
        }
        
        Ok(metrics)
    }

    /// Reads and processes the next FTDC document
    pub async fn read_next(&mut self) -> ReaderResult<Option<FtdcDocument>> {
        let doc = match self.read_bson_document().await? {
            Some(doc) => doc,
            None => return Ok(None),
        };

        // Extract document type and _id
        let doc_type = doc.get_i32("type")
            .map_err(|_| ReaderError::InvalidFile("Missing or invalid 'type' field".to_string()))?;
        let doc_type = FtdcDocType::try_from(doc_type)?;

        let timestamp = match doc.get("_id") {
            Some(Bson::DateTime(dt)) => dt.to_system_time(),
            _ => return Err(ReaderError::InvalidFile("Missing or invalid '_id' field".to_string())),
        };

        match doc_type {
            FtdcDocType::Metadata => {
                // Store reference document for future metric parsing
                if let Some(Bson::Document(ref_doc)) = doc.get("doc") {
                    self.reference_doc = Some(ref_doc.clone());
                    let metrics = self.extract_metrics(ref_doc, timestamp, "")?;
                    Ok(Some(FtdcDocument { timestamp, metrics }))
                } else {
                    Box::pin(self.read_next()).await
                }
            }
            FtdcDocType::Metric => {
                if let Some(ref_doc) = &self.reference_doc {
                    let metrics = self.extract_metrics(ref_doc, timestamp, "")?;
                    Ok(Some(FtdcDocument { timestamp, metrics }))
                } else {
                    Err(ReaderError::InvalidFile("No reference document available".to_string()))
                }
            }
            FtdcDocType::MetadataDelta => {
                // Skip metadata delta documents in the stream
                Box::pin(self.read_next()).await
            }
        }
    }
    
    /// Returns an async stream of FTDC documents
    pub fn stream_documents(self) -> Pin<Box<dyn Stream<Item = ReaderResult<FtdcDocument>> + Send>> {
        Box::pin(futures::stream::unfold(self, |mut reader| async move {
            match reader.read_next().await {
                Ok(Some(doc)) => Some((Ok(doc), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(e), reader)),
            }
        }))
    }
} 