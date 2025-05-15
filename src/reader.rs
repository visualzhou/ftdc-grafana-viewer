use bson::{Bson, Document};
use futures::Stream;
use std::path::Path;
use std::pin::Pin;
use std::time::SystemTime;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};

use crate::{ChunkParser, FtdcDocument, FtdcDocumentTS, FtdcError, MetricType, MetricValue};

const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer

#[derive(Debug)]
enum FtdcDocType {
    Metadata = 0,
    Metric = 1,
    MetadataDelta = 2,
}

impl TryFrom<i32> for FtdcDocType {
    type Error = FtdcError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FtdcDocType::Metadata),
            1 => Ok(FtdcDocType::Metric),
            2 => Ok(FtdcDocType::MetadataDelta),
            _ => Err(FtdcError::Format(format!(
                "Invalid FTDC document type: {}",
                value
            ))),
        }
    }
}

pub type ReaderResult<T> = std::result::Result<T, FtdcError>;

/// Handler trait for processing FTDC documents
pub trait MetricsDocHandler {
    /// The type returned after transformation
    type TransformedType;

    /// Process a metadata document
    fn handle_metadata(
        &self,
        reader: &FtdcReader,
        ref_doc: &Document,
        timestamp: SystemTime,
    ) -> ReaderResult<Option<Self::TransformedType>>;

    /// Process a metric document
    fn handle_metric(
        &self,
        doc: &Document,
        timestamp: SystemTime,
    ) -> ReaderResult<Self::TransformedType>;

    /// Process a metadata delta document
    fn handle_metadata_delta(
        &self,
        doc: &Document,
        timestamp: SystemTime,
    ) -> ReaderResult<Option<Self::TransformedType>>;
}

/// Reader for FTDC files that supports async streaming
pub struct FtdcReader {
    reader: BufReader<File>,
    file_path: Option<String>,
    folder_path: Option<String>,
}

impl FtdcReader {
    /// Creates a new FTDC reader from a file path
    pub async fn new<P: AsRef<Path>>(path: P) -> ReaderResult<Self> {
        let file = File::open(path).await?;
        let reader = BufReader::with_capacity(BUFFER_SIZE, file);

        Ok(Self {
            reader,
            file_path: None,
            folder_path: None,
        })
    }

    /// Set the file and folder paths
    pub fn set_paths(&mut self, file_path: String, folder_path: String) {
        self.file_path = Some(file_path);
        self.folder_path = Some(folder_path);
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
                return Err(FtdcError::Format(
                    "Unexpected EOF while reading document".to_string(),
                ));
            }
            Err(e) => return Err(e.into()),
        }

        // Parse BSON document
        match bson::from_slice(&doc_data) {
            Ok(doc) => Ok(Some(doc)),
            Err(e) => Err(FtdcError::Bson(e)),
        }
    }

    /// Extracts metrics from a BSON document
    /// Only useful for reference documents.
    fn extract_metrics(
        &self,
        doc: &Document,
        timestamp: SystemTime,
        prefix: &str,
    ) -> ReaderResult<Vec<MetricValue>> {
        let mut metrics = Vec::new();

        for (key, value) in doc.iter() {
            if key == "_id"
                || key == "type"
                || key == "doc"
                || key == "start"
                || key == "end"
                || key == "data"
            {
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
                                let nested_metrics =
                                    self.extract_metrics(subdoc, timestamp, &array_metric_name)?;
                                metrics.extend(nested_metrics);
                            }
                            _ => self.extract_metric_value(
                                item,
                                timestamp,
                                &array_metric_name,
                                &mut metrics,
                            )?,
                        }
                    }
                }
                Bson::Document(subdoc) => {
                    let nested_metrics = self.extract_metrics(subdoc, timestamp, &metric_name)?;
                    metrics.extend(nested_metrics);
                }
                _ => continue,
            }
        }

        Ok(metrics)
    }

    /// Helper function to extract a single metric value from a BSON value
    fn extract_metric_value(
        &self,
        value: &Bson,
        timestamp: SystemTime,
        name: &str,
        metrics: &mut Vec<MetricValue>,
    ) -> ReaderResult<()> {
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
            // Skip string values - they don't represent numeric metrics
            Bson::String(_) => {}
            _ => {
                return Err(FtdcError::Format(format!(
                    "Unsupported BSON type: {:?}",
                    value
                )))
            }
        }
        Ok(())
    }

    /// Iterates through FTDC documents and processes them with the given handler
    pub async fn iterate_next<H, T>(&mut self, handler: &H) -> ReaderResult<Option<T>>
    where
        H: MetricsDocHandler<TransformedType = T>,
    {
        let doc = match self.read_bson_document().await? {
            Some(doc) => doc,
            None => return Ok(None),
        };

        // Extract document type and _id
        let doc_type = doc
            .get_i32("type")
            .map_err(|_| FtdcError::Format("Missing or invalid 'type' field".to_string()))?;
        let doc_type = FtdcDocType::try_from(doc_type)?;

        let timestamp = match doc.get("_id") {
            Some(Bson::DateTime(dt)) => dt.to_system_time(),
            _ => {
                return Err(FtdcError::Format(
                    "Missing or invalid '_id' field".to_string(),
                ))
            }
        };

        match doc_type {
            FtdcDocType::Metadata => {
                // Metadata is only for one-shot metrics that have no time series
                if let Some(Bson::Document(ref_doc)) = doc.get("doc") {
                    println!(
                        "Found reference document (type 0) with {} fields",
                        ref_doc.len()
                    );
                    let result = handler.handle_metadata(self, ref_doc, timestamp)?;
                    if result.is_some() {
                        return Ok(result);
                    }
                } else {
                    println!("WARNING: Metadata document missing 'doc' field");
                }
                // Try next document
                Box::pin(self.iterate_next(handler)).await
            }
            FtdcDocType::Metric => {
                let result = handler.handle_metric(&doc, timestamp)?;
                Ok(Some(result))
            }
            FtdcDocType::MetadataDelta => {
                // Handle metadata delta documents
                match handler.handle_metadata_delta(&doc, timestamp)? {
                    Some(result) => Ok(Some(result)),
                    None => {
                        // If handler returned None, continue to the next document
                        Box::pin(self.iterate_next(handler)).await
                    }
                }
            }
        }
    }

    pub async fn read_next_time_series(&mut self) -> ReaderResult<Option<FtdcDocumentTS>> {
        struct TimeSeriesDocHandler {}

        impl MetricsDocHandler for TimeSeriesDocHandler {
            type TransformedType = FtdcDocumentTS;

            fn handle_metadata(
                &self,
                _reader: &FtdcReader,
                _ref_doc: &Document,
                _timestamp: SystemTime,
            ) -> ReaderResult<Option<Self::TransformedType>> {
                println!("Skipping metadata for time series processing");
                Ok(None)
            }

            fn handle_metric(
                &self,
                doc: &Document,
                _timestamp: SystemTime,
            ) -> ReaderResult<Self::TransformedType> {
                println!("Processing metric document (type 1)  {} bytes", doc.len());
                let chunk_parser = ChunkParser;
                let chunk = chunk_parser.parse_chunk_header(&doc)?;
                let time_series = chunk_parser.decode_time_series(&chunk)?;
                Ok(FtdcDocumentTS {
                    metrics: time_series,
                })
            }

            fn handle_metadata_delta(
                &self,
                _doc: &Document,
                _timestamp: SystemTime,
            ) -> ReaderResult<Option<Self::TransformedType>> {
                Ok(None)
            }
        }

        let handler = TimeSeriesDocHandler {};
        self.iterate_next(&handler).await
    }

    /// Reads and processes the next FTDC document
    pub async fn read_next(&mut self) -> ReaderResult<Option<FtdcDocument>> {
        // Use a default handler that returns FtdcDocument
        struct DefaultDocHandler {}

        impl MetricsDocHandler for DefaultDocHandler {
            type TransformedType = FtdcDocument;

            fn handle_metadata(
                &self,
                reader: &FtdcReader,
                ref_doc: &Document,
                timestamp: SystemTime,
            ) -> ReaderResult<Option<Self::TransformedType>> {
                let metrics = reader.extract_metrics(ref_doc, timestamp, "")?;
                Ok(Some(FtdcDocument { timestamp, metrics }))
            }

            fn handle_metric(
                &self,
                doc: &Document,
                timestamp: SystemTime,
            ) -> ReaderResult<Self::TransformedType> {
                let chunk_parser = ChunkParser;
                println!("Processing metric document (type 1)  {} bytes", doc.len());
                let chunk = chunk_parser.parse_chunk_header(&doc)?;
                let metrics = chunk_parser.decode_chunk_values(&chunk)?;

                Ok(FtdcDocument { timestamp, metrics })
            }

            fn handle_metadata_delta(
                &self,
                _doc: &Document,
                _timestamp: SystemTime,
            ) -> ReaderResult<Option<Self::TransformedType>> {
                // Skip this document type
                Ok(None)
            }
        }

        let handler = DefaultDocHandler {};
        self.iterate_next(&handler).await
    }

    /// Returns an async stream of FTDC documents
    pub fn stream_documents(
        self,
    ) -> Pin<Box<dyn Stream<Item = ReaderResult<FtdcDocument>> + Send>> {
        Box::pin(futures::stream::unfold(self, |mut reader| async move {
            match reader.read_next().await {
                Ok(Some(doc)) => Some((Ok(doc), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(e), reader)),
            }
        }))
    }
}
