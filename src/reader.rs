use bson::{Bson, Document};
use futures::Stream;
use std::path::Path;
use std::pin::Pin;
use std::time::SystemTime;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};

use crate::{Compression, FtdcDocument, FtdcError, MetricType, MetricValue};

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

    /// Reads and processes the next FTDC document
    pub async fn read_next(&mut self) -> ReaderResult<Option<FtdcDocument>> {
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
                // Metadata is only for one-shot metrics that have no time series, like "architecture" and "OS".
                if let Some(Bson::Document(ref_doc)) = doc.get("doc") {
                    println!(
                        "Found reference document (type 0) with {} fields",
                        ref_doc.len()
                    );
                    self.reference_doc = Some(ref_doc.clone());
                    let metrics = self.extract_metrics(ref_doc, timestamp, "")?;
                    Ok(Some(FtdcDocument { timestamp, metrics }))
                } else {
                    println!("WARNING: Metadata document missing 'doc' field");
                    Box::pin(self.read_next()).await
                }
            }
            FtdcDocType::Metric => {
                // Extract and decompress metric data
                if let Some(Bson::Binary(bin)) = doc.get("data") {
                    println!(
                        "Processing metric document (type 1) with chunk of binary data: {} bytes",
                        bin.bytes.len()
                    );

                    // Extract numeric values from reference document to use for delta decoding
                    let mut reference_values = Vec::new();
                    let mut ref_keys = Vec::new();

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
                                    extract_numeric_values_recursive(
                                        subdoc,
                                        &field_name,
                                        keys,
                                        values,
                                    );
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

                    extract_numeric_values_recursive(
                        ref_doc,
                        "",
                        &mut ref_keys,
                        &mut reference_values,
                    );

                    println!(
                        "\tReference document has {} key/value pairs",
                        reference_values.len()
                    );

                    // Decompress using reference values
                    let decompressed = match Compression::decompress_metrics_chunk(
                        &bin.bytes,
                        Some(&reference_values),
                    ) {
                        Ok(values) => values,
                        Err(e) => {
                            println!("Decompression error: {:?}", e);
                            // Fallback to using reference document if decompression fails
                            let metrics = self.extract_metrics(ref_doc, timestamp, "")?;
                            return Ok(Some(FtdcDocument { timestamp, metrics }));
                        }
                    };

                    println!("\tSuccessfully decompressed {} values", decompressed.len());

                    // Create metrics directly from the column-oriented data
                    let mut metrics = Vec::new();

                    // Skip the first value which is the timestamp in seconds
                    let metric_count = ref_keys.len();
                    if decompressed.len() >= metric_count && !ref_keys.is_empty() {
                        for (i, key) in ref_keys.iter().enumerate() {
                            if i < decompressed.len() {
                                let value = decompressed[i];

                                // Determine metric type from key naming convention
                                // (This would be better done with a proper schema)
                                let metric_type =
                                    if key.contains("_timestamp_") || key.contains("Time") {
                                        MetricType::DateTime
                                    } else if key.contains("_ismaster_")
                                        || key.contains("Enabled")
                                        || key.contains("_ok")
                                    {
                                        MetricType::Boolean
                                    } else if value > 0x7FFFFFFF {
                                        MetricType::Int64
                                    } else {
                                        MetricType::Int32
                                    };

                                // Convert value based on type
                                let f64_value = match metric_type {
                                    MetricType::Double => f64::from_bits(value),
                                    MetricType::Int32 => value as i32 as f64,
                                    MetricType::Int64 => value as i64 as f64,
                                    MetricType::Boolean => (value != 0) as i32 as f64,
                                    MetricType::DateTime => value as f64,
                                    _ => value as f64,
                                };

                                metrics.push(MetricValue {
                                    name: key.clone(),
                                    value: f64_value,
                                    timestamp,
                                    metric_type,
                                });
                            }
                        }
                    } else {
                        println!("WARNING: Decompressed data size ({}) doesn't match reference keys count ({})",
                                decompressed.len(), metric_count);
                        // Fallback to using reference document
                        let metrics = self.extract_metrics(ref_doc, timestamp, "")?;
                        return Ok(Some(FtdcDocument { timestamp, metrics }));
                    }

                        Ok(Some(FtdcDocument { timestamp, metrics }))
                    } else {
                        // Fallback to using reference document if no compressed data
                        let metrics = self.extract_metrics(ref_doc, timestamp, "")?;
                        Ok(Some(FtdcDocument { timestamp, metrics }))
                    }
                } else {
                    Err(FtdcError::Format(
                        "No reference document available".to_string(),
                    ))
                }
            }
            FtdcDocType::MetadataDelta => {
                // Skip metadata delta documents in the stream
                println!("Skipping MetadataDelta document (type 2) ");
                Box::pin(self.read_next()).await
            }
        }
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
