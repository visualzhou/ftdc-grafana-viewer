use bson::doc;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use thiserror::Error;

pub mod ftdc_decoder;
pub mod prometheus;
pub mod reader;
mod varint;
pub mod victoria_metrics;

pub use ftdc_decoder::{Chunk, ChunkParser};
pub use prometheus::{ImportMetadata, PrometheusRemoteWriteClient};
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
    Server {
        status: reqwest::StatusCode,
        message: String,
    },
}

pub type Result<T> = std::result::Result<T, FtdcError>;

/// Represents a single FTDC document containing multiple metrics, in time series format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtdcDocumentTS {
    pub metrics: Vec<FtdcTimeSeries>,
    pub timestamps: Vec<SystemTime>,
}

/// Represents a single FTDC time series
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtdcTimeSeries {
    pub name: String,
    pub values: Vec<i64>,
}
