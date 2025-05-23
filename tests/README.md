# FTDC Format Analysis

This directory contains tests and utilities for analyzing the FTDC (Full-Time Diagnostic Data Capture) format used by MongoDB.

## FTDC Format Overview

The FTDC format is a series of BSON documents used by MongoDB to store diagnostic data. It provides efficient storage of time-series metrics while maintaining queryability.

FTDC files contain three types of documents:

1. **Metadata** (type: 0) - Contains reference information about the MongoDB instance
2. **Metric** (type: 1) - Contains compressed metric data
3. **MetadataDelta** (type: 2) - Contains changes to the metadata

Metric documents contain compressed time-series data that follows this structure:

1. Each metric document contains multiple samples (up to 300 in our example)
2. Each sample contains multiple metrics (up to 3479 in our example)
3. The metrics are stored in a column-oriented format and compressed using:
   - Delta encoding
   - Run-length encoding of zeros
   - Varint compression
   - Zlib compression

## Metric Document Structure

A metric document has the following structure:

```
{
  "_id": <DateTime>,
  "type": 1,
  "data": <BinData>
}
```

The `data` field contains:
1. Uncompressed size (4 bytes, uint32, little-endian)
2. Zlib compressed data

After decompression, the data contains:
1. Reference document (BSON)
2. Sample count (4 bytes, uint32, little-endian)
3. Metric count (4 bytes, uint32, little-endian)
4. Compressed metrics array

## Test Files

### Actual Tests
- `ftdc_decoder_test.rs` - Tests the core FTDC decoding functionality including chunk parsing and time series conversion
- `integration_test.rs` - Tests the full workflow with real FTDC files and integration with Prometheus Remote Write protocol

### Utility Scripts
- `ftdc_extract_metric_test.rs` - Utility script to extract and dump sample metric documents for fixture preparation
- `show_reference_doc.rs` - Utility script to explore and analyze the structure of reference documents in FTDC chunks

## Generated Fixtures

- `metric-example.bson` - A raw BSON metric document
- `metric-<timestamp>.bson` - A raw BSON metric document with timestamp
- `metric-<timestamp>.json` - A JSON representation of the metric document
- `decompressed-metric.bin` - The decompressed data from a metric document
- `compressed-metrics-array.bin` - The compressed metrics array from a metric document
- `ftdc-metrics-example` - Example FTDC file used for integration tests 