# FTDC Format Analysis

This directory contains tests and utilities for analyzing the FTDC (Full-Time Diagnostic Data Capture) format used by MongoDB.

## FTDC Document Types

Based on our analysis, an FTDC file contains three types of documents:

1. **Metadata** (type: 0) - Contains reference information about the MongoDB instance
2. **Metric** (type: 1) - Contains compressed metric data
3. **MetadataDelta** (type: 2) - Contains changes to the metadata

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

- `ftdc_format_test.rs` - Detects and prints information about the different types of FTDC chunks
- `ftdc_metric_parser_test.rs` - Analyzes the structure of a metric document
- `ftdc_chunk_detector_test.rs` - Implements the full FTDC parsing logic
- `ftdc_extract_metric_test.rs` - Extracts and dumps a single metric document

## Generated Files

- `metric-example.bson` - A raw BSON metric document
- `metric-<timestamp>.bson` - A raw BSON metric document with timestamp
- `metric-<timestamp>.json` - A JSON representation of the metric document
- `decompressed-metric.bin` - The decompressed data from a metric document
- `compressed-metrics-array.bin` - The compressed metrics array from a metric document

## FTDC Format Summary

The FTDC format is a series of BSON documents. Metric documents contain compressed time-series data that follows this structure:

1. Each metric document contains multiple samples (3479 in our example)
2. Each sample contains multiple metrics (299 in our example)
3. The metrics are stored in a column-oriented format and compressed using:
   - Delta encoding
   - Run-length encoding of zeros
   - Varint compression
   - Zlib compression

This format allows for efficient storage of time-series metrics data while maintaining the ability to query and analyze the data. 