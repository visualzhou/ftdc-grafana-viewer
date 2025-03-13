# FTDC to Victoria Metrics Importer: Phase 1 (MVP)

## Goal

Create a minimal viable product that can read MongoDB's FTDC (Full Time Diagnostic Data Capture) format metrics files and successfully import the data into Victoria Metrics while handling the compressed format efficiently.

FTDC: https://github.com/mongodb/mongo/tree/master/src/mongo/db/ftdc

## Requirements

- Parse and decompress MongoDB FTDC files
- Extract essential metrics (timestamps, metric names, and values)
- Convert metrics to a format Victoria Metrics can ingest
- Import the converted metrics into Victoria Metrics
- Handle files of at least small to medium size (up to ~50MB compressed)
- Maintain data integrity throughout the process
- Provide basic error reporting

## Implementation Plan

### FTDC Parser Component

- Implement Zstandard decompression
- Create basic BSON document parsing
- Extract metric names, values, and timestamps
- Focus on correctness rather than optimization

### Simple Metrics Converter

- Convert extracted metrics to InfluxDB Line Protocol (efficient format for Victoria Metrics)
- Preserve timestamps and metric values
- Maintain essential metadata and labels

### Basic Import Mechanism

- Implement direct HTTP POST to Victoria Metrics import endpoint
- Process files sequentially without advanced batching
- Include simple error handling and reporting

### Testing Framework

- Create small test FTDC files with known metric values
- Implement verification tests to ensure data matches after import
- Test parser with various small valid FTDC files
- Include basic error case testing (malformed files, connection issues)

### Minimal CLI

- Accept input file path and Victoria Metrics endpoint URL
- Provide basic logging of the process
- Report simple statistics (metrics processed, import time)

## Implementation Notes

The Phase 1 implementation will prioritize getting a working end-to-end solution before adding optimizations for performance or advanced features. This approach will validate the core concept quickly while establishing a foundation that can be expanded in later phases.