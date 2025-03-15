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

## Usage

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/ftdc-importer.git
cd ftdc-importer

# Build the project
cargo build --release
```

### Running the Importer

```bash
# Basic usage
./target/release/ftdc-importer /path/to/ftdc/file.ftdc --vm-url http://localhost:8428

# With verbose output
./target/release/ftdc-importer /path/to/ftdc/file.ftdc --vm-url http://localhost:8428 -v

# Adjust batch size
./target/release/ftdc-importer /path/to/ftdc/file.ftdc --vm-url http://localhost:8428 --batch-size 5000
```

### Options

- `--vm-url`: Victoria Metrics URL (default: http://localhost:8428)
- `--batch-size`: Number of metrics to send in each batch (default: 1000)
- `-v, --verbose`: Enable verbose output
- `--check`: Check mode - analyze all metrics in the file and print statistics without sending

### Check Mode

The importer provides a special mode for inspecting FTDC files:

```bash
./target/release/ftdc-importer /path/to/ftdc/file.ftdc --check
```

This will display:
- Total number of documents and metrics
- Number of unique metric names
- Distribution of metric types (Int32, Int64, Double)
- Example of each metric type with line protocol format
- Sample of unique metric names

This mode is useful for understanding the content of FTDC files without actually sending data to Victoria Metrics.

## Victoria Metrics Integration

The importer converts FTDC metrics to InfluxDB Line Protocol format, which is compatible with Victoria Metrics. Each metric is sent with the following format:

```
mongodb_ftdc,metric_type=<type> value=<value> <timestamp>
```

Where:
- `metric_type` is the type of the metric (double, int32, int64)
- `value` is the metric value
- `timestamp` is the nanosecond-precision timestamp

## Appendix: FTDC Format Insights

During the implementation of this project, we discovered several important details about MongoDB's FTDC format that weren't explicitly documented:

### FTDC File Structure

1. **No Magic Header**: Unlike many binary formats, FTDC files don't have a magic header or signature bytes. Each file is simply a sequence of BSON documents.

2. **Document Format**: Each document in an FTDC file follows this structure:
   - 4-byte size prefix (little-endian) indicating the total document size in bytes
   - BSON document data (including the size bytes as part of the BSON format)

3. **Document Types**: FTDC files contain three types of documents:
   - **Metadata (type 0)**: Contains reference metrics structure and initial values
   - **Metric (type 1)**: Contains metric values that reference the structure defined in metadata
   - **Metadata Delta (type 2)**: Contains changes to the metadata structure

### Document Structure

1. **Common Fields**:
   - `_id`: Timestamp as BSON DateTime, representing when metrics were collected
   - `type`: Integer indicating document type (0=Metadata, 1=Metric, 2=MetadataDelta)
   - `doc`: For metadata documents, contains the actual metrics structure

2. **Nested Metrics**: Metrics are often deeply nested in a hierarchical structure:
   ```
   sysMaxOpenFiles: {
     start: <timestamp>,
     sys_max_file_handles: <value>,
     end: <timestamp>
   }
   ```

3. **Metric Types**: Metrics can be various BSON types:
   - Double (floating point values)
   - Int32 (32-bit integers)
   - Int64 (64-bit integers)
   - Nested documents (containing sub-metrics)

### Surprising Aspects

1. **Reference-Based Model**: The first document (metadata) establishes a reference structure that subsequent metric documents use. This is more efficient than repeating the structure in every document.

2. **No Compression Within Documents**: While the MongoDB documentation mentions compression, individual BSON documents within the FTDC file are not compressed. The compression mentioned likely refers to the delta encoding used between metric documents.

3. **Hierarchical Naming**: Metric names follow a hierarchical pattern based on their position in nested documents (e.g., `ulimits_cpuTime_secs_soft`).

4. **Timestamp Handling**: Each metric collection has an overall timestamp (`_id`), but individual metrics or sections may also have their own start/end timestamps.

5. **Special Values**: Some metrics use special values like `-1` or maximum int64 values to represent "unlimited" or "not applicable" states.

### Example Metrics

From our analysis of real FTDC files, we observed these common metrics:

1. **System Resource Metrics**:
   - `sysMaxOpenFiles_sys_max_file_handles`: Maximum number of file handles allowed by the system
   - `ulimits_cpuTime_secs_soft/hard`: CPU time limits (soft and hard)
   - `ulimits_fileSize_blocks_soft/hard`: File size limits
   - `ulimits_stackSize_kb_soft/hard`: Stack size limits
   - `ulimits_dataSegSize_kb_soft/hard`: Data segment size limits

2. **MongoDB-Specific Metrics**:
   - `hostInfo_system_numNumaNodes`: Number of NUMA nodes
   - `hostInfo_extra_pageSize`: System page size
   - `buildInfo_ok`: Build information status
   - `getCmdLineOpts_parsed_net_port`: Configured MongoDB port

3. **Performance Metrics**:
   - Memory usage statistics
   - Connection counts
   - Operation counters (queries, inserts, updates, deletes)
   - Replication lag metrics (for replica sets)

### Implementation Challenges and Lessons Learned

1. **BSON Document Handling**: The BSON format requires careful handling of document sizes and structure. We found that:
   - The 4-byte size prefix must be included in the document data when parsing
   - BSON documents must be read in their entirety to be valid
   - Error handling for malformed BSON is essential for robustness

2. **Recursive Metric Extraction**: Extracting metrics from nested documents required a recursive approach:
   - Metric names are constructed by joining parent and child keys with underscores
   - Different data types (Double, Int32, Int64) need proper conversion to a common format
   - Skipping special fields like timestamps and metadata is necessary to avoid duplicate metrics

3. **Document Type Handling**: Different document types require different processing:
   - Metadata documents establish the reference structure and provide initial values
   - Metric documents rely on the reference structure but provide updated values
   - Metadata delta documents can be skipped in simple implementations

4. **Streaming Large Files**: For efficient processing of large FTDC files:
   - Buffered reading is essential for performance
   - Async processing allows for better resource utilization
   - Stream-based APIs provide a clean interface for consumers

This understanding of the FTDC format was crucial for implementing an effective parser that can handle real-world MongoDB diagnostic data files.