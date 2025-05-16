# FTDC to Victoria Metrics Importer

A high-performance tool for importing MongoDB's FTDC (Full Time Diagnostic Data Capture) metrics into Victoria Metrics.

## Key Features

- **High Performance**: Imports 6 hours of FTDC data in ~20 seconds using a single core
- **Protobuf Integration**: Uses Prometheus remote write protocol with protobuf for efficient data transfer
- **Flexible Integration**: Works with Victoria Metrics and any Prometheus-compatible endpoint

## Usage

### Installation

```bash
# Clone the repository
git clone https://github.com/visualzhou/ftdc-grafana-viewer.git
cd ftdc-grafana-viewer

# Build the project
cargo build --release
```

### Basic Commands

```bash
# Import FTDC file to Victoria Metrics with the default VM URL.
./target/release/ftdc-importer /path/to/ftdc/file.ftdc

# Analyze FTDC file contents without importing
./target/release/ftdc-importer /path/to/ftdc/file.ftdc --check

# Import with cleanup of existing metrics
./target/release/ftdc-importer /path/to/ftdc/file.ftdc --vm-url http://localhost:8428 --clean

# Verify successful import
./target/release/ftdc-importer /path/to/ftdc/file.ftdc --vm-url http://localhost:8428 --verify
```

### Command Line Options

- `--vm-url`: Victoria Metrics URL (default: http://localhost:8428)
- `--batch-size`: Number of metrics to send in each batch (default: 1000)
- `-v, --verbose`: Enable verbose output
- `--check`: Analyze metrics without importing
- `--clean`: Remove existing metrics before importing
- `--verify`: Verify metrics after import

## How It Works

The importer:
1. Parses and decompresses MongoDB FTDC files
2. Extracts metrics into time series format
3. Converts to Prometheus protocol buffer format
4. Sends to Victoria Metrics via remote write API

## Check Mode

Check mode provides detailed analysis of FTDC files without sending data:

```bash
./target/release/ftdc-importer /path/to/ftdc/file.ftdc --check
```

This displays:
- Total documents and metrics count
- Unique metric names
- Metric type distribution
- Example metrics with formatting
- Full list of unique metric names

## FTDC Format

FTDC files contain MongoDB diagnostic data in a compressed format. They consist of:
- Metadata documents (type 0): Define metric structure
- Metric documents (type 1): Contain actual metric values
- Metadata delta documents (type 2): Contain structure changes

The importer efficiently processes these documents to extract valuable metrics for monitoring and analysis.

For more information about FTDC format, see: https://github.com/mongodb/mongo/tree/master/src/mongo/db/ftdc
