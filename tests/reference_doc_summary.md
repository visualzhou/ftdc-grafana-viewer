# FTDC Reference Document Summary

The reference document in the FTDC metric chunk contains a snapshot of MongoDB server metrics. Here's a summary of its structure:

## Top-level Structure

The reference document has the following top-level fields:

- `start`: Timestamp when the metrics collection started
- `replSetGetStatus`: Replica set status information
- `local.oplog.rs.stats`: Statistics about the local oplog
- `config.transactions.stats`: Transaction statistics
- `config.image_collection.stats`: Image collection statistics
- `serverStatus`: Detailed server status metrics (largest section)
- `transportLayerStats`: Network transport layer statistics
- `systemMetrics`: System-level metrics (CPU, memory, disk, etc.)
- `end`: Timestamp when the metrics collection ended

## Key Sections

### serverStatus

This is the largest section and contains detailed metrics about the MongoDB server, including:

- Command statistics
- Connection information
- Database metrics
- Document operations
- Network statistics
- Query statistics
- Replication information
- Storage engine metrics (WiredTiger)

### systemMetrics

Contains system-level metrics such as:

- CPU usage and information
- Memory statistics
- Network statistics (TCP/IP)
- Disk I/O statistics
- File system mount information
- Virtual memory statistics

## Metrics Structure

The reference document serves as a template for the metrics in the compressed metrics array. Each metric in the array corresponds to a numeric value in this reference document. The metrics are stored in a column-oriented format, with each column representing a time series of values for a specific metric.

## Sample and Metric Counts

- Sample count: 3479 (number of time points in the time series)
- Metric count: 299 (number of metrics tracked at each time point)

This means the compressed metrics array contains 3479 × 299 = 1,040,221 individual metric values.

## Compression Approach

The metrics are compressed using:
1. Delta encoding (storing differences between consecutive values)
2. Run-length encoding of zeros (compressing sequences of zeros)
3. Varint compression (using fewer bytes for smaller numbers)
4. Zlib compression (general-purpose compression)

This multi-layered compression approach allows FTDC to efficiently store large amounts of time-series metrics data while maintaining the ability to query and analyze the data. 