use bson::{doc, Bson};
use bson::{Document, RawDocumentBuf};
use ftdc_importer::ChunkParser;
use ftdc_importer::{Chunk, MetricType};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn test_parse_chunk() -> io::Result<()> {
    let path = Path::new("tests/fixtures/metric-example.bson");
    let mut file = File::open(path)?;
    let mut doc_data = Vec::new();
    file.read_to_end(&mut doc_data)?;

    // Parse the BSON document
    let doc = bson::from_slice::<Document>(&doc_data).unwrap();

    println!("\n=== Testing parse_chunk method ===");

    let chunk_parser = ChunkParser;
    let chunk = chunk_parser.parse_chunk_header(&doc).unwrap();
    assert!(!chunk.reference_doc.is_empty());
    assert_eq!(chunk.n_keys, 3479);
    assert_eq!(chunk.n_deltas, 299);
    assert!(!chunk.deltas.is_empty());

    assert_eq!(chunk.keys.len(), chunk.n_keys as usize);

    // Print the keys with index
    // for (i, key) in chunk.keys.iter().enumerate() {
    //     println!("Key {}: {}", i, key.0);
    // }
    Ok(())
}

#[tokio::test]
async fn test_decode_chunk() -> io::Result<()> {
    let lre_deltas = vec![1, 1, 1, 0, 3];
    let mut varint_deltas = Vec::new();
    for delta in &lre_deltas {
        ftdc_importer::encode_varint_vec(*delta, &mut varint_deltas).unwrap();
    }
    let timestamp = SystemTime::now();
    let chunk = Chunk {
        reference_doc: RawDocumentBuf::from_document(&doc! {}).unwrap(), // Empty reference document
        n_keys: 2,                                                       // Two metrics: "a" and "x"
        n_deltas: 3,                                                     // 3 samples per metric
        deltas: varint_deltas, // The varint-encoded deltas
        keys: vec![
            ("a".to_string(), MetricType::Int64, Bson::Int64(1)),
            ("x".to_string(), MetricType::Int64, Bson::Int64(2)),
        ],
        timestamp,
    };

    // Decode the chunk
    let chunk_parser = ChunkParser;

    let actual = chunk_parser.decode_chunk_values(&chunk).unwrap();
    let expected = [
        ("a", 1.0),
        ("a", 2.0),
        ("a", 3.0),
        ("a", 4.0),
        ("x", 2.0),
        ("x", 2.0),
        ("x", 2.0),
        ("x", 2.0),
    ];

    assert_eq!(actual.len(), 8); // Note:  8 = 2 metrics(keys) * 4 samples(deltas).  The reference doc counts as a sample too.
    for (actual, (name, value)) in actual.iter().zip(expected.iter()) {
        assert_eq!(actual.name, *name);
        assert_eq!(actual.value, *value);
        assert_eq!(actual.metric_type, MetricType::Int64);
    }
    Ok(())
}

#[tokio::test]
async fn test_decode_time_series() -> io::Result<()> {
    // This test verifies time series decoding with the "start" time series
    // which is always expected to be present in real FTDC data
    let lre_deltas = vec![1, 1, 1, 0, 2];
    let mut varint_deltas = Vec::new();
    for delta in &lre_deltas {
        ftdc_importer::encode_varint_vec(*delta, &mut varint_deltas).unwrap();
    }

    // Create a base timestamp for reference
    let base_ts = UNIX_EPOCH + Duration::from_secs(1600000000); // Some base timestamp

    // Create a chunk with "start" as the first metric
    let chunk = Chunk {
        reference_doc: RawDocumentBuf::from_document(&doc! {}).unwrap(),
        n_keys: 2,             // Two metrics: "start" and "x"
        n_deltas: 3,           // 3 samples per metric
        deltas: varint_deltas, // The varint-encoded deltas
        keys: vec![
            // "start" metric with millisecond values since UNIX epoch
            (
                "start".to_string(),
                MetricType::Int64,
                Bson::Int64(1600000000000),
            ),
            // Another random metric
            ("x".to_string(), MetricType::Int64, Bson::Int64(42)),
        ],
        timestamp: base_ts, // This should be ignored in favor of "start" values
    };

    // Decode the chunk
    let chunk_parser = ChunkParser;
    let actual = chunk_parser.decode_time_series(&chunk).unwrap();

    // Expected values for the metrics (each delta of 1 adds 1ms to the timestamp)
    let expected_metrics = [
        (
            "start",
            vec![
                1600000000000i64,
                1600000000001,
                1600000000002,
                1600000000003,
            ],
        ),
        ("x", vec![42i64, 42, 42, 42]),
    ];

    // Verify metrics were decoded correctly
    assert_eq!(actual.metrics.len(), 2);
    for (actual, (name, expected_values)) in actual.metrics.iter().zip(expected_metrics.iter()) {
        assert_eq!(actual.name, *name);
        assert_eq!(actual.values, *expected_values);
    }

    // Verify timestamps were taken from "start" metric
    assert_eq!(actual.timestamps.len(), 4); // 4 timestamps: reference + 3 deltas

    // Expected timestamps (converted from "start" metric values)
    let expected_timestamps = [
        UNIX_EPOCH + Duration::from_millis(1600000000000),
        UNIX_EPOCH + Duration::from_millis(1600000000001),
        UNIX_EPOCH + Duration::from_millis(1600000000002),
        UNIX_EPOCH + Duration::from_millis(1600000000003),
    ];

    // Check each timestamp matches what we expect from the "start" metric
    for (i, (actual, expected)) in actual
        .timestamps
        .iter()
        .zip(expected_timestamps.iter())
        .enumerate()
    {
        assert_eq!(
            actual, expected,
            "Timestamp at index {} doesn't match expected value from 'start' metric",
            i
        );
    }

    // Verify timestamps increase monotonically
    for i in 1..actual.timestamps.len() {
        assert!(
            actual.timestamps[i] > actual.timestamps[i - 1],
            "Timestamps should be strictly increasing"
        );
    }

    // Print timestamp difference information
    if actual.timestamps.len() >= 2 {
        let first = actual.timestamps[0];
        let last = actual.timestamps[actual.timestamps.len() - 1];
        let diff = last.duration_since(first).unwrap();

        println!("Timestamp range: {:?} to {:?}", first, last);
        println!("Total duration: {:?}", diff);
        println!(
            "Average increment: {:?}",
            diff / (actual.timestamps.len() as u32 - 1)
        );
        assert!(last >= first);
    }

    Ok(())
}

#[tokio::test]
async fn test_decode_time_series_with_example_file() -> io::Result<()> {
    let path = Path::new("tests/fixtures/metric-example.bson");
    let mut file = File::open(path)?;
    let mut doc_data = Vec::new();
    file.read_to_end(&mut doc_data)?;

    let doc = bson::from_slice::<Document>(&doc_data).unwrap();
    let chunk_parser = ChunkParser;
    let chunk = chunk_parser.parse_chunk_header(&doc).unwrap();
    let actual = chunk_parser.decode_time_series(&chunk).unwrap();
    assert!(!chunk.reference_doc.is_empty());
    assert_eq!(chunk.n_keys, 3479);
    assert_eq!(chunk.n_deltas, 299);
    assert!(!chunk.deltas.is_empty());

    assert_eq!(chunk.keys.len(), chunk.n_keys as usize);

    // Find the "start" metric in the keys
    let start_metric = chunk.keys.iter().find(|(name, _, _)| name == "start");
    assert!(
        start_metric.is_some(),
        "The 'start' metric should exist in the keys"
    );

    assert_eq!(actual.metrics.len(), 3479);
    assert_eq!(actual.timestamps.len(), 300); // 300 = reference + 299 deltas

    // Verify the "start" time series exists in decoded metrics
    let start_series = actual.metrics.iter().find(|m| m.name == "start");
    assert!(
        start_series.is_some(),
        "The 'start' time series should exist in the decoded metrics"
    );

    for time_series in actual.metrics {
        assert_eq!(time_series.values.len(), 300);
    }

    // Verify timestamps increase monotonically
    // Check timestamps are monotonically increasing
    let start_timestamp = start_metric
        .unwrap()
        .2
        .as_datetime()
        .unwrap()
        .to_system_time();
    for i in 1..actual.timestamps.len() {
        assert!(actual.timestamps[i] >= start_timestamp);
        assert!(actual.timestamps[i] > actual.timestamps[i - 1]);
    }

    Ok(())
}

#[test]
fn test_bson_to_i64() {
    use bson::Bson;
    use bson::RawDocumentBuf;
    use ftdc_importer::{Chunk, ChunkParser, MetricType};
    use std::time::SystemTime;

    // Test DateTime
    let dt = bson::DateTime::now();
    let expected_dt = dt.timestamp_millis();
    let bson_dt = Bson::DateTime(dt);

    // Create proper deltas - Keep it simple following test_decode_time_series pattern
    let lre_deltas = vec![1, 1, 1, 1, 1, 1, 1, 1]; // Just 1 delta of 1 for each metric
    let mut varint_deltas = Vec::new();
    for delta in &lre_deltas {
        ftdc_importer::encode_varint_vec(*delta, &mut varint_deltas).unwrap();
    }

    let chunk = Chunk {
        reference_doc: RawDocumentBuf::from_document(&doc! {}).unwrap(),
        n_keys: 8,   // Eight different metrics with the added 'start' metric
        n_deltas: 1, // Just one delta for simplicity
        deltas: varint_deltas,
        keys: vec![
            // start is required for decode_time_series
            ("start".to_string(), MetricType::Int64, Bson::Int64(1000)),
            (
                "datetime".to_string(),
                MetricType::DateTime,
                bson_dt.clone(),
            ),
            ("int32".to_string(), MetricType::Int32, Bson::Int32(42)),
            ("int64".to_string(), MetricType::Int64, Bson::Int64(9999)),
            ("double".to_string(), MetricType::Double, Bson::Double(3.14)),
            (
                "boolean_true".to_string(),
                MetricType::Boolean,
                Bson::Boolean(true),
            ),
            (
                "boolean_false".to_string(),
                MetricType::Boolean,
                Bson::Boolean(false),
            ),
            (
                "string".to_string(),
                MetricType::Int64,
                Bson::String("test".to_string()),
            ),
        ],
        timestamp: SystemTime::now(),
    };

    // Use decode_time_series instead of decode_chunk_values
    let chunk_parser = ChunkParser;
    let result = chunk_parser.decode_time_series(&chunk).unwrap();

    // Verify all metrics have the right number of values (initial + 1 delta)
    assert_eq!(result.metrics.len(), 8); // 8 metrics including start
    for metric in &result.metrics {
        assert_eq!(metric.values.len(), 2); // Initial value + 1 delta
    }

    // Find each time series and check the base value
    let dt_series = result
        .metrics
        .iter()
        .find(|m| m.name == "datetime")
        .unwrap();
    let int32_series = result.metrics.iter().find(|m| m.name == "int32").unwrap();
    let int64_series = result.metrics.iter().find(|m| m.name == "int64").unwrap();
    let double_series = result.metrics.iter().find(|m| m.name == "double").unwrap();
    let bool_true_series = result
        .metrics
        .iter()
        .find(|m| m.name == "boolean_true")
        .unwrap();
    let bool_false_series = result
        .metrics
        .iter()
        .find(|m| m.name == "boolean_false")
        .unwrap();
    let string_series = result.metrics.iter().find(|m| m.name == "string").unwrap();

    // Verify initial values are converted correctly from BSON to i64
    assert_eq!(dt_series.values[0], expected_dt);
    assert_eq!(int32_series.values[0], 42);
    assert_eq!(int64_series.values[0], 9999);
    assert_eq!(double_series.values[0], 3); // Double value 3.14 gets truncated to 3 when converted to i64
    assert_eq!(bool_true_series.values[0], 1);
    assert_eq!(bool_false_series.values[0], 0);
    assert_eq!(string_series.values[0], 0); // Other types should default to 0

    // Also verify delta is applied correctly (should be +1 to each value)
    assert_eq!(dt_series.values[1], expected_dt + 1);
    assert_eq!(int32_series.values[1], 43);
    assert_eq!(int64_series.values[1], 10000);
    assert_eq!(double_series.values[1], 4);
    assert_eq!(bool_true_series.values[1], 2);
    assert_eq!(bool_false_series.values[1], 1);
    assert_eq!(string_series.values[1], 1);
}
