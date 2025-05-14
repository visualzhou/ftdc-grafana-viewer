use bson::{doc, Bson};
use bson::{Document, RawDocumentBuf};
use ftdc_importer::ChunkParser;
use ftdc_importer::{Chunk, MetricType};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::time::SystemTime;

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
    let expected = vec![
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
