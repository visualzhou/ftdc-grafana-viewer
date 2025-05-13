use bson::{ Document};
use ftdc_importer::{
    ChunkParser,   
};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;


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
    assert_eq!(chunk.reference_doc.len(), 9);
    assert_eq!(chunk.n_keys, 3479);
    assert_eq!(chunk.n_deltas, 299);
    assert!(!chunk.deltas.is_empty());

    // TODO(XXX): fix this: assertion failed. left: 3476 right: 3479
    assert_eq!(chunk.key_names.len(), chunk.n_keys as usize);
    Ok(())
}
