use ftdc_importer::{
    FtdcDocument, FtdcDocumentTS, FtdcTimeSeries, ImportMetadata, MetricType, MetricValue,
    VictoriaMetricsClient,
};
use std::time::{Duration, UNIX_EPOCH};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

#[tokio::test]
async fn test_victoria_metrics_client() {
    // Start a mock server
    let mock_server = MockServer::start().await;

    // Create a test document with metrics
    let timestamp = UNIX_EPOCH + Duration::from_secs(1615000000);
    let doc = FtdcDocument {
        timestamp,
        metrics: vec![
            MetricValue {
                name: "test_metric1".to_string(),
                value: 42.5,
                timestamp,
                metric_type: MetricType::Double,
            },
            MetricValue {
                name: "test_metric2".to_string(),
                value: 100.0,
                timestamp,
                metric_type: MetricType::Int64,
            },
        ],
    };

    // Create metadata and client
    let metadata = ImportMetadata::new(
        Some("test/sample.ftdc".to_string()),
        Some("test".to_string()),
    );
    let client = VictoriaMetricsClient::new(mock_server.uri(), 1000, metadata);

    // Setup the mock to expect a POST request to /write
    Mock::given(method("POST"))
        .and(path("/influx/write"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Import the document
    let result = client.import_document(&doc, false).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_victoria_metrics_client_error() {
    // Start a mock server
    let mock_server = MockServer::start().await;

    // Create a test document with metrics
    let timestamp = UNIX_EPOCH + Duration::from_secs(1615000000);
    let doc = FtdcDocument {
        timestamp,
        metrics: vec![MetricValue {
            name: "test_metric1".to_string(),
            value: 42.5,
            timestamp,
            metric_type: MetricType::Double,
        }],
    };

    // Create metadata and client
    let metadata = ImportMetadata::new(
        Some("test/sample.ftdc".to_string()),
        Some("test".to_string()),
    );
    let client = VictoriaMetricsClient::new(mock_server.uri(), 1000, metadata);

    // Setup the mock to return a 500 error
    Mock::given(method("POST"))
        .and(path("/influx/write"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal server error"))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Import the document
    let result = client.import_document(&doc, false).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_line_protocol_conversion() {
    // Create a test document with metrics
    let timestamp = UNIX_EPOCH + Duration::from_secs(1615000000);
    let doc = FtdcDocument {
        timestamp,
        metrics: vec![
            MetricValue {
                name: "test.metric.with.dots".to_string(),
                value: 42.5,
                timestamp,
                metric_type: MetricType::Double,
            },
            MetricValue {
                name: "test metric with spaces".to_string(),
                value: 100.0,
                timestamp,
                metric_type: MetricType::Int64,
            },
        ],
    };

    // Create metadata and client
    let metadata = ImportMetadata::new(
        Some("test/sample.ftdc".to_string()),
        Some("test".to_string()),
    );
    let client = VictoriaMetricsClient::new("http://localhost:8428".to_string(), 1000, metadata);

    // Convert to line protocol
    let lines = client.document_to_line_protocol(&doc).unwrap();

    // Print the lines for debugging
    println!("Line 1: {}", lines[0]);
    println!("Line 2: {}", lines[1]);

    // Check that we have the expected number of lines
    assert_eq!(lines.len(), 2);

    // Check that the lines are formatted correctly
    assert!(lines[0].contains("test.metric.with.dots"));
    assert!(lines[0].contains("value=42.5"));

    assert!(lines[1].contains("test metric with spaces"));
    assert!(lines[1].contains("value=100"));
}

#[tokio::test]
async fn test_time_series_line_protocol_conversion() {
    // Create a test FtdcDocumentTS with metrics and timestamps
    let base_timestamp = UNIX_EPOCH + Duration::from_secs(1615000000);

    // Create a series of timestamps, 5 samples at 1-second intervals
    let timestamps = vec![
        base_timestamp,
        base_timestamp + Duration::from_secs(1),
        base_timestamp + Duration::from_secs(2),
        base_timestamp + Duration::from_secs(3),
        base_timestamp + Duration::from_secs(4),
    ];

    // Create two time series with 5 values each
    let doc = FtdcDocumentTS {
        metrics: vec![
            FtdcTimeSeries {
                name: "time_series_1".to_string(),
                values: vec![10, 20, 30, 40, 50],
            },
            FtdcTimeSeries {
                name: "time_series_2".to_string(),
                values: vec![100, 200, 300, 400, 500],
            },
        ],
        timestamps: timestamps.clone(),
    };

    // Start a mock server
    let mock_server = MockServer::start().await;

    // Create metadata and client
    let metadata = ImportMetadata::new(
        Some("test/sample.ftdc".to_string()),
        Some("test".to_string()),
    );
    let client = VictoriaMetricsClient::new(mock_server.uri(), 1000, metadata);

    // Setup the mock to expect a POST request to /influx/write
    Mock::given(method("POST"))
        .and(path("/influx/write"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Import the document
    let result = client.import_document_ts(&doc, false).await;
    assert!(
        result.is_ok(),
        "Failed to import time series document: {:?}",
        result
    );
}
