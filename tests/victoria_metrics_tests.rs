use ftdc_importer::{FtdcDocument, MetricType, MetricValue, VictoriaMetricsClient};
use std::time::{Duration, UNIX_EPOCH};
use tokio;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

#[tokio::test]
async fn test_victoria_metrics_client() {
    // Start a mock server
    let mock_server = MockServer::start().await;

    // Create a Victoria Metrics client pointing to our mock server
    let client = VictoriaMetricsClient::new(mock_server.uri(), 1000);

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

    // Setup the mock to expect a POST request to /write
    Mock::given(method("POST"))
        .and(path("/write"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Import the document
    let result = client.import_document(&doc).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_victoria_metrics_client_error() {
    // Start a mock server
    let mock_server = MockServer::start().await;

    // Create a Victoria Metrics client pointing to our mock server
    let client = VictoriaMetricsClient::new(mock_server.uri(), 1000);

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

    // Setup the mock to return a 500 error
    Mock::given(method("POST"))
        .and(path("/write"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal server error"))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Import the document
    let result = client.import_document(&doc).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_line_protocol_conversion() {
    // Create a Victoria Metrics client
    let client = VictoriaMetricsClient::new("http://localhost:8428".to_string(), 1000);

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

    // Convert to line protocol
    let lines = client.document_to_line_protocol(&doc).unwrap();

    // Print the lines for debugging
    println!("Line 1: {}", lines[0]);
    println!("Line 2: {}", lines[1]);

    // Check that we have the expected number of lines
    assert_eq!(lines.len(), 2);

    // Check that the lines are formatted correctly
    assert!(lines[0].contains("mongodb_ftdc,metric_type=double"));
    assert!(lines[0].contains("value=42.5"));

    assert!(lines[1].contains("mongodb_ftdc,metric_type=int64"));
    assert!(lines[1].contains("value=100"));

    // Check that special characters are handled correctly
    assert!(!lines[0].contains("test.metric.with.dots"));
    assert!(lines[0].contains("test_metric_with_dots"));
    assert!(!lines[1].contains("test metric with spaces"));
    assert!(lines[1].contains("test_metric_with_spaces"));
}
