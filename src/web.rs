use axum::{
    extract::{Multipart, Path, Query, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tower_http::cors::CorsLayer;
use uuid::Uuid;

use crate::reader::FtdcReader;
use crate::session::{
    cleanup_expired_sessions, group_metrics, new_session_store, SessionData, SessionMetadata,
    SessionStore,
};

const UPLOAD_PAGE: &str = include_str!("../static/index.html");
const DASHBOARD_PAGE: &str = include_str!("../static/dashboard.html");

pub async fn start_server(port: u16) -> anyhow::Result<()> {
    let store = new_session_store();

    // Spawn cleanup task
    let cleanup_store = store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            interval.tick().await;
            cleanup_expired_sessions(
                &cleanup_store,
                std::time::Duration::from_secs(24 * 60 * 60),
            )
            .await;
        }
    });

    let app = Router::new()
        .route("/", get(upload_page))
        .route("/api/upload", post(handle_upload))
        .route("/api/sessions/{id}", get(get_session))
        .route("/api/sessions/{id}/metrics", get(get_metrics))
        .route("/dashboard/{id}", get(dashboard_page))
        .layer(CorsLayer::permissive())
        .with_state(store);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    println!("Server running at http://localhost:{port}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn upload_page() -> Html<&'static str> {
    Html(UPLOAD_PAGE)
}

async fn dashboard_page() -> Html<&'static str> {
    Html(DASHBOARD_PAGE)
}

#[derive(Deserialize)]
struct MetricsQuery {
    group: Option<String>,
    names: Option<String>,
}

async fn handle_upload(
    State(store): State<SessionStore>,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, AppError> {
    let mut file_data: Option<(String, Vec<u8>)> = None;

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        AppError::bad_request(format!("Failed to read multipart field: {e}"))
    })? {
        if field.name() == Some("file") {
            let filename = field.file_name().unwrap_or("upload").to_string();
            let data = field.bytes().await.map_err(|e| {
                AppError::bad_request(format!("Failed to read file data: {e}"))
            })?;
            file_data = Some((filename, data.to_vec()));
            break;
        }
    }

    let (filename, data) = file_data.ok_or_else(|| AppError::bad_request("No file uploaded"))?;

    // Process the uploaded file
    let session_data = process_upload(&filename, &data).await?;
    let session_id = session_data.metadata.id.clone();

    store.write().await.insert(session_id.clone(), session_data);

    Ok(Json(serde_json::json!({
        "session_id": session_id,
        "redirect_url": format!("/dashboard/{session_id}")
    })))
}

async fn get_session(
    State(store): State<SessionStore>,
    Path(id): Path<String>,
) -> Result<Json<SessionMetadata>, AppError> {
    let mut sessions = store.write().await;
    let session = sessions
        .get_mut(&id)
        .ok_or_else(|| AppError::not_found("Session not found"))?;
    session.last_accessed = Instant::now();
    Ok(Json(session.metadata.clone()))
}

async fn get_metrics(
    State(store): State<SessionStore>,
    Path(id): Path<String>,
    Query(query): Query<MetricsQuery>,
) -> Result<Response, AppError> {
    let mut sessions = store.write().await;
    let session = sessions
        .get_mut(&id)
        .ok_or_else(|| AppError::not_found("Session not found"))?;
    session.last_accessed = Instant::now();

    // Determine which metrics to return
    let metric_names: Vec<String> = if let Some(ref group) = query.group {
        session
            .metadata
            .groups
            .iter()
            .find(|g| g.name == *group)
            .map(|g| g.metrics.clone())
            .unwrap_or_default()
    } else if let Some(ref names) = query.names {
        names.split(',').map(|s| s.to_string()).collect()
    } else {
        // Return all metric names (no data) if no filter specified
        return Ok(Json(serde_json::json!({
            "timestamps": &session.timestamps,
            "metrics": []
        }))
        .into_response());
    };

    let metrics: Vec<serde_json::Value> = metric_names
        .iter()
        .filter_map(|name| {
            session.metrics.get(name).map(|values| {
                serde_json::json!({
                    "name": name,
                    "values": values
                })
            })
        })
        .collect();

    let body = serde_json::json!({
        "timestamps": &session.timestamps,
        "metrics": metrics
    });

    // Return as JSON with appropriate content type
    let json_bytes = serde_json::to_vec(&body).unwrap();
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json_bytes,
    )
        .into_response())
}

async fn process_upload(_filename: &str, data: &[u8]) -> Result<SessionData, AppError> {
    let is_zip = data.len() >= 4 && &data[0..4] == b"PK\x03\x04";

    let ftdc_files: Vec<Vec<u8>> = if is_zip {
        extract_ftdc_from_zip(data)?
    } else {
        // Treat as a single FTDC file
        vec![data.to_vec()]
    };

    if ftdc_files.is_empty() {
        return Err(AppError::bad_request(
            "No FTDC metric files found in the upload",
        ));
    }

    let mut all_timestamps: Vec<u64> = Vec::new();
    let mut all_metrics: HashMap<String, Vec<f64>> = HashMap::new();

    for ftdc_data in &ftdc_files {
        // Write to temp file for FtdcReader
        let tmp = tempfile::NamedTempFile::new()
            .map_err(|e| AppError::internal(format!("Failed to create temp file: {e}")))?;
        std::fs::write(tmp.path(), ftdc_data)
            .map_err(|e| AppError::internal(format!("Failed to write temp file: {e}")))?;

        let mut reader = FtdcReader::new(tmp.path()).await.map_err(|e| {
            AppError::internal(format!("Failed to open FTDC file: {e}"))
        })?;

        while let Ok(Some(doc)) = reader.read_next_time_series().await {
            let chunk_len = doc.timestamps.len();
            let chunk_start = all_timestamps.len();

            for ts in &doc.timestamps {
                let millis = ts
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                all_timestamps.push(millis);
            }

            // Pad existing metrics that aren't in this chunk
            for values in all_metrics.values_mut() {
                values.resize(chunk_start + chunk_len, f64::NAN);
            }

            // Add metric values
            for metric in &doc.metrics {
                let entry = all_metrics.entry(metric.name.clone()).or_insert_with(|| {
                    vec![f64::NAN; chunk_start]
                });
                entry.extend(metric.values.iter().map(|&v| v as f64));
            }
        }
    }

    if all_timestamps.is_empty() {
        return Err(AppError::bad_request(
            "No metric data found in the uploaded FTDC file(s)",
        ));
    }

    // Ensure all metrics have the same length
    let total_len = all_timestamps.len();
    for values in all_metrics.values_mut() {
        values.resize(total_len, f64::NAN);
    }

    let metric_names: Vec<String> = all_metrics.keys().cloned().collect();
    let groups = group_metrics(&metric_names);

    let time_range = (
        *all_timestamps.first().unwrap(),
        *all_timestamps.last().unwrap(),
    );

    let session_id = Uuid::new_v4().to_string()[..8].to_string();

    let metadata = SessionMetadata {
        id: session_id.clone(),
        created_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        time_range,
        metric_count: all_metrics.len(),
        sample_count: total_len,
        groups,
    };

    Ok(SessionData {
        metadata,
        timestamps: all_timestamps,
        metrics: all_metrics,
        last_accessed: Instant::now(),
    })
}

fn extract_ftdc_from_zip(data: &[u8]) -> Result<Vec<Vec<u8>>, AppError> {
    let cursor = Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|e| AppError::bad_request(format!("Invalid zip file: {e}")))?;

    let mut ftdc_files = Vec::new();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).map_err(|e| {
            AppError::internal(format!("Failed to read zip entry: {e}"))
        })?;

        // Skip directories
        if file.is_dir() {
            continue;
        }

        let name = file.name().to_string();

        // Look for FTDC metric files by name pattern
        let basename = name.rsplit('/').next().unwrap_or(&name);
        let is_ftdc = basename.starts_with("metrics.")
            || basename.starts_with("metric.")
            || basename == "metrics"
            || (!basename.contains('.')
                && !basename.starts_with('.')
                && file.size() > 100);

        // Skip known non-FTDC files
        let skip_extensions = [".log", ".json", ".txt", ".md", ".yaml", ".yml", ".toml", ".csv"];
        let should_skip = skip_extensions.iter().any(|ext| basename.ends_with(ext));

        if is_ftdc && !should_skip {
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).map_err(|e| {
                AppError::internal(format!("Failed to read zip entry data: {e}"))
            })?;

            // Quick BSON validation: first 4 bytes should be a reasonable document size
            if buf.len() >= 5 {
                let doc_size = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                if doc_size >= 5 && doc_size <= buf.len() {
                    ftdc_files.push(buf);
                }
            }
        }
    }

    Ok(ftdc_files)
}

// Error handling
struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }

    fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }

    fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}
