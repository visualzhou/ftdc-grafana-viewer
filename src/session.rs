use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize)]
pub struct MetricGroup {
    pub name: String,
    pub metrics: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionMetadata {
    pub id: String,
    pub created_at: u64,
    pub time_range: (u64, u64),
    pub metric_count: usize,
    pub sample_count: usize,
    pub groups: Vec<MetricGroup>,
}

#[derive(Debug, Clone)]
pub struct SessionData {
    pub metadata: SessionMetadata,
    pub timestamps: Vec<u64>,
    pub metrics: HashMap<String, Vec<f64>>,
    pub last_accessed: Instant,
}

pub type SessionStore = Arc<RwLock<HashMap<String, SessionData>>>;

pub fn new_session_store() -> SessionStore {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Group metrics by their logical category based on MongoDB FTDC metric naming conventions
pub fn group_metrics(metric_names: &[String]) -> Vec<MetricGroup> {
    let mut groups: HashMap<String, Vec<String>> = HashMap::new();

    for name in metric_names {
        let group_name = categorize_metric(name);
        groups.entry(group_name).or_default().push(name.clone());
    }

    let mut result: Vec<MetricGroup> = groups
        .into_iter()
        .map(|(name, mut metrics)| {
            metrics.sort();
            MetricGroup { name, metrics }
        })
        .collect();

    result.sort_by(|a, b| a.name.cmp(&b.name));
    result
}

fn categorize_metric(name: &str) -> String {
    if name.starts_with("systemMetrics_cpu") {
        return "CPU".to_string();
    }
    if name.starts_with("systemMetrics_mem") || name.starts_with("systemMetrics_memory") {
        return "Memory".to_string();
    }
    if name.starts_with("systemMetrics_disks") {
        return "Disk I/O".to_string();
    }
    if name.starts_with("systemMetrics_network") || name.starts_with("serverStatus_network") {
        return "Network".to_string();
    }
    if name.starts_with("serverStatus_wiredTiger") {
        return "WiredTiger".to_string();
    }
    if name.starts_with("serverStatus_opcounters") {
        return "Operations".to_string();
    }
    if name.starts_with("serverStatus_connections") {
        return "Connections".to_string();
    }
    if name.starts_with("serverStatus_locks") {
        return "Locks".to_string();
    }
    if name.starts_with("serverStatus_globalLock") {
        return "Global Lock".to_string();
    }
    if name.starts_with("serverStatus_mem") {
        return "Server Memory".to_string();
    }
    if name.starts_with("serverStatus_metrics") {
        return "Server Metrics".to_string();
    }
    if name.starts_with("serverStatus_repl") || name.starts_with("replSetGetStatus") {
        return "Replication".to_string();
    }
    if name.starts_with("serverStatus_transactions") {
        return "Transactions".to_string();
    }
    if name.starts_with("serverStatus_flowControl") {
        return "Flow Control".to_string();
    }
    if name.starts_with("serverStatus_") {
        let rest = &name["serverStatus_".len()..];
        if let Some(pos) = rest.find('_') {
            let component = &rest[..pos];
            return format!("Server: {component}");
        }
    }
    if name.starts_with("systemMetrics_") {
        let rest = &name["systemMetrics_".len()..];
        if let Some(pos) = rest.find('_') {
            let component = &rest[..pos];
            return format!("System: {component}");
        }
    }
    "Other".to_string()
}

/// Clean up sessions older than the given duration
pub async fn cleanup_expired_sessions(store: &SessionStore, max_age: std::time::Duration) {
    let mut sessions = store.write().await;
    let now = Instant::now();
    sessions.retain(|_, session| now.duration_since(session.last_accessed) < max_age);
}
