//! Worker management

use crate::error::{Error, Result};
use crate::workflow::{TaskId, StepId, WorkflowExecutionId};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Worker status
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Unspecified,
    Healthy,
    Unhealthy,
    Draining,
    Offline,
}

/// Worker information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Worker {
    pub worker_id: String,
    pub worker_type: String,
    pub task_queues: Vec<String>,
    pub status: WorkerStatus,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub capabilities: Value,
    pub metadata: Value,
    pub max_concurrent_tasks: u32,
    pub current_task_count: u32,
}

/// Worker registry trait
#[async_trait]
pub trait WorkerRegistry: Send + Sync {
    /// Register a worker
    async fn register_worker(&self, worker: Worker) -> Result<Worker>;
    
    /// Update worker heartbeat
    async fn heartbeat(&self, worker_id: &str, status: WorkerStatus, current_task_count: u32, metrics: Value) -> Result<WorkerHeartbeatResponse>;
    
    /// Deregister a worker
    async fn deregister_worker(&self, worker_id: &str, graceful: bool) -> Result<()>;
    
    /// Get worker by ID
    async fn get_worker(&self, worker_id: &str) -> Result<Option<Worker>>;
    
    /// List workers
    async fn list_workers(&self, task_queue: Option<&str>, status: Option<WorkerStatus>, page_size: usize, page_token: Option<String>) -> Result<(Vec<Worker>, Option<String>)>;
    
    /// Get available workers for a task queue
    async fn get_available_workers(&self, task_queue: &str) -> Result<Vec<Worker>>;
}

/// Worker heartbeat response
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerHeartbeatResponse {
    pub should_drain: bool,
    pub assigned_task_queues: Vec<String>,
    pub heartbeat_interval: Duration,
}

/// In-memory worker registry
pub struct InMemoryWorkerRegistry {
    workers: Arc<RwLock<HashMap<String, Worker>>>,
}

impl InMemoryWorkerRegistry {
    pub fn new() -> Self {
        Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl WorkerRegistry for InMemoryWorkerRegistry {
    async fn register_worker(&self, mut worker: Worker) -> Result<Worker> {
        let mut workers = self.workers.write().await;
        
        if workers.contains_key(&worker.worker_id) {
            return Err(Error::WorkerAlreadyRegistered(worker.worker_id.clone()));
        }
        
        worker.registered_at = Utc::now();
        worker.last_heartbeat = Utc::now();
        worker.status = WorkerStatus::Healthy;
        
        workers.insert(worker.worker_id.clone(), worker.clone());
        info!("Registered worker: {}", worker.worker_id);
        
        Ok(worker)
    }
    
    async fn heartbeat(&self, worker_id: &str, status: WorkerStatus, current_task_count: u32, metrics: Value) -> Result<WorkerHeartbeatResponse> {
        let mut workers = self.workers.write().await;
        
        if let Some(worker) = workers.get_mut(worker_id) {
            worker.status = status;
            worker.current_task_count = current_task_count;
            worker.last_heartbeat = Utc::now();
            worker.metadata = metrics;
            
            Ok(WorkerHeartbeatResponse {
                should_drain: matches!(status, WorkerStatus::Draining),
                assigned_task_queues: worker.task_queues.clone(),
                heartbeat_interval: Duration::seconds(30),
            })
        } else {
            Err(Error::WorkerNotFound(worker_id.to_string()))
        }
    }
    
    async fn deregister_worker(&self, worker_id: &str, graceful: bool) -> Result<()> {
        let mut workers = self.workers.write().await;
        
        if let Some(worker) = workers.get(worker_id) {
            if graceful && worker.current_task_count > 0 {
                // Mark as draining instead of removing
                if let Some(w) = workers.get_mut(worker_id) {
                    w.status = WorkerStatus::Draining;
                }
                info!("Worker {} marked as draining", worker_id);
            } else {
                workers.remove(worker_id);
                info!("Worker {} deregistered", worker_id);
            }
            Ok(())
        } else {
            Err(Error::WorkerNotFound(worker_id.to_string()))
        }
    }
    
    async fn get_worker(&self, worker_id: &str) -> Result<Option<Worker>> {
        let workers = self.workers.read().await;
        Ok(workers.get(worker_id).cloned())
    }
    
    async fn list_workers(&self, task_queue: Option<&str>, status: Option<WorkerStatus>, page_size: usize, page_token: Option<String>) -> Result<(Vec<Worker>, Option<String>)> {
        let workers = self.workers.read().await;
        let mut filtered: Vec<Worker> = workers
            .values()
            .filter(|w| {
                if let Some(tq) = task_queue {
                    if !w.task_queues.iter().any(|q| q == tq) {
                        return false;
                    }
                }
                if let Some(s) = status {
                    if w.status != s {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();
        
        // Simple pagination using page_token as offset
        let offset = page_token.and_then(|t| t.parse::<usize>().ok()).unwrap_or(0);
        let end = (offset + page_size).min(filtered.len());
        let page = filtered[offset..end].to_vec();
        
        let next_token = if end < filtered.len() {
            Some(end.to_string())
        } else {
            None
        };
        
        Ok((page, next_token))
    }
    
    async fn get_available_workers(&self, task_queue: &str) -> Result<Vec<Worker>> {
        let workers = self.workers.read().await;
        let available: Vec<Worker> = workers
            .values()
            .filter(|w| {
                w.status == WorkerStatus::Healthy
                    && w.task_queues.iter().any(|q| q == task_queue)
                    && w.current_task_count < w.max_concurrent_tasks
            })
            .cloned()
            .collect();
        
        Ok(available)
    }
}

impl Default for InMemoryWorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}