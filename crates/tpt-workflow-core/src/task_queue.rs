//! Task queue management

use crate::error::{Error, Result};
use crate::workflow::{TaskId, WorkflowExecutionId, StepId};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

/// Task status
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Scheduled,
    Running,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
}

/// Task in the queue
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueuedTask {
    pub task_id: TaskId,
    pub task_type: String,
    pub task_queue: String,
    pub input: Value,
    pub workflow_id: WorkflowExecutionId,
    pub step_id: StepId,
    pub attempt: u32,
    pub status: TaskStatus,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub timeout_policy: crate::workflow::TimeoutPolicy,
    pub retry_policy: crate::workflow::RetryPolicy,
    pub task_token: String,
    pub permission: crate::workflow::PermissionRequirement,
}

/// Task queue trait
#[async_trait]
pub trait TaskQueue: Send + Sync {
    /// Add a task to the queue
    async fn enqueue(&self, task: QueuedTask) -> Result<()>;
    
    /// Get next task from the queue
    async fn dequeue(&self, worker_id: &str, max_tasks: usize) -> Result<Vec<QueuedTask>>;
    
    /// Mark task as running
    async fn start_task(&self, task_token: &str, worker_id: &str) -> Result<()>;
    
    /// Complete a task
    async fn complete_task(&self, task_token: &str, output: Value) -> Result<()>;
    
    /// Fail a task
    async fn fail_task(&self, task_token: &str, error: String) -> Result<()>;
    
    /// Cancel a task
    async fn cancel_task(&self, task_token: &str, reason: String) -> Result<()>;
    
    /// Get task by token
    async fn get_task(&self, task_token: &str) -> Result<Option<QueuedTask>>;
    
    /// Get queue stats
    async fn get_stats(&self) -> Result<TaskQueueStats>;
    
    /// Heartbeat for a running task
    async fn heartbeat_task(&self, task_token: &str, progress: Value) -> Result<bool>;
}

/// Task queue statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskQueueStats {
    pub name: String,
    pub pending_tasks: u64,
    pub running_tasks: u64,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub updated_at: DateTime<Utc>,
}

/// In-memory task queue implementation
pub struct InMemoryTaskQueue {
    name: String,
    pending: Arc<Mutex<VecDeque<QueuedTask>>>,
    running: Arc<RwLock<HashMap<String, QueuedTask>>>,
    completed: Arc<RwLock<HashMap<String, QueuedTask>>>,
    stats: Arc<RwLock<TaskQueueStats>>,
}

impl InMemoryTaskQueue {
    pub fn new(name: String) -> Self {
        let now = Utc::now();
        Self {
            name: name.clone(),
            pending: Arc::new(Mutex::new(VecDeque::new())),
            running: Arc::new(RwLock::new(HashMap::new())),
            completed: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(TaskQueueStats {
                name,
                pending_tasks: 0,
                running_tasks: 0,
                completed_tasks: 0,
                failed_tasks: 0,
                updated_at: now,
            })),
        }
    }
    
    async fn update_stats(&self) {
        let mut stats = self.stats.write().await;
        let pending = self.pending.lock().await;
        let running = self.running.read().await;
        let completed = self.completed.read().await;
        
        stats.pending_tasks = pending.len() as u64;
        stats.running_tasks = running.len() as u64;
        stats.completed_tasks = completed
            .values()
            .filter(|t| t.status == TaskStatus::Completed)
            .count() as u64;
        stats.failed_tasks = completed
            .values()
            .filter(|t| t.status == TaskStatus::Failed)
            .count() as u64;
        stats.updated_at = Utc::now();
    }
}

#[async_trait]
impl TaskQueue for InMemoryTaskQueue {
    async fn enqueue(&self, task: QueuedTask) -> Result<()> {
        let mut pending = self.pending.lock().await;
        pending.push_back(task);
        self.update_stats().await;
        Ok(())
    }
    
    async fn dequeue(&self, worker_id: &str, max_tasks: usize) -> Result<Vec<QueuedTask>> {
        let mut pending = self.pending.lock().await;
        let mut running = self.running.write().await;
        let mut tasks = Vec::new();
        
        for _ in 0..max_tasks {
            if let Some(mut task) = pending.pop_front() {
                task.status = TaskStatus::Running;
                task.started_at = Some(Utc::now());
                running.insert(task.task_token.clone(), task.clone());
                tasks.push(task);
            } else {
                break;
            }
        }
        
        self.update_stats().await;
        Ok(tasks)
    }
    
    async fn start_task(&self, task_token: &str, worker_id: &str) -> Result<()> {
        let mut running = self.running.write().await;
        if let Some(task) = running.get_mut(task_token) {
            task.status = TaskStatus::Running;
            task.started_at = Some(Utc::now());
            self.update_stats().await;
            Ok(())
        } else {
            Err(Error::TaskNotFound(task_token.to_string()))
        }
    }
    
    async fn complete_task(&self, task_token: &str, output: Value) -> Result<()> {
        let mut running = self.running.write().await;
        let mut completed = self.completed.write().await;
        
        if let Some(mut task) = running.remove(task_token) {
            task.status = TaskStatus::Completed;
            task.completed_at = Some(Utc::now());
            completed.insert(task_token.to_string(), task);
            self.update_stats().await;
            Ok(())
        } else {
            Err(Error::TaskNotFound(task_token.to_string()))
        }
    }
    
    async fn fail_task(&self, task_token: &str, error: String) -> Result<()> {
        let mut running = self.running.write().await;
        let mut completed = self.completed.write().await;
        
        if let Some(mut task) = running.remove(task_token) {
            task.status = TaskStatus::Failed;
            task.completed_at = Some(Utc::now());
            completed.insert(task_token.to_string(), task);
            self.update_stats().await;
            Ok(())
        } else {
            Err(Error::TaskNotFound(task_token.to_string()))
        }
    }
    
    async fn cancel_task(&self, task_token: &str, reason: String) -> Result<()> {
        // Check pending queue
        let mut pending = self.pending.lock().await;
        if let Some(pos) = pending.iter().position(|t| t.task_token == task_token) {
            let mut task = pending.remove(pos).unwrap();
            task.status = TaskStatus::Cancelled;
            task.completed_at = Some(Utc::now());
            let mut completed = self.completed.write().await;
            completed.insert(task_token.to_string(), task);
            self.update_stats().await;
            return Ok(());
        }
        
        // Check running tasks
        let mut running = self.running.write().await;
        if let Some(mut task) = running.remove(task_token) {
            task.status = TaskStatus::Cancelled;
            task.completed_at = Some(Utc::now());
            let mut completed = self.completed.write().await;
            completed.insert(task_token.to_string(), task);
            self.update_stats().await;
            Ok(())
        } else {
            Err(Error::TaskNotFound(task_token.to_string()))
        }
    }
    
    async fn get_task(&self, task_token: &str) -> Result<Option<QueuedTask>> {
        // Check pending
        let pending = self.pending.lock().await;
        if let Some(task) = pending.iter().find(|t| t.task_token == task_token) {
            return Ok(Some(task.clone()));
        }
        drop(pending);
        
        // Check running
        let running = self.running.read().await;
        if let Some(task) = running.get(task_token) {
            return Ok(Some(task.clone()));
        }
        drop(running);
        
        // Check completed
        let completed = self.completed.read().await;
        Ok(completed.get(task_token).cloned())
    }
    
    async fn get_stats(&self) -> Result<TaskQueueStats> {
        self.update_stats().await;
        Ok(self.stats.read().await.clone())
    }
    
    async fn heartbeat_task(&self, task_token: &str, progress: Value) -> Result<bool> {
        let mut running = self.running.write().await;
        if let Some(task) = running.get_mut(task_token) {
            // In real implementation, check if task should be cancelled
            Ok(false)
        } else {
            Err(Error::TaskNotFound(task_token.to_string()))
        }
    }
}

/// Task queue manager - manages multiple task queues
pub struct TaskQueueManager {
    queues: Arc<RwLock<HashMap<String, Arc<dyn TaskQueue>>>>,
}

impl TaskQueueManager {
    pub fn new() -> Self {
        Self {
            queues: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Register a task queue
    pub async fn register_queue(&self, queue: Arc<dyn TaskQueue>) -> Result<()> {
        let stats = queue.get_stats().await?;
        let mut queues = self.queues.write().await;
        queues.insert(stats.name.clone(), queue);
        info!("Registered task queue: {}", stats.name);
        Ok(())
    }
    
    /// Get a task queue by name
    pub async fn get_queue(&self, name: &str) -> Result<Arc<dyn TaskQueue>> {
        let queues = self.queues.read().await;
        queues.get(name)
            .cloned()
            .ok_or_else(|| Error::TaskQueueNotFound(name.to_string()))
    }
    
    /// Schedule a task on a queue
    pub async fn schedule_task(
        &self,
        queue_name: &str,
        task_type: &str,
        input: Value,
        workflow_id: WorkflowExecutionId,
        step_id: StepId,
    ) -> Result<TaskId> {
        let queue = self.get_queue(queue_name).await?;
        
        let task_id = TaskId::new();
        let task_token = uuid::Uuid::new_v4().to_string();
        
        let task = QueuedTask {
            task_id: task_id.clone(),
            task_type: task_type.to_string(),
            task_queue: queue_name.to_string(),
            input,
            workflow_id,
            step_id,
            attempt: 1,
            status: TaskStatus::Pending,
            scheduled_at: Utc::now(),
            started_at: None,
            completed_at: None,
            timeout_policy: crate::workflow::TimeoutPolicy::default(),
            retry_policy: crate::workflow::RetryPolicy::default(),
            task_token,
            permission: crate::workflow::PermissionRequirement::default(),
        };
        
        queue.enqueue(task).await?;
        debug!("Scheduled task {} on queue {}", task_id, queue_name);
        
        Ok(task_id)
    }
    
    /// Poll tasks for a worker
    pub async fn poll_tasks(&self, worker_id: &str, queue_names: &[String], max_tasks: usize) -> Result<Vec<QueuedTask>> {
        let mut all_tasks = Vec::new();
        
        for queue_name in queue_names {
            if let Ok(queue) = self.get_queue(queue_name).await {
                let tasks = queue.dequeue(worker_id, max_tasks).await?;
                all_tasks.extend(tasks);
                if all_tasks.len() >= max_tasks {
                    break;
                }
            }
        }
        
        Ok(all_tasks)
    }
    
    /// List all queue names
    pub async fn list_queues(&self) -> Result<Vec<String>> {
        let queues = self.queues.read().await;
        Ok(queues.keys().cloned().collect())
    }
}

impl Default for TaskQueueManager {
    fn default() -> Self {
        Self::new()
    }
}