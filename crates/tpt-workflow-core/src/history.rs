//! Workflow execution history store

use crate::error::{Error, Result};
use crate::workflow::{WorkflowEvent, WorkflowEventType, WorkflowExecutionId, StepId, TaskId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// History event with metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HistoryEvent {
    pub event_id: u64,
    pub workflow_id: WorkflowExecutionId,
    pub event_type: WorkflowEventType,
    pub timestamp: DateTime<Utc>,
    pub step_id: Option<StepId>,
    pub task_id: Option<TaskId>,
    pub attributes: Value,
}

/// Trait for history storage backends
#[async_trait]
pub trait HistoryStore: Send + Sync {
    /// Append events to history
    async fn append_events(&self, workflow_id: &WorkflowExecutionId, events: Vec<HistoryEvent>) -> Result<()>;
    
    /// Get events after a specific event ID (for replay)
    async fn get_events_after(&self, workflow_id: &WorkflowExecutionId, after_event_id: u64) -> Result<Vec<HistoryEvent>>;
    
    /// Get all events for a workflow
    async fn get_all_events(&self, workflow_id: &WorkflowExecutionId) -> Result<Vec<HistoryEvent>>;
    
    /// Get the last event ID for a workflow
    async fn get_last_event_id(&self, workflow_id: &WorkflowExecutionId) -> Result<Option<u64>>;
    
    /// Get event count for a workflow
    async fn get_event_count(&self, workflow_id: &WorkflowExecutionId) -> Result<u64>;
    
    /// Delete history for a workflow (for cleanup)
    async fn delete_history(&self, workflow_id: &WorkflowExecutionId) -> Result<()>;
}

/// In-memory history store (for testing/dev)
pub struct InMemoryHistoryStore {
    events: Arc<RwLock<HashMap<WorkflowExecutionId, Vec<HistoryEvent>>>>,
}

impl InMemoryHistoryStore {
    pub fn new() -> Self {
        Self {
            events: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl HistoryStore for InMemoryHistoryStore {
    async fn append_events(&self, workflow_id: &WorkflowExecutionId, events: Vec<HistoryEvent>) -> Result<()> {
        let mut events_map = self.events.write().await;
        let workflow_events = events_map.entry(workflow_id.clone()).or_insert_with(Vec::new);
        
        for mut event in events {
            event.event_id = workflow_events.len() as u64 + 1;
            workflow_events.push(event);
        }
        
        debug!("Appended {} events to workflow {}", events.len(), workflow_id);
        Ok(())
    }
    
    async fn get_events_after(&self, workflow_id: &WorkflowExecutionId, after_event_id: u64) -> Result<Vec<HistoryEvent>> {
        let events_map = self.events.read().await;
        let workflow_events = events_map.get(workflow_id).cloned().unwrap_or_default();
        
        Ok(workflow_events
            .into_iter()
            .filter(|e| e.event_id > after_event_id)
            .collect())
    }
    
    async fn get_all_events(&self, workflow_id: &WorkflowExecutionId) -> Result<Vec<HistoryEvent>> {
        let events_map = self.events.read().await;
        Ok(events_map.get(workflow_id).cloned().unwrap_or_default())
    }
    
    async fn get_last_event_id(&self, workflow_id: &WorkflowExecutionId) -> Result<Option<u64>> {
        let events_map = self.events.read().await;
        let workflow_events = events_map.get(workflow_id);
        Ok(workflow_events.map(|e| e.last().map(|evt| evt.event_id)).flatten())
    }
    
    async fn get_event_count(&self, workflow_id: &WorkflowExecutionId) -> Result<u64> {
        let events_map = self.events.read().await;
        Ok(events_map.get(workflow_id).map(|e| e.len() as u64).unwrap_or(0))
    }
    
    async fn delete_history(&self, workflow_id: &WorkflowExecutionId) -> Result<()> {
        let mut events_map = self.events.write().await;
        events_map.remove(workflow_id);
        info!("Deleted history for workflow {}", workflow_id);
        Ok(())
    }
}

impl Default for InMemoryHistoryStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Postgres-backed history store (placeholder)
#[cfg(feature = "postgres")]
pub struct PostgresHistoryStore {
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresHistoryStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl HistoryStore for PostgresHistoryStore {
    async fn append_events(&self, workflow_id: &WorkflowExecutionId, events: Vec<HistoryEvent>) -> Result<()> {
        // Implementation would use sqlx to batch insert events
        todo!("Postgres history store not yet implemented")
    }
    
    async fn get_events_after(&self, workflow_id: &WorkflowExecutionId, after_event_id: u64) -> Result<Vec<HistoryEvent>> {
        todo!("Postgres history store not yet implemented")
    }
    
    async fn get_all_events(&self, workflow_id: &WorkflowExecutionId) -> Result<Vec<HistoryEvent>> {
        todo!("Postgres history store not yet implemented")
    }
    
    async fn get_last_event_id(&self, workflow_id: &WorkflowExecutionId) -> Result<Option<u64>> {
        todo!("Postgres history store not yet implemented")
    }
    
    async fn get_event_count(&self, workflow_id: &WorkflowExecutionId) -> Result<u64> {
        todo!("Postgres history store not yet implemented")
    }
    
    async fn delete_history(&self, workflow_id: &WorkflowExecutionId) -> Result<()> {
        todo!("Postgres history store not yet implemented")
    }
}