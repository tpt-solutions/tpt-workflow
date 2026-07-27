//! Persistence layer for workflow state

use crate::error::Result;
use crate::workflow::{
    WorkflowDefinition, WorkflowDefinitionId, WorkflowEvent, WorkflowExecution,
    WorkflowExecutionId, WorkflowExecutionStatus,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Trait for persistence layer
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    // Workflow Definition
    async fn save_workflow_definition(&self, definition: &WorkflowDefinition) -> Result<()>;
    async fn get_workflow_definition(
        &self,
        id: &WorkflowDefinitionId,
        version: Option<&str>,
    ) -> Result<Option<WorkflowDefinition>>;
    async fn list_workflow_definitions(
        &self,
        page_size: usize,
        page_token: Option<String>,
        name_filter: Option<&str>,
    ) -> Result<(Vec<WorkflowDefinition>, Option<String>)>;
    async fn delete_workflow_definition(
        &self,
        id: &WorkflowDefinitionId,
        version: Option<&str>,
    ) -> Result<bool>;

    // Workflow Execution
    async fn save_workflow_execution(&self, execution: &WorkflowExecution) -> Result<()>;
    async fn get_workflow_execution(
        &self,
        id: &WorkflowExecutionId,
    ) -> Result<Option<WorkflowExecution>>;
    async fn list_workflow_executions(
        &self,
        page_size: usize,
        page_token: Option<String>,
        workflow_definition_id: Option<&WorkflowDefinitionId>,
        statuses: Option<Vec<WorkflowExecutionStatus>>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<(Vec<WorkflowExecution>, Option<String>)>;

    // Workflow History/Events
    async fn append_workflow_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        events: Vec<WorkflowEvent>,
    ) -> Result<()>;
    async fn get_workflow_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        after_event_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<WorkflowEvent>>;
    async fn get_workflow_event_count(&self, workflow_id: &WorkflowExecutionId) -> Result<u64>;

    // Task state
    async fn save_task_state(&self, task_id: &str, state: Value) -> Result<()>;
    async fn get_task_state(&self, task_id: &str) -> Result<Option<Value>>;
}

/// In-memory persistence layer (for testing/dev)
pub struct InMemoryPersistence {
    definitions: Arc<RwLock<HashMap<String, HashMap<String, WorkflowDefinition>>>>,
    executions: Arc<RwLock<HashMap<String, WorkflowExecution>>>,
    events: Arc<RwLock<HashMap<String, Vec<WorkflowEvent>>>>,
    task_states: Arc<RwLock<HashMap<String, Value>>>,
}

impl InMemoryPersistence {
    pub fn new() -> Self {
        Self {
            definitions: Arc::new(RwLock::new(HashMap::new())),
            executions: Arc::new(RwLock::new(HashMap::new())),
            events: Arc::new(RwLock::new(HashMap::new())),
            task_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl PersistenceLayer for InMemoryPersistence {
    async fn save_workflow_definition(&self, definition: &WorkflowDefinition) -> Result<()> {
        let mut definitions = self.definitions.write().await;
        let versions = definitions
            .entry(definition.id.to_string())
            .or_insert_with(HashMap::new);
        versions.insert(definition.version.clone(), definition.clone());
        debug!(
            "Saved workflow definition: {} v{}",
            definition.id, definition.version
        );
        Ok(())
    }

    async fn get_workflow_definition(
        &self,
        id: &WorkflowDefinitionId,
        version: Option<&str>,
    ) -> Result<Option<WorkflowDefinition>> {
        let definitions = self.definitions.read().await;
        let versions = definitions.get(&id.to_string());

        match versions {
            Some(versions) => {
                if let Some(v) = version {
                    Ok(versions.get(v).cloned())
                } else {
                    // Return latest version
                    let latest = versions.values().max_by_key(|d| &d.version);
                    Ok(latest.cloned())
                }
            }
            None => Ok(None),
        }
    }

    async fn list_workflow_definitions(
        &self,
        page_size: usize,
        page_token: Option<String>,
        name_filter: Option<&str>,
    ) -> Result<(Vec<WorkflowDefinition>, Option<String>)> {
        let definitions = self.definitions.read().await;
        let mut all: Vec<WorkflowDefinition> = definitions
            .values()
            .flat_map(|v| v.values())
            .filter(|d| name_filter.is_none_or(|f| d.name.contains(f)))
            .cloned()
            .collect();

        // Sort by name then version
        all.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.version.cmp(&b.version)));

        let offset = page_token
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let end = (offset + page_size).min(all.len());
        let page = all[offset..end].to_vec();

        let next_token = if end < all.len() {
            Some(end.to_string())
        } else {
            None
        };

        Ok((page, next_token))
    }

    async fn delete_workflow_definition(
        &self,
        id: &WorkflowDefinitionId,
        version: Option<&str>,
    ) -> Result<bool> {
        let mut definitions = self.definitions.write().await;

        if let Some(versions) = definitions.get_mut(&id.to_string()) {
            if let Some(v) = version {
                versions.remove(v);
                if versions.is_empty() {
                    definitions.remove(&id.to_string());
                }
                Ok(true)
            } else {
                // Delete all versions
                definitions.remove(&id.to_string());
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }

    async fn save_workflow_execution(&self, execution: &WorkflowExecution) -> Result<()> {
        let mut executions = self.executions.write().await;
        executions.insert(execution.workflow_id.to_string(), execution.clone());
        debug!("Saved workflow execution: {}", execution.workflow_id);
        Ok(())
    }

    async fn get_workflow_execution(
        &self,
        id: &WorkflowExecutionId,
    ) -> Result<Option<WorkflowExecution>> {
        let executions = self.executions.read().await;
        Ok(executions.get(&id.to_string()).cloned())
    }

    async fn list_workflow_executions(
        &self,
        page_size: usize,
        page_token: Option<String>,
        workflow_definition_id: Option<&WorkflowDefinitionId>,
        statuses: Option<Vec<WorkflowExecutionStatus>>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<(Vec<WorkflowExecution>, Option<String>)> {
        let executions = self.executions.read().await;
        let mut filtered: Vec<WorkflowExecution> = executions
            .values()
            .filter(|e| {
                if let Some(wd_id) = workflow_definition_id {
                    if &e.workflow_definition_id != wd_id {
                        return false;
                    }
                }
                if let Some(ref statuses) = statuses {
                    if !statuses.contains(&e.status) {
                        return false;
                    }
                }
                if let Some(st) = start_time {
                    if e.started_at < st {
                        return false;
                    }
                }
                if let Some(et) = end_time {
                    if e.started_at > et {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        // Sort by start time descending
        filtered.sort_by_key(|b| std::cmp::Reverse(b.started_at));

        let offset = page_token
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let end = (offset + page_size).min(filtered.len());
        let page = filtered[offset..end].to_vec();

        let next_token = if end < filtered.len() {
            Some(end.to_string())
        } else {
            None
        };

        Ok((page, next_token))
    }

    async fn append_workflow_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        events: Vec<WorkflowEvent>,
    ) -> Result<()> {
        let mut events_map = self.events.write().await;
        let workflow_events = events_map
            .entry(workflow_id.to_string())
            .or_insert_with(Vec::new);
        let count = events.len();

        for mut event in events {
            // Assign event ID if not set
            if event.event_id == 0 {
                event.event_id = workflow_events.len() as u64 + 1;
            }
            workflow_events.push(event);
        }

        debug!("Appended {} events to workflow {}", count, workflow_id);
        Ok(())
    }

    async fn get_workflow_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        after_event_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<WorkflowEvent>> {
        let events_map = self.events.read().await;
        let workflow_events = events_map
            .get(&workflow_id.to_string())
            .cloned()
            .unwrap_or_default();

        let start = after_event_id
            .map(|id| {
                workflow_events
                    .iter()
                    .position(|e| e.event_id > id)
                    .unwrap_or(0)
            })
            .unwrap_or(0);

        let end = (start + limit).min(workflow_events.len());
        Ok(workflow_events[start..end].to_vec())
    }

    async fn get_workflow_event_count(&self, workflow_id: &WorkflowExecutionId) -> Result<u64> {
        let events_map = self.events.read().await;
        Ok(events_map
            .get(&workflow_id.to_string())
            .map(|e| e.len() as u64)
            .unwrap_or(0))
    }

    async fn save_task_state(&self, task_id: &str, state: Value) -> Result<()> {
        let mut task_states = self.task_states.write().await;
        task_states.insert(task_id.to_string(), state);
        Ok(())
    }

    async fn get_task_state(&self, task_id: &str) -> Result<Option<Value>> {
        let task_states = self.task_states.read().await;
        Ok(task_states.get(task_id).cloned())
    }
}

impl Default for InMemoryPersistence {
    fn default() -> Self {
        Self::new()
    }
}

/// Postgres persistence layer (placeholder)
#[cfg(feature = "postgres")]
pub struct PostgresPersistence {
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresPersistence {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl PersistenceLayer for PostgresPersistence {
    async fn save_workflow_definition(&self, definition: &WorkflowDefinition) -> Result<()> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn get_workflow_definition(
        &self,
        id: &WorkflowDefinitionId,
        version: Option<&str>,
    ) -> Result<Option<WorkflowDefinition>> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn list_workflow_definitions(
        &self,
        page_size: usize,
        page_token: Option<String>,
        name_filter: Option<&str>,
    ) -> Result<(Vec<WorkflowDefinition>, Option<String>)> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn delete_workflow_definition(
        &self,
        id: &WorkflowDefinitionId,
        version: Option<&str>,
    ) -> Result<bool> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn save_workflow_execution(&self, execution: &WorkflowExecution) -> Result<()> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn get_workflow_execution(
        &self,
        id: &WorkflowExecutionId,
    ) -> Result<Option<WorkflowExecution>> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn list_workflow_executions(
        &self,
        page_size: usize,
        page_token: Option<String>,
        workflow_definition_id: Option<&WorkflowDefinitionId>,
        statuses: Option<Vec<WorkflowExecutionStatus>>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<(Vec<WorkflowExecution>, Option<String>)> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn append_workflow_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        events: Vec<WorkflowEvent>,
    ) -> Result<()> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn get_workflow_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        after_event_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<WorkflowEvent>> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn get_workflow_event_count(&self, workflow_id: &WorkflowExecutionId) -> Result<u64> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn save_task_state(&self, task_id: &str, state: Value) -> Result<()> {
        todo!("Postgres persistence not yet implemented")
    }

    async fn get_task_state(&self, task_id: &str) -> Result<Option<Value>> {
        todo!("Postgres persistence not yet implemented")
    }
}
