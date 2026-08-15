//! Persistence layer for workflow state

#[cfg(feature = "postgres")]
use sqlx::Row;

#[cfg(feature = "postgres")]
use crate::error::Error;
use crate::error::Result;
#[cfg(feature = "postgres")]
use crate::workflow::{StepId, TaskId};
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

/// Postgres-backed persistence layer (Phase 4: tpt-keystone-db integration).
///
/// Implements the durable append of workflow event history and the historical
/// query API against a Postgres wire-protocol connection (via `sqlx`). Schema
/// is managed by embedded migrations; call [`PostgresPersistence::migrate`] or
/// [`PostgresPersistence::connect`].
#[cfg(feature = "postgres")]
pub struct PostgresPersistence {
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresPersistence {
    /// Create from an existing pool. The caller is responsible for running
    /// [`PostgresPersistence::migrate`] (or having already applied migrations).
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Connect to Postgres via a connection string and run migrations.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .map_err(Error::Database)?;
        let this = Self { pool };
        this.migrate().await?;
        Ok(this)
    }

    /// Run embedded SQL migrations (definitions / executions / events / tasks).
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| Error::Migration(e.to_string()))?;
        Ok(())
    }

    /// The underlying connection pool.
    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl PersistenceLayer for PostgresPersistence {
    async fn save_workflow_definition(&self, d: &WorkflowDefinition) -> Result<()> {
        let steps = serde_json::to_value(&d.steps)?;
        sqlx::query(
            "INSERT INTO workflow_definitions \
             (id, version, name, description, steps, created_at, updated_at, metadata, search_attributes) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (id, version) DO UPDATE SET \
               name = EXCLUDED.name, \
               description = EXCLUDED.description, \
               steps = EXCLUDED.steps, \
               updated_at = EXCLUDED.updated_at, \
               metadata = EXCLUDED.metadata, \
               search_attributes = EXCLUDED.search_attributes",
        )
        .bind(d.id.to_string())
        .bind(&d.version)
        .bind(&d.name)
        .bind(&d.description)
        .bind(steps)
        .bind(d.created_at)
        .bind(d.updated_at)
        .bind(&d.metadata)
        .bind(&d.search_attributes)
        .execute(&self.pool)
        .await
        .map_err(Error::Database)?;
        Ok(())
    }

    async fn get_workflow_definition(
        &self,
        id: &WorkflowDefinitionId,
        version: Option<&str>,
    ) -> Result<Option<WorkflowDefinition>> {
        let row = match version {
            Some(v) => sqlx::query(
                "SELECT id, version, name, description, steps, created_at, updated_at, metadata, search_attributes \
                 FROM workflow_definitions WHERE id = $1 AND version = $2",
            )
            .bind(id.to_string())
            .bind(v)
            .fetch_optional(&self.pool)
            .await
            .map_err(Error::Database)?,
            None => sqlx::query(
                "SELECT id, version, name, description, steps, created_at, updated_at, metadata, search_attributes \
                 FROM workflow_definitions WHERE id = $1 ORDER BY version DESC LIMIT 1",
            )
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(Error::Database)?,
        };
        row.map(row_to_definition).transpose()
    }

    async fn list_workflow_definitions(
        &self,
        page_size: usize,
        page_token: Option<String>,
        name_filter: Option<&str>,
    ) -> Result<(Vec<WorkflowDefinition>, Option<String>)> {
        let offset = page_token
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let mut qb = sqlx::QueryBuilder::new(
            "SELECT id, version, name, description, steps, created_at, updated_at, metadata, search_attributes FROM workflow_definitions",
        );
        if let Some(name) = name_filter {
            qb.push(" WHERE name ILIKE ");
            qb.push_bind(format!("%{}%", name));
        }
        qb.push(" ORDER BY name ASC, version ASC LIMIT ");
        qb.push_bind((page_size + 1) as i64);
        qb.push(" OFFSET ");
        qb.push_bind(offset as i64);

        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(Error::Database)?;
        let next = if rows.len() > page_size {
            Some((offset + page_size).to_string())
        } else {
            None
        };
        let page = rows
            .into_iter()
            .take(page_size)
            .map(row_to_definition)
            .collect::<Result<Vec<_>>>()?;
        Ok((page, next))
    }

    async fn delete_workflow_definition(
        &self,
        id: &WorkflowDefinitionId,
        version: Option<&str>,
    ) -> Result<bool> {
        let affected = match version {
            Some(v) => {
                sqlx::query("DELETE FROM workflow_definitions WHERE id = $1 AND version = $2")
                    .bind(id.to_string())
                    .bind(v)
                    .execute(&self.pool)
                    .await
                    .map_err(Error::Database)?
                    .rows_affected()
            }
            None => sqlx::query("DELETE FROM workflow_definitions WHERE id = $1")
                .bind(id.to_string())
                .execute(&self.pool)
                .await
                .map_err(Error::Database)?
                .rows_affected(),
        };
        Ok(affected > 0)
    }

    async fn save_workflow_execution(&self, e: &WorkflowExecution) -> Result<()> {
        sqlx::query(
            "INSERT INTO workflow_executions \
             (workflow_id, workflow_definition_id, workflow_definition_version, status, input, output, search_attributes, metadata, \
              started_at, completed_at, updated_at, error_message, error_type, history_length, history_size_bytes, \
              parent_workflow_id, parent_step_id, current_step) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) \
             ON CONFLICT (workflow_id) DO UPDATE SET \
               workflow_definition_id = EXCLUDED.workflow_definition_id, \
               workflow_definition_version = EXCLUDED.workflow_definition_version, \
               status = EXCLUDED.status, \
               input = EXCLUDED.input, \
               output = EXCLUDED.output, \
               search_attributes = EXCLUDED.search_attributes, \
               metadata = EXCLUDED.metadata, \
               started_at = EXCLUDED.started_at, \
               completed_at = EXCLUDED.completed_at, \
               updated_at = EXCLUDED.updated_at, \
               error_message = EXCLUDED.error_message, \
               error_type = EXCLUDED.error_type, \
               history_length = EXCLUDED.history_length, \
               history_size_bytes = EXCLUDED.history_size_bytes, \
               parent_workflow_id = EXCLUDED.parent_workflow_id, \
               parent_step_id = EXCLUDED.parent_step_id, \
               current_step = EXCLUDED.current_step",
        )
        .bind(e.workflow_id.to_string())
        .bind(e.workflow_definition_id.to_string())
        .bind(&e.workflow_definition_version)
        .bind(enum_to_db(&e.status)?)
        .bind(&e.input)
        .bind(&e.output)
        .bind(&e.search_attributes)
        .bind(&e.metadata)
        .bind(e.started_at)
        .bind(e.completed_at)
        .bind(e.updated_at)
        .bind(&e.error_message)
        .bind(&e.error_type)
        .bind(e.history_length as i64)
        .bind(e.history_size_bytes as i64)
        .bind(e.parent_workflow_id.as_ref().map(|s| s.to_string()))
        .bind(e.parent_step_id.as_ref().map(|s| s.to_string()))
        .bind(e.current_step.as_ref().map(|s| s.to_string()))
        .execute(&self.pool)
        .await
        .map_err(Error::Database)?;
        Ok(())
    }

    async fn get_workflow_execution(
        &self,
        id: &WorkflowExecutionId,
    ) -> Result<Option<WorkflowExecution>> {
        let row = sqlx::query(
            "SELECT workflow_id, workflow_definition_id, workflow_definition_version, status, input, output, search_attributes, metadata, \
             started_at, completed_at, updated_at, error_message, error_type, history_length, history_size_bytes, \
             parent_workflow_id, parent_step_id, current_step \
             FROM workflow_executions WHERE workflow_id = $1",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(Error::Database)?;
        row.map(row_to_execution).transpose()
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
        let offset = page_token
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let status_strings: Vec<String> = statuses
            .iter()
            .flatten()
            .map(|s| enum_to_db(s).unwrap_or_default())
            .collect();

        let mut qb = sqlx::QueryBuilder::new(
            "SELECT workflow_id, workflow_definition_id, workflow_definition_version, status, input, output, search_attributes, metadata, \
             started_at, completed_at, updated_at, error_message, error_type, history_length, history_size_bytes, \
             parent_workflow_id, parent_step_id, current_step \
             FROM workflow_executions",
        );
        let mut where_open = false;
        let mut cond = |qb: &mut sqlx::QueryBuilder<sqlx::Postgres>, sql: &str| {
            if where_open {
                qb.push(" AND ");
            } else {
                qb.push(" WHERE ");
                where_open = true;
            }
            qb.push(sql);
        };

        if let Some(def) = workflow_definition_id {
            cond(&mut qb, " workflow_definition_id = ");
            qb.push_bind(def.to_string());
        }
        if !status_strings.is_empty() {
            cond(&mut qb, " status = ANY(");
            qb.push_bind(status_strings);
            qb.push(")");
        }
        if let Some(st) = start_time {
            cond(&mut qb, " started_at >= ");
            qb.push_bind(st);
        }
        if let Some(et) = end_time {
            cond(&mut qb, " started_at <= ");
            qb.push_bind(et);
        }
        qb.push(" ORDER BY started_at DESC LIMIT ");
        qb.push_bind((page_size + 1) as i64);
        qb.push(" OFFSET ");
        qb.push_bind(offset as i64);

        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(Error::Database)?;
        let next = if rows.len() > page_size {
            Some((offset + page_size).to_string())
        } else {
            None
        };
        let page = rows
            .into_iter()
            .take(page_size)
            .map(row_to_execution)
            .collect::<Result<Vec<_>>>()?;
        Ok((page, next))
    }

    async fn append_workflow_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        events: Vec<WorkflowEvent>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(Error::Database)?;
        let max_id: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(event_id), 0) FROM workflow_events WHERE workflow_id = $1",
        )
        .bind(workflow_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(Error::Database)?;

        let base = max_id + 1;
        for (i, ev) in events.into_iter().enumerate() {
            let event_id = base + i as i64;
            sqlx::query(
                "INSERT INTO workflow_events \
                 (workflow_id, event_id, event_type, timestamp, step_id, task_id, attributes) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(workflow_id.to_string())
            .bind(event_id)
            .bind(enum_to_db(&ev.event_type)?)
            .bind(ev.timestamp)
            .bind(ev.step_id.as_ref().map(|s| s.0.clone()))
            .bind(ev.task_id.as_ref().map(|s| s.0.clone()))
            .bind(&ev.attributes)
            .execute(&mut *tx)
            .await
            .map_err(Error::Database)?;
        }
        tx.commit().await.map_err(Error::Database)?;
        Ok(())
    }

    async fn get_workflow_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        after_event_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<WorkflowEvent>> {
        let rows = sqlx::query(
            "SELECT workflow_id, event_id, event_type, timestamp, step_id, task_id, attributes \
             FROM workflow_events WHERE workflow_id = $1 AND event_id > $2 ORDER BY event_id ASC LIMIT $3",
        )
        .bind(workflow_id.to_string())
        .bind(after_event_id.unwrap_or(0) as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(Error::Database)?;
        rows.into_iter().map(row_to_workflow_event).collect()
    }

    async fn get_workflow_event_count(&self, workflow_id: &WorkflowExecutionId) -> Result<u64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1")
                .bind(workflow_id.to_string())
                .fetch_one(&self.pool)
                .await
                .map_err(Error::Database)?;
        Ok(count as u64)
    }

    async fn save_task_state(&self, task_id: &str, state: Value) -> Result<()> {
        sqlx::query(
            "INSERT INTO task_states (task_id, state) VALUES ($1, $2) \
             ON CONFLICT (task_id) DO UPDATE SET state = EXCLUDED.state",
        )
        .bind(task_id)
        .bind(state)
        .execute(&self.pool)
        .await
        .map_err(Error::Database)?;
        Ok(())
    }

    async fn get_task_state(&self, task_id: &str) -> Result<Option<Value>> {
        let row = sqlx::query("SELECT state FROM task_states WHERE task_id = $1")
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(Error::Database)?;
        Ok(row.map(|r| r.try_get::<Value, _>("state")).transpose()?)
    }
}

/// Serialize an enum to its snake_case DB string (via serde).
#[cfg(feature = "postgres")]
fn enum_to_db<T: serde::Serialize>(v: &T) -> Result<String> {
    match serde_json::to_value(v)? {
        serde_json::Value::String(s) => Ok(s),
        other => Ok(other.to_string()),
    }
}

/// Deserialize an enum from its snake_case DB string (via serde).
#[cfg(feature = "postgres")]
fn db_to_enum<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(s.to_string())).map_err(Error::Serialization)
}

#[cfg(feature = "postgres")]
fn row_to_definition(row: sqlx::postgres::PgRow) -> Result<WorkflowDefinition> {
    let steps: serde_json::Value = row.try_get("steps")?;
    Ok(WorkflowDefinition {
        id: WorkflowDefinitionId::from_string(row.try_get::<String, _>("id")?),
        name: row.try_get("name")?,
        version: row.try_get("version")?,
        description: row.try_get("description")?,
        steps: serde_json::from_value(steps)?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        metadata: row.try_get("metadata")?,
        search_attributes: row.try_get("search_attributes")?,
    })
}

#[cfg(feature = "postgres")]
fn row_to_execution(row: sqlx::postgres::PgRow) -> Result<WorkflowExecution> {
    let status: String = row.try_get("status")?;
    Ok(WorkflowExecution {
        workflow_id: WorkflowExecutionId::from_string(row.try_get::<String, _>("workflow_id")?),
        workflow_definition_id: WorkflowDefinitionId::from_string(
            row.try_get::<String, _>("workflow_definition_id")?,
        ),
        workflow_definition_version: row.try_get("workflow_definition_version")?,
        status: db_to_enum(&status)?,
        input: row.try_get("input")?,
        output: row.try_get("output")?,
        search_attributes: row.try_get("search_attributes")?,
        metadata: row.try_get("metadata")?,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        updated_at: row.try_get("updated_at")?,
        error_message: row.try_get("error_message")?,
        error_type: row.try_get("error_type")?,
        history_length: row.try_get::<i64, _>("history_length")? as u64,
        history_size_bytes: row.try_get::<i64, _>("history_size_bytes")? as u64,
        parent_workflow_id: row
            .try_get::<Option<String>, _>("parent_workflow_id")?
            .map(WorkflowExecutionId::from_string),
        parent_step_id: row
            .try_get::<Option<String>, _>("parent_step_id")?
            .map(StepId::from_string),
        current_step: row
            .try_get::<Option<String>, _>("current_step")?
            .map(StepId::from_string),
    })
}

#[cfg(feature = "postgres")]
fn row_to_workflow_event(row: sqlx::postgres::PgRow) -> Result<WorkflowEvent> {
    let event_type: String = row.try_get("event_type")?;
    Ok(WorkflowEvent {
        event_id: row.try_get::<i64, _>("event_id")? as u64,
        event_type: db_to_enum(&event_type)?,
        timestamp: row.try_get("timestamp")?,
        step_id: row
            .try_get::<Option<String>, _>("step_id")?
            .map(StepId::from_string),
        task_id: row
            .try_get::<Option<String>, _>("task_id")?
            .map(TaskId::from_string),
        attributes: row.try_get("attributes")?,
    })
}
