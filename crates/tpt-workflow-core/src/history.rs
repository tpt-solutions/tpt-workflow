//! Workflow execution history store

#[cfg(feature = "postgres")]
use sqlx::Row;

#[cfg(feature = "postgres")]
use crate::error::Error;
use crate::error::Result;
use crate::workflow::{StepId, TaskId, WorkflowEventType, WorkflowExecutionId};
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
    async fn append_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        events: Vec<HistoryEvent>,
    ) -> Result<()>;

    /// Get events after a specific event ID (for replay)
    async fn get_events_after(
        &self,
        workflow_id: &WorkflowExecutionId,
        after_event_id: u64,
    ) -> Result<Vec<HistoryEvent>>;

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
    async fn append_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        events: Vec<HistoryEvent>,
    ) -> Result<()> {
        let mut events_map = self.events.write().await;
        let workflow_events = events_map
            .entry(workflow_id.clone())
            .or_insert_with(Vec::new);
        let count = events.len();

        for mut event in events {
            event.event_id = workflow_events.len() as u64 + 1;
            workflow_events.push(event);
        }

        debug!("Appended {} events to workflow {}", count, workflow_id);
        Ok(())
    }

    async fn get_events_after(
        &self,
        workflow_id: &WorkflowExecutionId,
        after_event_id: u64,
    ) -> Result<Vec<HistoryEvent>> {
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
        Ok(workflow_events.and_then(|e| e.last().map(|evt| evt.event_id)))
    }

    async fn get_event_count(&self, workflow_id: &WorkflowExecutionId) -> Result<u64> {
        let events_map = self.events.read().await;
        Ok(events_map
            .get(workflow_id)
            .map(|e| e.len() as u64)
            .unwrap_or(0))
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

/// Postgres-backed history store (Phase 4: tpt-keystone-db integration).
///
/// Durably appends workflow history events to the same `workflow_events` table
/// used by [`crate::persistence::PostgresPersistence`]. Schema/migrations are
/// shared; run [`PostgresHistoryStore::migrate`] or [`PostgresHistoryStore::connect`].
#[cfg(feature = "postgres")]
pub struct PostgresHistoryStore {
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresHistoryStore {
    /// Create from an existing pool (migrations assumed applied).
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

    /// Run embedded SQL migrations.
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
impl HistoryStore for PostgresHistoryStore {
    async fn append_events(
        &self,
        workflow_id: &WorkflowExecutionId,
        events: Vec<HistoryEvent>,
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

    async fn get_events_after(
        &self,
        workflow_id: &WorkflowExecutionId,
        after_event_id: u64,
    ) -> Result<Vec<HistoryEvent>> {
        let rows = sqlx::query(
            "SELECT workflow_id, event_id, event_type, timestamp, step_id, task_id, attributes \
             FROM workflow_events WHERE workflow_id = $1 AND event_id > $2 ORDER BY event_id ASC",
        )
        .bind(workflow_id.to_string())
        .bind(after_event_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(Error::Database)?;
        rows.into_iter().map(row_to_history_event).collect()
    }

    async fn get_all_events(&self, workflow_id: &WorkflowExecutionId) -> Result<Vec<HistoryEvent>> {
        let rows = sqlx::query(
            "SELECT workflow_id, event_id, event_type, timestamp, step_id, task_id, attributes \
             FROM workflow_events WHERE workflow_id = $1 ORDER BY event_id ASC",
        )
        .bind(workflow_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(Error::Database)?;
        rows.into_iter().map(row_to_history_event).collect()
    }

    async fn get_last_event_id(&self, workflow_id: &WorkflowExecutionId) -> Result<Option<u64>> {
        let max_id: Option<i64> =
            sqlx::query_scalar("SELECT MAX(event_id) FROM workflow_events WHERE workflow_id = $1")
                .bind(workflow_id.to_string())
                .fetch_one(&self.pool)
                .await
                .map_err(Error::Database)?;
        Ok(max_id.map(|v| v as u64))
    }

    async fn get_event_count(&self, workflow_id: &WorkflowExecutionId) -> Result<u64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1")
                .bind(workflow_id.to_string())
                .fetch_one(&self.pool)
                .await
                .map_err(Error::Database)?;
        Ok(count as u64)
    }

    async fn delete_history(&self, workflow_id: &WorkflowExecutionId) -> Result<()> {
        sqlx::query("DELETE FROM workflow_events WHERE workflow_id = $1")
            .bind(workflow_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(Error::Database)?;
        Ok(())
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
fn row_to_history_event(row: sqlx::postgres::PgRow) -> Result<HistoryEvent> {
    let event_type: String = row.try_get("event_type")?;
    Ok(HistoryEvent {
        event_id: row.try_get::<i64, _>("event_id")? as u64,
        workflow_id: WorkflowExecutionId::from_string(row.try_get::<String, _>("workflow_id")?),
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
