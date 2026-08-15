//! Synapse-backed task-queue transport (Phase 5 integration).
//!
//! This module implements the engine's [`TaskQueue`] trait against the vendored
//! tpt-synapse `MatchingService` gRPC facade — the queue+ack primitive that
//! Phase 5 dispatches through. Tasks are enqueued via `AddTask`, pulled by
//! workers via `PollTask` (batch dequeue) or `StreamTasks` (continuous
//! streaming), and completed/failed idempotently via `RespondTaskCompleted` /
//! `RespondTaskFailed`.
//!
//! The wire contract is vendored from `tpt-synapse/tpt-workflow/proto/
//! workflow.proto`; that cross-repo crate owns the **server** side. tpt-workflow
//! only consumes it as a gRPC **client** (per the Phase 5 errata), so this
//! module never reimplements the matching-service dispatch semantics.
//!
//! Two deliverables live here:
//! * [`SynapseTaskQueue`] — a `TaskQueue` over the synapse primitive.
//! * [`SynapseTaskCompletionListener`] — async completion listening with a
//!   consumer group per task queue (see [`ConsumerGroup`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

use crate::error::{Error, Result};
use crate::task_queue::{QueuedTask, TaskQueue, TaskQueueStats, TaskStatus};

pub use tpt_workflow_proto::synapse::workflow::v1::{
    matching_service_client::MatchingServiceClient, AddTaskRequest, AddTaskResponse,
    PollTaskRequest, PollTaskResponse, RespondRequest, RespondResponse, StreamTasksRequest, Task,
};

/// Default visibility timeout for tasks dispatched onto a synapse queue.
pub const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);
/// Default long-poll ceiling used by [`SynapseTaskQueue::dequeue`].
pub const DEFAULT_LONG_POLL_TIMEOUT: Duration = Duration::from_millis(200);

/// Connection + routing config for a synapse-backed task queue.
#[derive(Debug, Clone)]
pub struct SynapseTaskQueueConfig {
    /// gRPC endpoint of the tpt-synapse `MatchingService` (e.g.
    /// `http://127.0.0.1:8080`).
    pub endpoint: String,
    /// Synapse tenant the queue is scoped to.
    pub tenant: String,
    /// The synapse task queue name (the `task_queue` field of the wire API).
    pub task_queue: String,
    /// Optional per-type sub-queue routing (`task_type`).
    pub task_type: Option<String>,
    /// Visibility timeout handed to `AddTask`; an unacked task is redelivered
    /// after this window elapses (worker crash / stall).
    pub visibility_timeout: Duration,
    /// Long-poll ceiling used when pulling a single batch in `dequeue`.
    pub long_poll_timeout: Duration,
}

impl Default for SynapseTaskQueueConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8080".to_string(),
            tenant: "tpt-workflow".to_string(),
            task_queue: "default".to_string(),
            task_type: None,
            visibility_timeout: DEFAULT_VISIBILITY_TIMEOUT,
            long_poll_timeout: DEFAULT_LONG_POLL_TIMEOUT,
        }
    }
}

fn encode_task(task: &QueuedTask) -> Result<Vec<u8>> {
    serde_json::to_vec(task).map_err(Error::from)
}

/// An in-flight task: the decoded [`QueuedTask`] plus the opaque **delivery**
/// token returned by synapse, which is the handle passed back to
/// `RespondTaskCompleted` / `RespondTaskFailed`.
type InflightEntry = (QueuedTask, Vec<u8>);

fn decode_task(payload: &[u8]) -> Result<QueuedTask> {
    serde_json::from_slice(payload).map_err(Error::from)
}

/// A [`TaskQueue`] implementation backed by the tpt-synapse queue+ack primitive.
///
/// The synapse transport owns the durable FIFO + visibility/redelivery/ack
/// semantics. This struct adds a thin local registry so the engine can still
/// inspect in-flight tasks (`get_task`, `start_task`, `get_stats`); the
/// authoritative pending depth lives in synapse, which this client does not
/// query, so `get_stats().pending_tasks` is reported as `0` (unknown).
pub struct SynapseTaskQueue {
    name: String,
    config: SynapseTaskQueueConfig,
    client: MatchingServiceClient<Channel>,
    /// Local registry of in-flight tasks, keyed by the **business** task token
    /// (the `QueuedTask.task_token` we assigned). The value carries the decoded
    /// task plus the opaque **delivery** token returned by synapse, which is the
    /// handle that must be passed back to `RespondTaskCompleted` /
    /// `RespondTaskFailed`.
    inflight: Arc<RwLock<HashMap<String, InflightEntry>>>,
    completed: AtomicU64,
    failed: AtomicU64,
}

impl SynapseTaskQueue {
    /// Connect to the `MatchingService` at `config.endpoint` (lazily; the
    /// channel is established on first RPC).
    pub fn connect(config: SynapseTaskQueueConfig) -> Result<Self> {
        let channel = Endpoint::from_shared(config.endpoint.clone())
            .map_err(|e| Error::Transport(Box::new(e)))?
            .connect_lazy();
        Ok(Self {
            name: config.task_queue.clone(),
            config,
            client: MatchingServiceClient::new(channel),
            inflight: Arc::new(RwLock::new(HashMap::new())),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        })
    }

    /// The canonical queue name this transport is bound to.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Number of tasks currently in-flight (dispatched, not yet acked) as known
    /// to this client's local registry.
    pub async fn inflight_count(&self) -> usize {
        self.inflight.read().await.len()
    }
}

#[async_trait]
impl TaskQueue for SynapseTaskQueue {
    async fn enqueue(&self, task: QueuedTask) -> Result<()> {
        let payload = encode_task(&task)?;
        let req = AddTaskRequest {
            tenant: self.config.tenant.clone(),
            task_queue: self.config.task_queue.clone(),
            task_type: self.config.task_type.clone().unwrap_or_default(),
            payload,
            visibility_timeout_ms: self.config.visibility_timeout.as_millis() as i64,
        };
        self.client
            .clone()
            .add_task(req)
            .await
            .map_err(Error::from)?;
        debug!(queue = %self.name, task = %task.task_token, "enqueued task onto synapse queue");
        Ok(())
    }

    async fn dequeue(&self, worker_id: &str, max_tasks: usize) -> Result<Vec<QueuedTask>> {
        let mut out = Vec::with_capacity(max_tasks);
        for _ in 0..max_tasks {
            let req = PollTaskRequest {
                tenant: self.config.tenant.clone(),
                task_queue: self.config.task_queue.clone(),
                worker_id: worker_id.to_string(),
                task_type: self.config.task_type.clone().unwrap_or_default(),
                long_poll_timeout_ms: self.config.long_poll_timeout.as_millis() as i64,
            };
            let task = self
                .client
                .clone()
                .poll_task(req)
                .await
                .map_err(Error::from)?
                .into_inner()
                .task;
            match task {
                Some(t) => {
                    let task = decode_task(&t.payload)?;
                    self.inflight.write().await.insert(
                        task.task_token.clone(),
                        (task.clone(), t.task_token.clone()),
                    );
                    out.push(task);
                }
                None => break,
            }
        }
        Ok(out)
    }

    async fn start_task(&self, task_token: &str, _worker_id: &str) -> Result<()> {
        let mut inflight = self.inflight.write().await;
        match inflight.get_mut(task_token) {
            Some((t, _)) => {
                t.status = TaskStatus::Running;
                t.started_at = Some(Utc::now());
                Ok(())
            }
            None => Err(Error::TaskNotFound(task_token.to_string())),
        }
    }

    async fn complete_task(&self, task_token: &str, output: Value) -> Result<()> {
        let delivery = {
            let inflight = self.inflight.read().await;
            match inflight.get(task_token) {
                Some((_, d)) => d.clone(),
                None => return Err(Error::TaskNotFound(task_token.to_string())),
            }
        };
        let req = RespondRequest {
            task_token: delivery,
            result_payload: serde_json::to_vec(&output).unwrap_or_default(),
            failure_message: String::new(),
            failure_stack: String::new(),
        };
        let resp = self
            .client
            .clone()
            .respond_task_completed(req)
            .await
            .map_err(Error::from)?
            .into_inner();
        if !resp.accepted {
            return Err(Error::TaskNotFound(task_token.to_string()));
        }
        self.inflight.write().await.remove(task_token);
        self.completed.fetch_add(1, Ordering::SeqCst);
        debug!(queue = %self.name, task = %task_token, "completed task on synapse queue");
        Ok(())
    }

    async fn fail_task(&self, task_token: &str, error: String) -> Result<()> {
        let delivery = {
            let inflight = self.inflight.read().await;
            match inflight.get(task_token) {
                Some((_, d)) => d.clone(),
                None => return Err(Error::TaskNotFound(task_token.to_string())),
            }
        };
        let req = RespondRequest {
            task_token: delivery,
            result_payload: Vec::new(),
            failure_message: error,
            failure_stack: String::new(),
        };
        let resp = self
            .client
            .clone()
            .respond_task_failed(req)
            .await
            .map_err(Error::from)?
            .into_inner();
        if !resp.accepted {
            return Err(Error::TaskNotFound(task_token.to_string()));
        }
        self.inflight.write().await.remove(task_token);
        self.failed.fetch_add(1, Ordering::SeqCst);
        debug!(queue = %self.name, task = %task_token, "failed task on synapse queue");
        Ok(())
    }

    async fn cancel_task(&self, task_token: &str, _reason: String) -> Result<()> {
        // The synapse queue+ack primitive has no hard-delete: a failed task is
        // redelivered for retry, not cancelled. Cancellation is therefore not
        // expressible through this transport; surface it honestly rather than
        // silently redelivering the task.
        Err(Error::NotImplemented(format!(
            "cancel_task is not supported by the synapse queue+ack transport (task_token={})",
            task_token
        )))
    }

    async fn get_task(&self, task_token: &str) -> Result<Option<QueuedTask>> {
        Ok(self
            .inflight
            .read()
            .await
            .get(task_token)
            .map(|(t, _)| t.clone()))
    }

    async fn get_stats(&self) -> Result<TaskQueueStats> {
        let inflight = self.inflight.read().await;
        Ok(TaskQueueStats {
            name: self.name.clone(),
            pending_tasks: 0,
            running_tasks: inflight.len() as u64,
            completed_tasks: self.completed.load(Ordering::SeqCst),
            failed_tasks: self.failed.load(Ordering::SeqCst),
            updated_at: Utc::now(),
        })
    }

    async fn heartbeat_task(&self, task_token: &str, _progress: Value) -> Result<bool> {
        Ok(self.inflight.read().await.contains_key(task_token))
    }
}

/// A named group of cooperative consumers for a single task queue.
///
/// Synapse delivers each task to exactly one polling consumer, so every worker
/// that streams the same `(tenant, task_queue, task_type)` collectively forms a
/// consumer group: work is spread across them, and a crashed member's in-flight
/// tasks are redelivered after their visibility timeout. The `group` name is
/// the shared identity used for bookkeeping and routing (see
/// [`ConsumerGroupRegistry`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerGroup {
    pub tenant: String,
    pub task_queue: String,
    pub task_type: String,
    /// Logical consumer-group name (e.g. a worker pool or deployment id).
    pub group: String,
}

/// Process-local registry of consumer-group memberships.
///
/// Tracks which listener member-ids are active in each group; useful for
/// visibility and coordinated shutdown. Group membership does not change
/// synapse's delivery behaviour (that is inherent to its single-delivery
/// queue) — this is purely bookkeeping on the tpt-workflow side.
#[derive(Default, Clone)]
pub struct ConsumerGroupRegistry {
    groups: Arc<RwLock<HashMap<String, HashMap<String, ()>>>>,
}

impl ConsumerGroupRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `member_id` as an active member of `group`.
    pub async fn join(&self, group: &str, member_id: &str) {
        self.groups
            .write()
            .await
            .entry(group.to_string())
            .or_default()
            .insert(member_id.to_string(), ());
    }

    /// Remove `member_id` from `group`.
    pub async fn leave(&self, group: &str, member_id: &str) {
        let mut groups = self.groups.write().await;
        if let Some(members) = groups.get_mut(group) {
            members.remove(member_id);
        }
    }

    /// Active member ids for `group`.
    pub async fn members(&self, group: &str) -> Vec<String> {
        self.groups
            .read()
            .await
            .get(group)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Number of active members in `group`.
    pub async fn member_count(&self, group: &str) -> usize {
        self.groups
            .read()
            .await
            .get(group)
            .map(|m| m.len())
            .unwrap_or(0)
    }
}

/// Handler invoked for each task delivered to a [`SynapseTaskCompletionListener`].
///
/// Return `Ok(output)` to ack (complete) the task, or `Err(message)` to nack
/// (fail/redeliver) it.
#[async_trait]
pub trait TaskHandler: Send + Sync {
    async fn handle(&self, task: QueuedTask) -> std::result::Result<Value, String>;
}

/// Async completion listener that consumes a synapse task queue as a member of a
/// [`ConsumerGroup`].
///
/// `start` opens a `StreamTasks` stream and processes deliveries continuously,
/// acking each via the handler's result. The loop re-opens the stream on error
/// (with backoff) until [`stop`](Self::stop) is called. Multiple listeners
/// sharing the same `ConsumerGroup` spread the work across themselves via
/// synapse's single-delivery semantics.
pub struct SynapseTaskCompletionListener {
    client: MatchingServiceClient<Channel>,
    group: ConsumerGroup,
    visibility_timeout: Duration,
    handler: Arc<dyn TaskHandler>,
    registry: Option<ConsumerGroupRegistry>,
    member_id: String,
    running: Arc<AtomicBool>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl SynapseTaskCompletionListener {
    /// Build a listener bound to `group`, consuming from the `MatchingService`
    /// at `endpoint` and dispatching to `handler`.
    pub fn connect(
        group: ConsumerGroup,
        endpoint: &str,
        handler: Arc<dyn TaskHandler>,
        visibility_timeout: Duration,
    ) -> Result<Self> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| Error::Transport(Box::new(e)))?
            .connect_lazy();
        Ok(Self {
            client: MatchingServiceClient::new(channel),
            group,
            visibility_timeout,
            handler,
            registry: None,
            member_id: uuid::Uuid::new_v4().to_string(),
            running: Arc::new(AtomicBool::new(false)),
            handle: Arc::new(Mutex::new(None)),
        })
    }

    /// Attach a [`ConsumerGroupRegistry`] so this listener records its
    /// membership while running.
    pub fn with_registry(mut self, registry: ConsumerGroupRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Whether the listener loop is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Start the streaming consume loop in the background.
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(()); // already running
        }
        if let Some(reg) = &self.registry {
            reg.join(&self.group.group, &self.member_id).await;
        }
        let mut client = self.client.clone();
        let group = self.group.clone();
        let visibility = self.visibility_timeout;
        let handler = self.handler.clone();
        let running = self.running.clone();
        let member_id = self.member_id.clone();
        let registry = self.registry.clone();

        let handle = tokio::spawn(async move {
            loop {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                if let Err(e) = stream_consume(
                    &mut client,
                    &group,
                    visibility,
                    handler.clone(),
                    running.clone(),
                )
                .await
                {
                    error!(group = %group.group, error = %e, "synapse completion stream errored");
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                if !running.load(Ordering::SeqCst) {
                    break;
                }
            }
            if let Some(reg) = &registry {
                reg.leave(&group.group, &member_id).await;
            }
        });
        *self.handle.lock().await = Some(handle);
        info!(group = %self.group.group, queue = %self.group.task_queue, "started synapse completion listener");
        Ok(())
    }

    /// Stop the consume loop and await its termination.
    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(h) = self.handle.lock().await.take() {
            let _ = h.await;
        }
        if let Some(reg) = &self.registry {
            reg.leave(&self.group.group, &self.member_id).await;
        }
    }
}

/// Open a `StreamTasks` stream and process deliveries until the stream ends or
/// `running` is cleared. Each task is decoded and handed to `handler`; the
/// result determines whether we ack (complete) or nack (fail).
async fn stream_consume(
    client: &mut MatchingServiceClient<Channel>,
    group: &ConsumerGroup,
    visibility: Duration,
    handler: Arc<dyn TaskHandler>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let req = StreamTasksRequest {
        tenant: group.tenant.clone(),
        task_queue: group.task_queue.clone(),
        worker_id: format!("cg:{}", group.group),
        task_type: group.task_type.clone(),
        visibility_timeout_ms: visibility.as_millis() as i64,
    };
    let mut stream = client
        .stream_tasks(req)
        .await
        .map_err(Error::from)?
        .into_inner();

    // Bound each read so the loop can re-check `running` and exit promptly on
    // `stop` even though the server keeps the stream open (long-poll style).
    const IDLE_TIMEOUT: Duration = Duration::from_secs(1);
    while running.load(Ordering::SeqCst) {
        let task = match tokio::time::timeout(IDLE_TIMEOUT, stream.message()).await {
            Ok(Ok(Some(t))) => t,
            Ok(Ok(None)) => return Ok(()), // stream closed by the server
            Ok(Err(e)) => return Err(Error::from(e)),
            Err(_) => continue, // idle timeout — re-check `running` and loop
        };

        let token = task.task_token.clone();
        let result = match decode_task(&task.payload) {
            Ok(qt) => handler.handle(qt).await,
            Err(e) => Err(format!("failed to decode task payload: {e}")),
        };

        let respond = RespondRequest {
            task_token: token.clone(),
            result_payload: match &result {
                Ok(v) => serde_json::to_vec(v).unwrap_or_default(),
                Err(_) => Vec::new(),
            },
            failure_message: match &result {
                Err(m) => m.clone(),
                Ok(_) => String::new(),
            },
            failure_stack: String::new(),
        };

        let resp = if result.is_ok() {
            client.respond_task_completed(respond).await
        } else {
            client.respond_task_failed(respond).await
        };

        match resp {
            Ok(inner) => {
                if !inner.into_inner().accepted {
                    warn!(task = ?token, "synapse respond reported unknown token");
                }
            }
            Err(e) => warn!(task = ?token, error = %e, "synapse respond failed"),
        }
    }
    Ok(())
}
