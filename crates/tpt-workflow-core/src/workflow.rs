//! Workflow definitions and types

use crate::error::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a workflow definition
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowDefinitionId(pub String);

impl WorkflowDefinitionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for WorkflowDefinitionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorkflowDefinitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a workflow execution
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowExecutionId(pub String);

impl WorkflowExecutionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for WorkflowExecutionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorkflowExecutionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a step within a workflow
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StepId(pub String);

impl StepId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for StepId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for StepId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a task
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub String);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Workflow definition containing all steps and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub id: WorkflowDefinitionId,
    pub name: String,
    pub version: String,
    pub description: String,
    pub steps: Vec<StepDefinition>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub metadata: serde_json::Value,
    pub search_attributes: serde_json::Value,
}

impl WorkflowDefinition {
    pub fn new(name: String, version: String) -> Self {
        let now = Utc::now();
        Self {
            id: WorkflowDefinitionId::new(),
            name,
            version,
            description: String::new(),
            steps: Vec::new(),
            created_at: now,
            updated_at: now,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            search_attributes: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    pub fn with_description(mut self, description: String) -> Self {
        self.description = description;
        self
    }

    pub fn add_step(mut self, step: StepDefinition) -> Self {
        self.steps.push(step);
        self.updated_at = Utc::now();
        self
    }

    pub fn get_step(&self, step_id: &StepId) -> Option<&StepDefinition> {
        self.steps.iter().find(|s| &s.id == step_id)
    }

    pub fn validate(&self) -> Result<()> {
        if self.steps.is_empty() {
            return Err(Error::InvalidWorkflowDefinition(
                "Workflow must have at least one step".to_string(),
            ));
        }

        // Check for duplicate step IDs
        let mut seen = std::collections::HashSet::new();
        for step in &self.steps {
            if !seen.insert(&step.id) {
                return Err(Error::InvalidWorkflowDefinition(format!(
                    "Duplicate step ID: {}",
                    step.id
                )));
            }
        }

        // Validate step dependencies exist
        for step in &self.steps {
            for dep in &step.depends_on {
                if !seen.contains(dep) {
                    return Err(Error::InvalidWorkflowDefinition(format!(
                        "Step {} depends on non-existent step {}",
                        step.id, dep
                    )));
                }
            }
        }

        Ok(())
    }
}

/// Type of a workflow step
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepType {
    #[default]
    Task,
    Decision,
    Parallel,
    Sequence,
    Wait,
    Signal,
    ChildWorkflow,
    ContinueAsNew,
}

/// Definition of a single workflow step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDefinition {
    pub id: StepId,
    pub name: String,
    pub description: String,
    pub step_type: StepType,
    pub task_queue: String,
    pub task_type: String,
    pub input: serde_json::Value,
    pub output: serde_json::Value,
    pub timeout_policy: TimeoutPolicy,
    pub retry_policy: RetryPolicy,
    pub permission: PermissionRequirement,
    pub depends_on: Vec<StepId>,
    pub next_steps: Vec<StepId>,
    pub error_steps: Vec<StepId>,
    pub metadata: serde_json::Value,
}

impl StepDefinition {
    pub fn new(id: StepId, name: String, step_type: StepType) -> Self {
        Self {
            id,
            name,
            description: String::new(),
            step_type,
            task_queue: "default".to_string(),
            task_type: String::new(),
            input: serde_json::Value::Object(serde_json::Map::new()),
            output: serde_json::Value::Object(serde_json::Map::new()),
            timeout_policy: TimeoutPolicy::default(),
            retry_policy: RetryPolicy::default(),
            permission: PermissionRequirement::default(),
            depends_on: Vec::new(),
            next_steps: Vec::new(),
            error_steps: Vec::new(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    pub fn task(id: StepId, name: String, task_queue: String, task_type: String) -> Self {
        let mut step = Self::new(id, name, StepType::Task);
        step.task_queue = task_queue;
        step.task_type = task_type;
        step
    }

    pub fn depends_on(mut self, step_ids: Vec<StepId>) -> Self {
        self.depends_on = step_ids;
        self
    }

    pub fn next_on_success(mut self, step_ids: Vec<StepId>) -> Self {
        self.next_steps = step_ids;
        self
    }

    pub fn next_on_error(mut self, step_ids: Vec<StepId>) -> Self {
        self.error_steps = step_ids;
        self
    }
}

/// Timeout policy for a step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutPolicy {
    pub start_to_close: Option<chrono::Duration>,
    pub schedule_to_start: Option<chrono::Duration>,
    pub schedule_to_close: Option<chrono::Duration>,
    pub heartbeat: Option<chrono::Duration>,
}

impl Default for TimeoutPolicy {
    fn default() -> Self {
        Self {
            start_to_close: Some(chrono::Duration::hours(1)),
            schedule_to_start: Some(chrono::Duration::minutes(5)),
            schedule_to_close: Some(chrono::Duration::hours(2)),
            heartbeat: Some(chrono::Duration::seconds(30)),
        }
    }
}

/// Retry policy for a step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_interval: chrono::Duration,
    pub backoff_coefficient: f64,
    pub maximum_interval: chrono::Duration,
    pub non_retryable_errors: Vec<String>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_interval: chrono::Duration::seconds(1),
            backoff_coefficient: 2.0,
            maximum_interval: chrono::Duration::minutes(5),
            non_retryable_errors: Vec::new(),
        }
    }
}

/// Permission requirement for a step
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PermissionRequirement {
    pub required_role: Option<String>,
    pub required_attributes: Vec<String>,
    pub required_level: Option<i32>,
}

/// Status of a workflow execution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowExecutionStatus {
    #[default]
    Unspecified,
    Running,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
    Terminated,
    ContinuedAsNew,
}

/// Workflow execution instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowExecution {
    pub workflow_id: WorkflowExecutionId,
    pub workflow_definition_id: WorkflowDefinitionId,
    pub workflow_definition_version: String,
    pub status: WorkflowExecutionStatus,
    pub input: serde_json::Value,
    pub output: serde_json::Value,
    pub search_attributes: serde_json::Value,
    pub metadata: serde_json::Value,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub error_message: Option<String>,
    pub error_type: Option<String>,
    pub history_length: u64,
    pub history_size_bytes: u64,
    pub parent_workflow_id: Option<WorkflowExecutionId>,
    pub parent_step_id: Option<StepId>,
    pub current_step: Option<StepId>,
}

impl WorkflowExecution {
    pub fn new(
        workflow_id: WorkflowExecutionId,
        workflow_definition_id: WorkflowDefinitionId,
        workflow_definition_version: String,
        input: serde_json::Value,
    ) -> Self {
        let now = Utc::now();
        Self {
            workflow_id,
            workflow_definition_id,
            workflow_definition_version,
            status: WorkflowExecutionStatus::Running,
            input,
            output: serde_json::Value::Object(serde_json::Map::new()),
            search_attributes: serde_json::Value::Object(serde_json::Map::new()),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            started_at: now,
            completed_at: None,
            updated_at: now,
            error_message: None,
            error_type: None,
            history_length: 0,
            history_size_bytes: 0,
            parent_workflow_id: None,
            parent_step_id: None,
            current_step: None,
        }
    }

    pub fn complete(&mut self, output: serde_json::Value) {
        self.status = WorkflowExecutionStatus::Completed;
        self.output = output;
        self.completed_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    pub fn fail(&mut self, error_message: String, error_type: String) {
        self.status = WorkflowExecutionStatus::Failed;
        self.error_message = Some(error_message);
        self.error_type = Some(error_type);
        self.completed_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    pub fn cancel(&mut self, reason: String) {
        self.status = WorkflowExecutionStatus::Cancelled;
        self.error_message = Some(reason);
        self.completed_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            WorkflowExecutionStatus::Completed
                | WorkflowExecutionStatus::Failed
                | WorkflowExecutionStatus::TimedOut
                | WorkflowExecutionStatus::Cancelled
                | WorkflowExecutionStatus::Terminated
                | WorkflowExecutionStatus::ContinuedAsNew
        )
    }
}

/// Type of workflow event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowEventType {
    Unspecified,
    WorkflowStarted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowTimedOut,
    WorkflowCancelled,
    WorkflowTerminated,
    WorkflowContinuedAsNew,
    StepStarted,
    StepCompleted,
    StepFailed,
    StepTimedOut,
    StepCancelled,
    TaskScheduled,
    TaskStarted,
    TaskCompleted,
    TaskFailed,
    TaskTimedOut,
    TaskCancelled,
    SignalReceived,
    TimerFired,
    ChildWorkflowStarted,
    ChildWorkflowCompleted,
    ChildWorkflowFailed,
}

/// Event in workflow execution history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowEvent {
    pub event_id: u64,
    pub event_type: WorkflowEventType,
    pub timestamp: DateTime<Utc>,
    pub step_id: Option<StepId>,
    pub task_id: Option<TaskId>,
    pub attributes: serde_json::Value,
}

impl WorkflowEvent {
    pub fn new(event_type: WorkflowEventType, attributes: serde_json::Value) -> Self {
        Self {
            event_id: 0, // Will be set by history store
            event_type,
            timestamp: Utc::now(),
            step_id: None,
            task_id: None,
            attributes,
        }
    }
}
