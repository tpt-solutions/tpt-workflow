//! Task-queue naming convention for tpt-workflow.
//!
//! Every dispatched task lives on a named queue. tpt-workflow uses a single
//! canonical scheme so that engine, workers, and the (future) synapse-backed
//! dispatch transport all agree on queue identity:
//!
//! ```text
//! <scope>/<logical-queue>
//! ```
//!
//! where `<scope>` is one of:
//!
//! * `exec:<workflow-execution-id>` — a queue scoped to a single workflow run
//!   (isolated: tasks from one run never land on another run's queue).
//! * `def:<workflow-definition-id>:<version>` — a queue shared by every
//!   execution of a given definition + version (useful for global pools such
//!   as "send all outbound emails through one rate-limited queue").
//!
//! `<logical-queue>` is a short, human-readable name such as `default`,
//! `email-send`, or `http-call`.
//!
//! The slash is the only structural separator; scope-id, version, and
//! logical-queue segments therefore must not contain `/`, `:`, or whitespace.

use crate::error::{Error, Result};
use crate::workflow::{WorkflowDefinitionId, WorkflowExecutionId};
use std::fmt;

/// Maximum length of a canonical queue name (bytes).
pub const MAX_QUEUE_NAME_LEN: usize = 255;

/// Allowed characters inside an id, version, or logical-queue segment.
fn is_segment_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}

fn validate_segment(label: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidInput(format!("{} must not be empty", label)));
    }
    if value.len() > MAX_QUEUE_NAME_LEN {
        return Err(Error::InvalidInput(format!(
            "{} too long ({} > {})",
            label,
            value.len(),
            MAX_QUEUE_NAME_LEN
        )));
    }
    if !value.chars().all(is_segment_char) {
        return Err(Error::InvalidInput(format!(
            "{} contains illegal characters: {:?}",
            label, value
        )));
    }
    Ok(())
}

/// The workflow scope a queue is bound to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QueueScope {
    /// Queue belongs to a single workflow execution run.
    Execution(WorkflowExecutionId),
    /// Queue is shared by all executions of a definition + version.
    Definition(WorkflowDefinitionId, String),
}

impl QueueScope {
    /// The canonical `<scope>` prefix (without the trailing `/`).
    pub fn scope_str(&self) -> String {
        match self {
            QueueScope::Execution(id) => format!("exec:{}", id.0),
            QueueScope::Definition(def_id, version) => {
                format!("def:{}:{}", def_id.0, version)
            }
        }
    }

    /// The workflow execution id this scope resolves to, if execution-scoped.
    pub fn execution_id(&self) -> Option<&WorkflowExecutionId> {
        match self {
            QueueScope::Execution(id) => Some(id),
            QueueScope::Definition(_, _) => None,
        }
    }

    /// The workflow definition id + version, if definition-scoped.
    pub fn definition(&self) -> Option<(&WorkflowDefinitionId, &str)> {
        match self {
            QueueScope::Definition(def_id, version) => Some((def_id, version)),
            QueueScope::Execution(_) => None,
        }
    }
}

impl fmt::Display for QueueScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.scope_str())
    }
}

/// A fully-qualified, canonical task-queue name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskQueueName {
    scope: QueueScope,
    queue: String,
}

impl TaskQueueName {
    /// Build an execution-scoped queue name.
    pub fn for_execution(execution_id: WorkflowExecutionId, queue: &str) -> Result<Self> {
        validate_segment("logical-queue", queue)?;
        Ok(TaskQueueName {
            scope: QueueScope::Execution(execution_id),
            queue: queue.to_string(),
        })
    }

    /// Build a definition-scoped (shared) queue name.
    pub fn for_definition(
        def_id: WorkflowDefinitionId,
        version: &str,
        queue: &str,
    ) -> Result<Self> {
        validate_segment("version", version)?;
        validate_segment("logical-queue", queue)?;
        Ok(TaskQueueName {
            scope: QueueScope::Definition(def_id, version.to_string()),
            queue: queue.to_string(),
        })
    }

    /// Build a queue name from an already-constructed scope.
    pub fn with_scope(scope: QueueScope, queue: &str) -> Result<Self> {
        validate_segment("logical-queue", queue)?;
        Ok(TaskQueueName {
            scope,
            queue: queue.to_string(),
        })
    }

    /// Parse a canonical queue name produced by [`TaskQueueName::to_string`].
    pub fn parse(s: &str) -> Result<Self> {
        if s.len() > MAX_QUEUE_NAME_LEN {
            return Err(Error::InvalidInput(format!(
                "queue name too long ({} > {})",
                s.len(),
                MAX_QUEUE_NAME_LEN
            )));
        }

        let (scope_part, queue_part) = s.split_once('/').ok_or_else(|| {
            Error::InvalidInput(format!("queue name missing '/' separator: {:?}", s))
        })?;

        validate_segment("logical-queue", queue_part)?;

        let scope = if let Some(rest) = scope_part.strip_prefix("exec:") {
            validate_segment("execution-id", rest)?;
            QueueScope::Execution(WorkflowExecutionId::from_string(rest.to_string()))
        } else if let Some(rest) = scope_part.strip_prefix("def:") {
            let (def_id, version) = rest.split_once(':').ok_or_else(|| {
                Error::InvalidInput(format!("definition scope missing version: {:?}", s))
            })?;
            validate_segment("definition-id", def_id)?;
            validate_segment("version", version)?;
            QueueScope::Definition(
                WorkflowDefinitionId::from_string(def_id.to_string()),
                version.to_string(),
            )
        } else {
            return Err(Error::InvalidInput(format!(
                "unknown scope prefix in queue name: {:?}",
                s
            )));
        };

        Ok(TaskQueueName {
            scope,
            queue: queue_part.to_string(),
        })
    }

    /// The workflow scope this queue is bound to.
    pub fn scope(&self) -> &QueueScope {
        &self.scope
    }

    /// The logical queue name (the part after the `/`).
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// The canonical `<scope>/<logical-queue>` string.
    pub fn canonical(&self) -> String {
        format!("{}/{}", self.scope.scope_str(), self.queue)
    }
}

impl fmt::Display for TaskQueueName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.canonical())
    }
}

impl std::str::FromStr for TaskQueueName {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        TaskQueueName::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exec_id() -> WorkflowExecutionId {
        WorkflowExecutionId::from_string("run-1234".to_string())
    }
    fn def_id() -> WorkflowDefinitionId {
        WorkflowDefinitionId::from_string("wf-9".to_string())
    }

    #[test]
    fn execution_scoped_round_trips() {
        let name = TaskQueueName::for_execution(exec_id(), "default").unwrap();
        assert_eq!(name.canonical(), "exec:run-1234/default");
        assert_eq!(name.queue(), "default");
        assert_eq!(name.scope().execution_id(), Some(&exec_id()));

        let parsed = TaskQueueName::parse(&name.canonical()).unwrap();
        assert_eq!(parsed, name);
    }

    #[test]
    fn definition_scoped_round_trips() {
        let name = TaskQueueName::for_definition(def_id(), "1.2.3", "email-send").unwrap();
        assert_eq!(name.canonical(), "def:wf-9:1.2.3/email-send");
        assert_eq!(name.scope().definition(), Some((&def_id(), "1.2.3")));

        let parsed = TaskQueueName::parse(&name.canonical()).unwrap();
        assert_eq!(parsed, name);
    }

    #[test]
    fn rejects_illegal_segments() {
        // empty logical queue
        assert!(TaskQueueName::for_execution(exec_id(), "").is_err());
        // slash in logical queue
        assert!(TaskQueueName::for_execution(exec_id(), "a/b").is_err());
        // colon in logical queue
        assert!(TaskQueueName::for_execution(exec_id(), "a:b").is_err());
        // whitespace
        assert!(TaskQueueName::for_execution(exec_id(), "a b").is_err());
        // bad version
        assert!(TaskQueueName::for_definition(def_id(), "", "q").is_err());
        assert!(TaskQueueName::for_definition(def_id(), "1.0", "a/b").is_err());
    }

    #[test]
    fn rejects_malformed_names() {
        // missing separator
        assert!(TaskQueueName::parse("exec:run-1").is_err());
        // unknown scope prefix
        assert!(TaskQueueName::parse("xyz:run-1/default").is_err());
        // definition scope missing version
        assert!(TaskQueueName::parse("def:wf-9/default").is_err());
        // empty queue
        assert!(TaskQueueName::parse("exec:run-1/").is_err());
        // illegal id char
        assert!(TaskQueueName::parse("exec:run 1/default").is_err());
    }
}
