//! Error types for tpt-workflow

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Workflow not found: {0}")]
    WorkflowNotFound(String),

    #[error("Workflow definition not found: {0}")]
    WorkflowDefinitionNotFound(String),

    #[error("Workflow execution not found: {0}")]
    WorkflowExecutionNotFound(String),

    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("Task queue not found: {0}")]
    TaskQueueNotFound(String),

    #[error("Worker not found: {0}")]
    WorkerNotFound(String),

    #[error("Worker already registered: {0}")]
    WorkerAlreadyRegistered(String),

    #[error("Invalid workflow definition: {0}")]
    InvalidWorkflowDefinition(String),

    #[error("Invalid workflow state transition: {0}")]
    InvalidStateTransition(String),

    #[error("Workflow execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Workflow execution timed out: {0}")]
    ExecutionTimedOut(String),

    #[error("Workflow execution cancelled: {0}")]
    ExecutionCancelled(String),

    #[error("Workflow execution terminated: {0}")]
    ExecutionTerminated(String),

    #[error("Task execution failed: {0}")]
    TaskFailed(String),

    #[error("Task execution timed out: {0}")]
    TaskTimedOut(String),

    #[error("Task cancelled: {0}")]
    TaskCancelled(String),

    #[error("Retry exhausted: {0}")]
    RetryExhausted(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Protobuf error: {0}")]
    Protobuf(#[from] prost::EncodeError),

    #[error("Protobuf decode error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),

    #[error("Database error: {0}")]
    #[cfg(feature = "postgres")]
    Database(#[from] sqlx::Error),

    #[error("gRPC error: {0}")]
    Grpc(Box<tonic::Status>),

    #[error("Transport error: {0}")]
    Transport(Box<tonic::transport::Error>),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("UUID error: {0}")]
    Uuid(#[from] uuid::Error),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Replay error: {0}")]
    Replay(String),
}

#[allow(clippy::result_large_err)]
pub type Result<T> = std::result::Result<T, Error>;

impl From<tonic::Status> for Error {
    fn from(e: tonic::Status) -> Self {
        Error::Grpc(Box::new(e))
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(e: tonic::transport::Error) -> Self {
        Error::Transport(Box::new(e))
    }
}
