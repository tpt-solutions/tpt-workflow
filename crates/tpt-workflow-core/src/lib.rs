//! Core workflow engine for tpt-workflow

pub mod dev_harness;
pub mod error;
pub mod execution;
pub mod grpc_server;
pub mod history;
#[cfg(feature = "flux")]
pub mod keystone_flux;
pub mod permissions;
pub mod persistence;
pub mod replay;
pub mod synapse;
pub mod task_queue;
pub mod task_queue_naming;
pub mod worker;
pub mod worker_client;
pub mod workflow;

pub use error::{Error, Result};
pub use execution::{ExecutionContext, WorkflowExecutor};
pub use history::{HistoryEvent, HistoryStore};
pub use permissions::{
    ActorContext, ActorStore, AllowAllAuth, AuthDecision, AuthorizationCheck, CompositeAuth,
    RoleBasedAuth, StepPermissionChecker,
};
pub use persistence::PersistenceLayer;
#[cfg(feature = "postgres")]
pub use persistence::PostgresPersistence;
pub use replay::ReplayEngine;
pub use synapse::{
    ConsumerGroup, ConsumerGroupRegistry, SynapseTaskCompletionListener, SynapseTaskQueue,
    SynapseTaskQueueConfig, TaskHandler, DEFAULT_LONG_POLL_TIMEOUT, DEFAULT_VISIBILITY_TIMEOUT,
};
pub use task_queue::{TaskQueue, TaskQueueManager};
pub use task_queue_naming::{QueueScope, TaskQueueName};
pub use worker::{Worker, WorkerRegistry, WorkerStatus};
pub use workflow::{
    StepDefinition, StepType, WorkflowDefinition, WorkflowEvent, WorkflowEventType,
    WorkflowExecution, WorkflowExecutionStatus,
};
