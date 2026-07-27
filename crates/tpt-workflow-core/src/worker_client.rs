//! Worker-side gRPC client SDK for connecting to the workflow engine
//!
//! Wraps the generated tonic client with a clean API for workers to:
//! - Register, heartbeat, and deregister with the engine
//! - Poll for tasks on assigned task queues
//! - Complete, heartbeat, and cancel tasks

use tpt_workflow_proto::tpt::workflow::v1 as proto;
use proto::workflow_engine_service_client::WorkflowEngineServiceClient;
use proto::*;

use tonic::transport::Channel;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Worker-side gRPC client that wraps the tonic-generated client
/// with convenience methods for workflow task operations.
pub struct WorkerGrpcClient {
    client: Arc<RwLock<WorkflowEngineServiceClient<Channel>>>,
    worker_id: String,
}

impl WorkerGrpcClient {
    /// Connect to the workflow engine at the given endpoint.
    pub async fn connect(endpoint: &str, worker_id: String) -> Result<Self, tonic::transport::Error> {
        let channel = tonic::transport::Endpoint::new(endpoint.to_string())?
            .connect()
            .await?;

        let client = WorkflowEngineServiceClient::new(channel);

        Ok(Self {
            client: Arc::new(RwLock::new(client)),
            worker_id,
        })
    }

    /// Register this worker with the engine.
    pub async fn register(
        &self,
        worker_type: String,
        task_queues: Vec<String>,
        capabilities: Option<prost_types::Struct>,
        max_concurrent_tasks: i32,
    ) -> Result<WorkerRegisterResponse, tonic::Status> {
        let mut client = self.client.write().await;
        client
            .register_worker(tonic::Request::new(WorkerRegisterRequest {
                worker_id: self.worker_id.clone(),
                worker_type,
                task_queues,
                capabilities,
                metadata: None,
                max_concurrent_tasks,
            }))
            .await
            .map(|r| r.into_inner())
    }

    /// Send a heartbeat to the engine to indicate this worker is alive.
    pub async fn heartbeat(
        &self,
        status: i32,
        current_task_count: i32,
        metrics: Option<prost_types::Struct>,
    ) -> Result<WorkerHeartbeatResponse, tonic::Status> {
        let mut client = self.client.write().await;
        client
            .worker_heartbeat(tonic::Request::new(WorkerHeartbeatRequest {
                worker_id: self.worker_id.clone(),
                status,
                current_task_count,
                metrics,
            }))
            .await
            .map(|r| r.into_inner())
    }

    /// Deregister this worker from the engine.
    pub async fn deregister(&self, graceful: bool) -> Result<WorkerDeregisterResponse, tonic::Status> {
        let mut client = self.client.write().await;
        client
            .deregister_worker(tonic::Request::new(WorkerDeregisterRequest {
                worker_id: self.worker_id.clone(),
                graceful,
            }))
            .await
            .map(|r| r.into_inner())
    }

    /// Poll for tasks on the given task queues.
    /// Uses long-polling: returns when a task is available or after timeout.
    pub async fn poll_tasks(
        &self,
        task_queues: Vec<String>,
        max_tasks: i32,
        long_poll_timeout: Option<prost_types::Duration>,
    ) -> Result<Vec<WorkerTaskAssignment>, tonic::Status> {
        let mut client = self.client.write().await;
        client
            .poll_task(tonic::Request::new(PollTaskRequest {
                worker_id: self.worker_id.clone(),
                task_queues,
                max_tasks,
                long_poll_timeout,
            }))
            .await
            .map(|r| r.into_inner().tasks)
    }

    /// Complete a task with the given result.
    pub async fn complete_task(
        &self,
        task_id: String,
        task_token: String,
        status: TaskResultStatus,
        output: Option<prost_types::Struct>,
    ) -> Result<TaskCompleteResponse, tonic::Status> {
        let mut client = self.client.write().await;
        client
            .complete_task(tonic::Request::new(TaskCompleteRequest {
                task_token: task_token.clone(),
                result: Some(TaskResult {
                    task_id,
                    task_token,
                    status: status.into(),
                    output,
                    search_attributes: None,
                    metadata: None,
                    error_message: String::new(),
                    error_type: String::new(),
                    attempt: 0,
                    completed_at: None,
                }),
            }))
            .await
            .map(|r| r.into_inner())
    }

    /// Send a heartbeat for a running task to prevent timeout.
    pub async fn task_heartbeat(
        &self,
        task_token: String,
        progress: Option<prost_types::Struct>,
    ) -> Result<TaskHeartbeatResponse, tonic::Status> {
        let mut client = self.client.write().await;
        client
            .heartbeat_task(tonic::Request::new(TaskHeartbeatRequest {
                task_token,
                progress,
            }))
            .await
            .map(|r| r.into_inner())
    }

    /// Cancel a running task.
    pub async fn cancel_task(
        &self,
        task_token: String,
        reason: String,
    ) -> Result<TaskCancelResponse, tonic::Status> {
        let mut client = self.client.write().await;
        client
            .cancel_task(tonic::Request::new(TaskCancelRequest {
                task_token,
                reason,
            }))
            .await
            .map(|r| r.into_inner())
    }

    /// Start a workflow execution.
    pub async fn start_workflow(
        &self,
        workflow_definition_id: String,
        workflow_definition_version: String,
        input: Option<prost_types::Struct>,
    ) -> Result<StartWorkflowExecutionResponse, tonic::Status> {
        let mut client = self.client.write().await;
        client
            .start_workflow_execution(tonic::Request::new(StartWorkflowExecutionRequest {
                workflow_id: String::new(),
                workflow_definition_id,
                workflow_definition_version,
                input,
                search_attributes: None,
                metadata: None,
                task_queue: String::new(),
                parent_workflow_id: String::new(),
                parent_step_id: String::new(),
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
            }))
            .await
            .map(|r| r.into_inner())
    }

    /// Get the status of a workflow execution.
    pub async fn get_workflow(
        &self,
        workflow_id: String,
        run_id: String,
    ) -> Result<GetWorkflowExecutionResponse, tonic::Status> {
        let mut client = self.client.write().await;
        client
            .get_workflow_execution(tonic::Request::new(GetWorkflowExecutionRequest {
                workflow_id,
                run_id,
            }))
            .await
            .map(|r| r.into_inner())
    }

    /// Get the worker ID for this client.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
}
