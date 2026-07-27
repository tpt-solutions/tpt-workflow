//! Engine-side gRPC server for workflow management

use crate::persistence::PersistenceLayer;
use crate::task_queue::TaskQueueManager;
use crate::worker::WorkerRegistry;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use tpt_workflow_proto::tpt::workflow::v1 as proto;
use proto::workflow_engine_service_server::{WorkflowEngineServiceServer, WorkflowEngineService};
use proto::*;

use crate::workflow as core;

pub struct WorkflowEngineServiceImpl {
    persistence: Arc<dyn PersistenceLayer>,
    task_queue_manager: Arc<TaskQueueManager>,
    worker_registry: Arc<dyn WorkerRegistry>,
    definitions: Arc<RwLock<Vec<core::WorkflowDefinition>>>,
}

impl WorkflowEngineServiceImpl {
    pub fn new(
        persistence: Arc<dyn PersistenceLayer>,
        task_queue_manager: Arc<TaskQueueManager>,
        worker_registry: Arc<dyn WorkerRegistry>,
    ) -> Self {
        Self {
            persistence,
            task_queue_manager,
            worker_registry,
            definitions: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn into_server(self) -> WorkflowEngineServiceServer<Self> {
        WorkflowEngineServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl WorkflowEngineService for WorkflowEngineServiceImpl {
    async fn create_workflow_definition(
        &self,
        request: Request<CreateWorkflowDefinitionRequest>,
    ) -> std::result::Result<Response<CreateWorkflowDefinitionResponse>, Status> {
        let req = request.into_inner();
        let proto_def = req
            .definition
            .ok_or_else(|| Status::invalid_argument("definition is required"))?;

        let mut def = core::WorkflowDefinition::new(proto_def.name, proto_def.version);
        def.description = proto_def.description;

        for proto_step in proto_def.steps {
            let step_id = core::StepId::from_string(proto_step.id);
            let step_type = match StepType::try_from(proto_step.r#type) {
                Ok(StepType::Task) => core::StepType::Task,
                Ok(StepType::Decision) => core::StepType::Decision,
                Ok(StepType::Parallel) => core::StepType::Parallel,
                Ok(StepType::Sequence) => core::StepType::Sequence,
                Ok(StepType::Wait) => core::StepType::Wait,
                Ok(StepType::Signal) => core::StepType::Signal,
                Ok(StepType::ChildWorkflow) => core::StepType::ChildWorkflow,
                Ok(StepType::ContinueAsNew) => core::StepType::ContinueAsNew,
                _ => core::StepType::Task,
            };

            let mut step = core::StepDefinition::new(step_id, proto_step.name, step_type);
            step.description = proto_step.description;
            step.task_queue = proto_step.task_queue;
            step.task_type = proto_step.task_type;

            if let Some(proto_retry) = proto_step.retry_policy {
                step.retry_policy.max_attempts = proto_retry.maximum_attempts as u32;
                if let Some(ref interval) = proto_retry.initial_interval {
                    step.retry_policy.initial_interval =
                        chrono::Duration::from_std(std::time::Duration::from_secs(
                            interval.seconds as u64,
                        ))
                        .unwrap_or_default();
                }
                step.retry_policy.backoff_coefficient = proto_retry.backoff_coefficient;
                if let Some(ref max_interval) = proto_retry.maximum_interval {
                    step.retry_policy.maximum_interval =
                        chrono::Duration::from_std(std::time::Duration::from_secs(
                            max_interval.seconds as u64,
                        ))
                        .unwrap_or_default();
                }
                step.retry_policy.non_retryable_errors = proto_retry.non_retryable_error_types;
            }

            if let Some(proto_timeout) = proto_step.timeout_policy {
                if let Some(ref start_to_close) = proto_timeout.start_to_close_timeout {
                    step.timeout_policy.start_to_close =
                        Some(chrono::Duration::from_std(std::time::Duration::from_secs(
                            start_to_close.seconds as u64,
                        ))
                        .unwrap_or_default());
                }
                if let Some(ref schedule_to_start) = proto_timeout.schedule_to_start_timeout {
                    step.timeout_policy.schedule_to_start =
                        Some(chrono::Duration::from_std(std::time::Duration::from_secs(
                            schedule_to_start.seconds as u64,
                        ))
                        .unwrap_or_default());
                }
            }

            if let Some(proto_perm) = proto_step.permission {
                step.permission.required_role = if proto_perm.resource.is_empty() {
                    None
                } else {
                    Some(proto_perm.resource)
                };
                step.permission.required_attributes = proto_perm.attributes;
                step.permission.required_level = if proto_perm.minimum_level == 0 {
                    None
                } else {
                    Some(proto_perm.minimum_level)
                };
            }

            def = def.add_step(step);
        }

        def.validate()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        self.persistence
            .save_workflow_definition(&def)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(CreateWorkflowDefinitionResponse {
            definition: Some(workflow_def_to_proto(&def)),
        }))
    }

    async fn get_workflow_definition(
        &self,
        request: Request<GetWorkflowDefinitionRequest>,
    ) -> std::result::Result<Response<GetWorkflowDefinitionResponse>, Status> {
        let req = request.into_inner();
        let version = if req.version.is_empty() {
            None
        } else {
            Some(req.version.as_str())
        };

        let def = self
            .persistence
            .get_workflow_definition(&core::WorkflowDefinitionId::from_string(req.id), version)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("workflow definition not found"))?;

        Ok(Response::new(GetWorkflowDefinitionResponse {
            definition: Some(workflow_def_to_proto(&def)),
        }))
    }

    async fn list_workflow_definitions(
        &self,
        _request: Request<ListWorkflowDefinitionsRequest>,
    ) -> std::result::Result<Response<ListWorkflowDefinitionsResponse>, Status> {
        Ok(Response::new(ListWorkflowDefinitionsResponse {
            definitions: Vec::new(),
            next_page_token: String::new(),
        }))
    }

    async fn update_workflow_definition(
        &self,
        _request: Request<UpdateWorkflowDefinitionRequest>,
    ) -> std::result::Result<Response<UpdateWorkflowDefinitionResponse>, Status> {
        Err(Status::unimplemented("update_workflow_definition not yet implemented"))
    }

    async fn delete_workflow_definition(
        &self,
        _request: Request<DeleteWorkflowDefinitionRequest>,
    ) -> std::result::Result<Response<DeleteWorkflowDefinitionResponse>, Status> {
        Err(Status::unimplemented("delete_workflow_definition not yet implemented"))
    }

    async fn start_workflow_execution(
        &self,
        request: Request<StartWorkflowExecutionRequest>,
    ) -> std::result::Result<Response<StartWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();

        let def = self
            .persistence
            .get_workflow_definition(
                &core::WorkflowDefinitionId::from_string(req.workflow_definition_id),
                if req.workflow_definition_version.is_empty() {
                    None
                } else {
                    Some(req.workflow_definition_version.as_str())
                },
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("workflow definition not found"))?;

        let workflow_id = if req.workflow_id.is_empty() {
            core::WorkflowExecutionId::new()
        } else {
            core::WorkflowExecutionId::from_string(req.workflow_id)
        };

        let mut execution = core::WorkflowExecution::new(
            workflow_id,
            def.id.clone(),
            def.version.clone(),
            serde_json::Value::Null,
        );
        execution.status = core::WorkflowExecutionStatus::Running;

        self.persistence
            .save_workflow_execution(&execution)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(StartWorkflowExecutionResponse {
            execution: Some(workflow_exec_to_proto(&execution)),
        }))
    }

    async fn get_workflow_execution(
        &self,
        request: Request<GetWorkflowExecutionRequest>,
    ) -> std::result::Result<Response<GetWorkflowExecutionResponse>, Status> {
        let req = request.into_inner();

        let execution = self
            .persistence
            .get_workflow_execution(&core::WorkflowExecutionId::from_string(req.workflow_id))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("workflow execution not found"))?;

        Ok(Response::new(GetWorkflowExecutionResponse {
            execution: Some(workflow_exec_to_proto(&execution)),
        }))
    }

    async fn list_workflow_executions(
        &self,
        _request: Request<ListWorkflowExecutionsRequest>,
    ) -> std::result::Result<Response<ListWorkflowExecutionsResponse>, Status> {
        Ok(Response::new(ListWorkflowExecutionsResponse {
            executions: Vec::new(),
            next_page_token: String::new(),
        }))
    }

    async fn cancel_workflow_execution(
        &self,
        _request: Request<CancelWorkflowExecutionRequest>,
    ) -> std::result::Result<Response<CancelWorkflowExecutionResponse>, Status> {
        Err(Status::unimplemented("cancel_workflow_execution not yet implemented"))
    }

    async fn terminate_workflow_execution(
        &self,
        _request: Request<TerminateWorkflowExecutionRequest>,
    ) -> std::result::Result<Response<TerminateWorkflowExecutionResponse>, Status> {
        Err(Status::unimplemented("terminate_workflow_execution not yet implemented"))
    }

    async fn signal_workflow_execution(
        &self,
        _request: Request<SignalWorkflowExecutionRequest>,
    ) -> std::result::Result<Response<SignalWorkflowExecutionResponse>, Status> {
        Err(Status::unimplemented("signal_workflow_execution not yet implemented"))
    }

    async fn get_workflow_execution_history(
        &self,
        _request: Request<GetWorkflowExecutionHistoryRequest>,
    ) -> std::result::Result<Response<GetWorkflowExecutionHistoryResponse>, Status> {
        Ok(Response::new(GetWorkflowExecutionHistoryResponse {
            events: Vec::new(),
            next_page_token: String::new(),
            has_more: false,
        }))
    }

    async fn create_task_queue(
        &self,
        _request: Request<CreateTaskQueueRequest>,
    ) -> std::result::Result<Response<CreateTaskQueueResponse>, Status> {
        Err(Status::unimplemented("create_task_queue not yet implemented"))
    }

    async fn get_task_queue(
        &self,
        _request: Request<GetTaskQueueRequest>,
    ) -> std::result::Result<Response<GetTaskQueueResponse>, Status> {
        Err(Status::unimplemented("get_task_queue not yet implemented"))
    }

    async fn list_task_queues(
        &self,
        _request: Request<ListTaskQueuesRequest>,
    ) -> std::result::Result<Response<ListTaskQueuesResponse>, Status> {
        Ok(Response::new(ListTaskQueuesResponse {
            queues: Vec::new(),
            next_page_token: String::new(),
        }))
    }

    async fn delete_task_queue(
        &self,
        _request: Request<DeleteTaskQueueRequest>,
    ) -> std::result::Result<Response<DeleteTaskQueueResponse>, Status> {
        Err(Status::unimplemented("delete_task_queue not yet implemented"))
    }

    async fn get_task_queue_stats(
        &self,
        _request: Request<GetTaskQueueStatsRequest>,
    ) -> std::result::Result<Response<GetTaskQueueStatsResponse>, Status> {
        Err(Status::unimplemented("get_task_queue_stats not yet implemented"))
    }

    async fn register_worker(
        &self,
        _request: Request<WorkerRegisterRequest>,
    ) -> std::result::Result<Response<WorkerRegisterResponse>, Status> {
        Err(Status::unimplemented("register_worker not yet implemented"))
    }

    async fn worker_heartbeat(
        &self,
        _request: Request<WorkerHeartbeatRequest>,
    ) -> std::result::Result<Response<WorkerHeartbeatResponse>, Status> {
        Err(Status::unimplemented("worker_heartbeat not yet implemented"))
    }

    async fn deregister_worker(
        &self,
        _request: Request<WorkerDeregisterRequest>,
    ) -> std::result::Result<Response<WorkerDeregisterResponse>, Status> {
        Err(Status::unimplemented("deregister_worker not yet implemented"))
    }

    async fn get_worker(
        &self,
        _request: Request<GetWorkerRequest>,
    ) -> std::result::Result<Response<GetWorkerResponse>, Status> {
        Err(Status::unimplemented("get_worker not yet implemented"))
    }

    async fn list_workers(
        &self,
        _request: Request<ListWorkersRequest>,
    ) -> std::result::Result<Response<ListWorkersResponse>, Status> {
        Ok(Response::new(ListWorkersResponse {
            workers: Vec::new(),
            next_page_token: String::new(),
        }))
    }

    async fn poll_task(
        &self,
        _request: Request<PollTaskRequest>,
    ) -> std::result::Result<Response<PollTaskResponse>, Status> {
        Ok(Response::new(PollTaskResponse { tasks: Vec::new() }))
    }

    async fn complete_task(
        &self,
        _request: Request<TaskCompleteRequest>,
    ) -> std::result::Result<Response<TaskCompleteResponse>, Status> {
        Ok(Response::new(TaskCompleteResponse {
            accepted: true,
            error_message: String::new(),
        }))
    }

    async fn heartbeat_task(
        &self,
        _request: Request<TaskHeartbeatRequest>,
    ) -> std::result::Result<Response<TaskHeartbeatResponse>, Status> {
        Ok(Response::new(TaskHeartbeatResponse {
            should_cancel: false,
            details: None,
        }))
    }

    async fn cancel_task(
        &self,
        _request: Request<TaskCancelRequest>,
    ) -> std::result::Result<Response<TaskCancelResponse>, Status> {
        Ok(Response::new(TaskCancelResponse {
            cancelled: true,
            error_message: String::new(),
        }))
    }
}

fn workflow_def_to_proto(def: &core::WorkflowDefinition) -> WorkflowDefinition {
    WorkflowDefinition {
        id: def.id.to_string(),
        name: def.name.clone(),
        version: def.version.clone(),
        description: def.description.clone(),
        steps: def.steps.iter().map(|s| step_def_to_proto(s)).collect(),
        created_at: Some(prost_types::Timestamp {
            seconds: def.created_at.timestamp(),
            nanos: def.created_at.timestamp_subsec_nanos() as i32,
        }),
        updated_at: Some(prost_types::Timestamp {
            seconds: def.updated_at.timestamp(),
            nanos: def.updated_at.timestamp_subsec_nanos() as i32,
        }),
        metadata: None,
        search_attributes: None,
    }
}

fn step_def_to_proto(step: &core::StepDefinition) -> StepDefinition {
    StepDefinition {
        id: step.id.to_string(),
        name: step.name.clone(),
        description: step.description.clone(),
        r#type: match step.step_type {
            core::StepType::Task => StepType::Task.into(),
            core::StepType::Decision => StepType::Decision.into(),
            core::StepType::Parallel => StepType::Parallel.into(),
            core::StepType::Sequence => StepType::Sequence.into(),
            core::StepType::Wait => StepType::Wait.into(),
            core::StepType::Signal => StepType::Signal.into(),
            core::StepType::ChildWorkflow => StepType::ChildWorkflow.into(),
            core::StepType::ContinueAsNew => StepType::ContinueAsNew.into(),
        },
        task_queue: step.task_queue.clone(),
        task_type: step.task_type.clone(),
        input: None,
        output: None,
        timeout_policy: None,
        retry_policy: None,
        permission: None,
        next_steps: step.depends_on.iter().map(|s| s.to_string()).collect(),
        error_steps: step.next_steps.iter().map(|s| s.to_string()).collect(),
        metadata: None,
    }
}

fn workflow_exec_to_proto(exec: &core::WorkflowExecution) -> WorkflowExecution {
    let status = match exec.status {
        core::WorkflowExecutionStatus::Unspecified => WorkflowExecutionStatus::Unspecified,
        core::WorkflowExecutionStatus::Running => WorkflowExecutionStatus::Running,
        core::WorkflowExecutionStatus::Completed => WorkflowExecutionStatus::Completed,
        core::WorkflowExecutionStatus::Failed => WorkflowExecutionStatus::Failed,
        core::WorkflowExecutionStatus::TimedOut => WorkflowExecutionStatus::TimedOut,
        core::WorkflowExecutionStatus::Cancelled => WorkflowExecutionStatus::Cancelled,
        core::WorkflowExecutionStatus::Terminated => WorkflowExecutionStatus::Terminated,
        core::WorkflowExecutionStatus::ContinuedAsNew => WorkflowExecutionStatus::ContinuedAsNew,
    };

    WorkflowExecution {
        workflow_id: exec.workflow_id.to_string(),
        workflow_definition_id: exec.workflow_definition_id.to_string(),
        workflow_definition_version: exec.workflow_definition_version.clone(),
        status: status.into(),
        input: None,
        output: None,
        search_attributes: None,
        metadata: None,
        started_at: Some(prost_types::Timestamp {
            seconds: exec.started_at.timestamp(),
            nanos: exec.started_at.timestamp_subsec_nanos() as i32,
        }),
        completed_at: exec.completed_at.map(|t| prost_types::Timestamp {
            seconds: t.timestamp(),
            nanos: t.timestamp_subsec_nanos() as i32,
        }),
        updated_at: Some(prost_types::Timestamp {
            seconds: exec.updated_at.timestamp(),
            nanos: exec.updated_at.timestamp_subsec_nanos() as i32,
        }),
        error_message: exec.error_message.clone().unwrap_or_default(),
        error_type: exec.error_type.clone().unwrap_or_default(),
        history_length: exec.history_length as i64,
        history_size_bytes: exec.history_size_bytes as i64,
        parent_workflow_id: exec
            .parent_workflow_id
            .as_ref()
            .map(|id| id.to_string())
            .unwrap_or_default(),
        parent_step_id: exec
            .parent_step_id
            .as_ref()
            .map(|id| id.to_string())
            .unwrap_or_default(),
        events: Vec::new(),
    }
}

