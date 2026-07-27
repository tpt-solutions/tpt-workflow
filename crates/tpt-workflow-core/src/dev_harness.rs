use crate::error::Result;
use crate::execution::{ExecutionContext, WorkflowExecutor};
use crate::history::InMemoryHistoryStore;
use crate::persistence::{InMemoryPersistence, PersistenceLayer};
use crate::task_queue::{InMemoryTaskQueue, TaskQueueManager};
use crate::worker::InMemoryWorkerRegistry;
use crate::workflow::{
    WorkflowDefinition, WorkflowExecution, WorkflowExecutionId, WorkflowExecutionStatus,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

fn derive_output(context: &ExecutionContext) -> serde_json::Value {
    if let Some(val) = context.variables.get("output") {
        return val.clone();
    }
    if let Some(last_step_id) = context.completed_step_order.last() {
        if let Some(val) = context.completed_steps.get(last_step_id) {
            return val.clone();
        }
    }
    serde_json::Value::Null
}

pub struct DevEnvironment {
    persistence: Arc<InMemoryPersistence>,
    task_queue_manager: Arc<TaskQueueManager>,
    #[allow(dead_code)]
    worker_registry: Arc<InMemoryWorkerRegistry>,
    #[allow(dead_code)]
    history_store: Arc<InMemoryHistoryStore>,
    executor: Arc<WorkflowExecutor>,
    task_handlers:
        HashMap<String, Box<dyn Fn(serde_json::Value) -> serde_json::Value + Send + Sync>>,
}

impl DevEnvironment {
    pub fn new() -> Self {
        let persistence = Arc::new(InMemoryPersistence::new());
        let task_queue_manager = Arc::new(TaskQueueManager::new());
        let worker_registry = Arc::new(InMemoryWorkerRegistry::new());
        let history_store = Arc::new(InMemoryHistoryStore::new());

        let executor = Arc::new(WorkflowExecutor::new(
            persistence.clone() as Arc<dyn crate::persistence::PersistenceLayer>,
            task_queue_manager.clone(),
            worker_registry.clone() as Arc<dyn crate::worker::WorkerRegistry>,
        ));

        Self {
            persistence,
            task_queue_manager,
            worker_registry,
            history_store,
            executor,
            task_handlers: HashMap::new(),
        }
    }

    pub fn register_task_handler<F>(mut self, task_type: &str, handler: F) -> Self
    where
        F: Fn(serde_json::Value) -> serde_json::Value + Send + Sync + 'static,
    {
        self.task_handlers
            .insert(task_type.to_string(), Box::new(handler));
        self
    }

    pub async fn register_queue(&self, queue_name: &str) -> Result<()> {
        let queue = Arc::new(InMemoryTaskQueue::new(queue_name.to_string()));
        self.task_queue_manager.register_queue(queue).await
    }

    pub async fn run_to_completion(
        &self,
        definition: WorkflowDefinition,
        input: serde_json::Value,
    ) -> Result<WorkflowExecution> {
        let def = Arc::new(definition);
        let workflow_id = WorkflowExecutionId::new();

        self.persistence.save_workflow_definition(&def).await?;

        for step in &def.steps {
            if !self
                .task_queue_manager
                .list_queues()
                .await?
                .contains(&step.task_queue)
            {
                self.register_queue(&step.task_queue).await?;
            }
        }

        let mut execution = WorkflowExecution::new(
            workflow_id.clone(),
            def.id.clone(),
            def.version.clone(),
            input.clone(),
        );
        execution.status = WorkflowExecutionStatus::Running;

        let mut context = ExecutionContext::new(workflow_id.clone(), def.clone(), input);

        info!("Starting workflow {} in dev environment", workflow_id);

        self.executor
            .execute_workflow(&mut execution, &mut context)
            .await?;

        if execution.status == WorkflowExecutionStatus::Completed {
            execution.output = derive_output(&context);
            self.persistence.save_workflow_execution(&execution).await?;
            return Ok(execution);
        }

        if execution.status != WorkflowExecutionStatus::Running {
            self.persistence.save_workflow_execution(&execution).await?;
            return Ok(execution);
        }

        let queues = self.task_queue_manager.list_queues().await?;

        loop {
            let tasks = self
                .task_queue_manager
                .poll_tasks("dev-worker", &queues, 10)
                .await?;

            if tasks.is_empty() {
                break;
            }

            for task in &tasks {
                let handler = self.task_handlers.get(&task.task_type).ok_or_else(|| {
                    crate::error::Error::WorkerNotFound(format!(
                        "No handler registered for task type: {}",
                        task.task_type
                    ))
                })?;

                let output = handler(task.input.clone());
                let queue = self.task_queue_manager.get_queue(&task.task_queue).await?;
                queue
                    .complete_task(&task.task_token, output.clone())
                    .await?;

                context.set_step_output(task.step_id.clone(), output);

                info!(
                    "Completed task {} (step {}) with output",
                    task.task_id, task.step_id
                );
            }

            self.executor
                .execute_workflow(&mut execution, &mut context)
                .await?;

            if execution.status != WorkflowExecutionStatus::Running {
                break;
            }
        }

        if execution.status == WorkflowExecutionStatus::Completed {
            execution.output = derive_output(&context);
        }

        self.persistence.save_workflow_execution(&execution).await?;
        Ok(execution)
    }

    pub async fn get_execution(
        &self,
        id: &WorkflowExecutionId,
    ) -> Result<Option<WorkflowExecution>> {
        self.persistence.get_workflow_execution(id).await
    }

    pub fn executor(&self) -> &Arc<WorkflowExecutor> {
        &self.executor
    }

    pub fn persistence(&self) -> &Arc<InMemoryPersistence> {
        &self.persistence
    }

    pub fn task_queue_manager(&self) -> &Arc<TaskQueueManager> {
        &self.task_queue_manager
    }
}

impl Default for DevEnvironment {
    fn default() -> Self {
        Self::new()
    }
}
