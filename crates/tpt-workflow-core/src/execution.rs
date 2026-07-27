//! Workflow execution engine

use crate::error::{Error, Result};
use crate::workflow::{
    WorkflowDefinition, WorkflowExecution, WorkflowExecutionId, WorkflowExecutionStatus,
    WorkflowEvent, WorkflowEventType, StepDefinition, StepId, TaskId,
};
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

/// Context for workflow execution
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    pub workflow_id: WorkflowExecutionId,
    pub workflow_definition: Arc<WorkflowDefinition>,
    pub input: Value,
    pub variables: HashMap<String, Value>,
    pub current_step: Option<StepId>,
    pub completed_steps: HashMap<StepId, Value>,
    pub step_attempts: HashMap<StepId, u32>,
}

impl ExecutionContext {
    pub fn new(
        workflow_id: WorkflowExecutionId,
        workflow_definition: Arc<WorkflowDefinition>,
        input: Value,
    ) -> Self {
        Self {
            workflow_id,
            workflow_definition,
            input,
            variables: HashMap::new(),
            current_step: None,
            completed_steps: HashMap::new(),
            step_attempts: HashMap::new(),
        }
    }
    
    pub fn get_step_output(&self, step_id: &StepId) -> Option<&Value> {
        self.completed_steps.get(step_id)
    }
    
    pub fn set_step_output(&mut self, step_id: StepId, output: Value) {
        self.completed_steps.insert(step_id, output);
    }
    
    pub fn increment_attempt(&mut self, step_id: &StepId) -> u32 {
        let attempt = self.step_attempts.entry(step_id.clone()).or_insert(0);
        *attempt += 1;
        *attempt
    }
    
    pub fn get_attempt(&self, step_id: &StepId) -> u32 {
        self.step_attempts.get(step_id).copied().unwrap_or(0)
    }
}

/// Result of executing a workflow step
#[derive(Debug, Clone)]
pub enum StepExecutionResult {
    /// Step completed successfully with output
    Completed(Value),
    /// Step failed with error
    Failed(String),
    /// Step is waiting for external event
    Waiting(String),
    /// Step should be retried
    Retry(String),
    /// Workflow should continue as new
    ContinueAsNew(Value),
}

/// Workflow executor - handles deterministic replay of workflows
pub struct WorkflowExecutor {
    persistence: Arc<dyn crate::persistence::PersistenceLayer>,
    task_queue_manager: Arc<crate::task_queue::TaskQueueManager>,
    worker_registry: Arc<crate::worker::WorkerRegistry>,
}

impl WorkflowExecutor {
    pub fn new(
        persistence: Arc<dyn crate::persistence::PersistenceLayer>,
        task_queue_manager: Arc<crate::task_queue::TaskQueueManager>,
        worker_registry: Arc<crate::worker::WorkerRegistry>,
    ) -> Self {
        Self {
            persistence,
            task_queue_manager,
            worker_registry,
        }
    }
    
    /// Start a new workflow execution
    #[instrument(skip(self, definition, input))]
    pub async fn start_workflow(
        &self,
        workflow_id: WorkflowExecutionId,
        definition: Arc<WorkflowDefinition>,
        input: Value,
    ) -> Result<WorkflowExecution> {
        info!("Starting workflow execution: {}", workflow_id);
        
        let mut execution = WorkflowExecution::new(
            workflow_id.clone(),
            definition.id.clone(),
            definition.version.clone(),
            input.clone(),
        );
        
        // Create execution context
        let mut context = ExecutionContext::new(workflow_id.clone(), definition.clone(), input);
        
        // Record workflow started event
        let event = WorkflowEvent::new(
            WorkflowEventType::WorkflowStarted,
            serde_json::json!({
                "workflow_definition_id": definition.id.to_string(),
                "workflow_definition_version": definition.version,
            }),
        );
        context.add_event(event);
        
        // Execute the workflow
        match self.execute_workflow(&mut execution, &mut context).await {
            Ok(_) => {
                execution.complete(context.variables.get("output").cloned().unwrap_or(Value::Null));
            }
            Err(e) => {
                execution.fail(e.to_string(), "ExecutionError".to_string());
            }
        }
        
        // Persist the execution
        self.persistence.save_workflow_execution(&execution).await?;
        
        Ok(execution)
    }
    
    /// Execute workflow from current state (for replay)
    #[instrument(skip(self, execution, context))]
    pub async fn execute_workflow(
        &self,
        execution: &mut WorkflowExecution,
        context: &mut ExecutionContext,
    ) -> Result<()> {
        let definition = &context.workflow_definition;
        
        // Determine next step to execute
        while let Some(step_id) = self.get_next_step(definition, context)? {
            context.current_step = Some(step_id.clone());
            execution.current_step = Some(step_id.clone());
            execution.updated_at = Utc::now();
            
            // Execute the step
            let step = definition.get_step(&step_id).ok_or_else(|| {
                Error::InvalidWorkflowDefinition(format!("Step not found: {}", step_id))
            })?;
            
            debug!("Executing step: {}", step_id);
            
            // Record step started event
            let event = WorkflowEvent::new(
                WorkflowEventType::StepStarted,
                serde_json::json!({
                    "step_id": step_id.to_string(),
                    "step_name": step.name,
                    "step_type": format!("{:?}", step.step_type),
                }),
            );
            context.add_event(event);
            
            match self.execute_step(step, context).await {
                Ok(StepExecutionResult::Completed(output)) => {
                    context.set_step_output(step_id.clone(), output);
                    
                    let event = WorkflowEvent::new(
                        WorkflowEventType::StepCompleted,
                        serde_json::json!({
                            "step_id": step_id.to_string(),
                            "output": output,
                        }),
                    );
                    context.add_event(event);
                }
                Ok(StepExecutionResult::Failed(error)) => {
                    // Check if we should retry
                    let attempt = context.increment_attempt(&step_id);
                    if attempt < step.retry_policy.max_attempts {
                        warn!("Step {} failed (attempt {}/{}): {}, retrying...", 
                            step_id, attempt, step.retry_policy.max_attempts, error);
                        
                        let event = WorkflowEvent::new(
                            WorkflowEventType::StepFailed,
                            serde_json::json!({
                                "step_id": step_id.to_string(),
                                "error": error,
                                "attempt": attempt,
                                "will_retry": true,
                            }),
                        );
                        context.add_event(event);
                        
                        // Wait for backoff
                        let delay = self.calculate_backoff(&step.retry_policy, attempt);
                        tokio::time::sleep(delay.to_std().unwrap_or_default()).await;
                        continue; // Retry the step
                    } else {
                        error!("Step {} failed after {} attempts: {}", step_id, attempt, error);
                        
                        let event = WorkflowEvent::new(
                            WorkflowEventType::StepFailed,
                            serde_json::json!({
                                "step_id": step_id.to_string(),
                                "error": error,
                                "attempt": attempt,
                                "will_retry": false,
                            }),
                        );
                        context.add_event(event);
                        
                        // Check for error handling steps
                        if !step.error_steps.is_empty() {
                            // Execute error steps
                            for error_step_id in &step.error_steps {
                                if let Some(error_step) = definition.get_step(error_step_id) {
                                    // Execute error handling step
                                    // For now, just continue
                                }
                            }
                        }
                        
                        return Err(Error::ExecutionFailed(format!("Step {} failed: {}", step_id, error)));
                    }
                }
                Ok(StepExecutionResult::Waiting(reason)) => {
                    // Step is waiting for external event
                    info!("Step {} waiting: {}", step_id, reason);
                    // Persist state and return
                    self.persistence.save_workflow_execution(execution).await?;
                    return Ok(());
                }
                Ok(StepExecutionResult::Retry(reason)) => {
                    let attempt = context.increment_attempt(&step_id);
                    if attempt < step.retry_policy.max_attempts {
                        warn!("Step {} retry requested (attempt {}/{}): {}", 
                            step_id, attempt, step.retry_policy.max_attempts, reason);
                        let delay = self.calculate_backoff(&step.retry_policy, attempt);
                        tokio::time::sleep(delay.to_std().unwrap_or_default()).await;
                        continue;
                    }
                    return Err(Error::ExecutionFailed(format!("Step {} retry exhausted: {}", step_id, reason)));
                }
                Ok(StepExecutionResult::ContinueAsNew(new_input)) => {
                    execution.status = WorkflowExecutionStatus::ContinuedAsNew;
                    execution.output = new_input;
                    execution.completed_at = Some(Utc::now());
                    return Ok(());
                }
                Err(e) => {
                    error!("Step {} execution error: {}", step_id, e);
                    return Err(e);
                }
            }
            
            // Persist state after each step
            self.persistence.save_workflow_execution(execution).await?;
        }
        
        // Workflow completed
        if execution.status == WorkflowExecutionStatus::Running {
            execution.status = WorkflowExecutionStatus::Completed;
            execution.completed_at = Some(Utc::now());
        }
        
        Ok(())
    }
    
    /// Execute a single step
    async fn execute_step(
        &self,
        step: &StepDefinition,
        context: &mut ExecutionContext,
    ) -> Result<StepExecutionResult> {
        match step.step_type {
            crate::workflow::StepType::Task => {
                self.execute_task_step(step, context).await
            }
            crate::workflow::StepType::Decision => {
                self.execute_decision_step(step, context).await
            }
            crate::workflow::StepType::Parallel => {
                self.execute_parallel_step(step, context).await
            }
            crate::workflow::StepType::Sequence => {
                self.execute_sequence_step(step, context).await
            }
            crate::workflow::StepType::Wait => {
                self.execute_wait_step(step, context).await
            }
            crate::workflow::StepType::Signal => {
                self.execute_signal_step(step, context).await
            }
            crate::workflow::StepType::ChildWorkflow => {
                self.execute_child_workflow_step(step, context).await
            }
            crate::workflow::StepType::ContinueAsNew => {
                self.execute_continue_as_new_step(step, context).await
            }
        }
    }
    
    /// Execute a task step (dispatch to worker)
    async fn execute_task_step(
        &self,
        step: &StepDefinition,
        context: &mut ExecutionContext,
    ) -> Result<StepExecutionResult> {
        let task_id = TaskId::new();
        
        // Prepare task input from workflow input and step input
        let task_input = self.prepare_task_input(step, context)?;
        
        // Schedule task via task queue
        self.task_queue_manager
            .schedule_task(
                &step.task_queue,
                &step.task_type,
                task_input,
                context.workflow_id.clone(),
                step.id.clone(),
            )
            .await?;
        
        // Record task scheduled event
        let event = WorkflowEvent::new(
            WorkflowEventType::TaskScheduled,
            serde_json::json!({
                "step_id": step.id.to_string(),
                "task_id": task_id.to_string(),
                "task_queue": step.task_queue,
                "task_type": step.task_type,
            }),
        );
        context.add_event(event);
        
        // For now, return waiting - in real implementation, we'd wait for completion
        // via async notification or polling
        Ok(StepExecutionResult::Waiting(format!("Task {} scheduled", task_id)))
    }
    
    /// Execute a decision step (evaluate condition)
    async fn execute_decision_step(
        &self,
        step: &StepDefinition,
        context: &mut ExecutionContext,
    ) -> Result<StepExecutionResult> {
        // Evaluate decision expression
        // For now, just return completed with decision result
        Ok(StepExecutionResult::Completed(serde_json::json!({
            "decision": "continue",
            "next_steps": step.next_steps.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        })))
    }
    
    /// Execute parallel step
    async fn execute_parallel_step(
        &self,
        step: &StepDefinition,
        context: &mut ExecutionContext,
    ) -> Result<StepExecutionResult> {
        // Execute multiple steps in parallel
        // For now, just return completed
        Ok(StepExecutionResult::Completed(serde_json::json!({
            "parallel_steps": step.next_steps.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        })))
    }
    
    /// Execute sequence step
    async fn execute_sequence_step(
        &self,
        step: &StepDefinition,
        context: &mut ExecutionContext,
    ) -> Result<StepExecutionResult> {
        // Sequence steps are handled by the main workflow loop
        Ok(StepExecutionResult::Completed(Value::Null))
    }
    
    /// Execute wait step
    async fn execute_wait_step(
        &self,
        step: &StepDefinition,
        context: &mut ExecutionContext,
    ) -> Result<StepExecutionResult> {
        // Wait for signal or timer
        Ok(StepExecutionResult::Waiting("Waiting for signal or timer".to_string()))
    }
    
    /// Execute signal step
    async fn execute_signal_step(
        &self,
        step: &StepDefinition,
        context: &mut ExecutionContext,
    ) -> Result<StepExecutionResult> {
        // Send signal to another workflow
        Ok(StepExecutionResult::Completed(Value::Null))
    }
    
    /// Execute child workflow step
    async fn execute_child_workflow_step(
        &self,
        step: &StepDefinition,
        context: &mut ExecutionContext,
    ) -> Result<StepExecutionResult> {
        // Start child workflow
        Ok(StepExecutionResult::Waiting("Child workflow started".to_string()))
    }
    
    /// Execute continue-as-new step
    async fn execute_continue_as_new_step(
        &self,
        step: &StepDefinition,
        context: &mut ExecutionContext,
    ) -> Result<StepExecutionResult> {
        let new_input = self.prepare_task_input(step, context)?;
        Ok(StepExecutionResult::ContinueAsNew(new_input))
    }
    
    /// Prepare task input by combining workflow input and step input
    fn prepare_task_input(&self, step: &StepDefinition, context: &ExecutionContext) -> Result<Value> {
        let mut input = context.input.clone();
        
        // Merge step-specific input
        if let Value::Object(step_input) = &step.input {
            if let Value::Object(workflow_input) = &mut input {
                for (k, v) in step_input {
                    workflow_input.insert(k.clone(), v.clone());
                }
            }
        }
        
        // Add completed step outputs
        for (step_id, output) in &context.completed_steps {
            if let Value::Object(obj) = &mut input {
                obj.insert(format!("step_{}", step_id), output.clone());
            }
        }
        
        Ok(input)
    }
    
    /// Calculate backoff duration
    fn calculate_backoff(&self, policy: &crate::workflow::RetryPolicy, attempt: u32) -> Duration {
        let base = policy.initial_interval;
        let coefficient = policy.backoff_coefficient;
        let max = policy.maximum_interval;
        
        let mut delay = base;
        for _ in 1..attempt {
            delay = Duration::nanoseconds((delay.num_nanoseconds().unwrap_or(0) as f64 * coefficient) as i64);
            if delay > max {
                delay = max;
                break;
            }
        }
        
        delay
    }
    
    /// Get next step to execute based on workflow definition and current state
    fn get_next_step(&self, definition: &WorkflowDefinition, context: &ExecutionContext) -> Result<Option<StepId>> {
        // Find steps that have all dependencies completed and haven't been executed
        for step in &definition.steps {
            // Skip if already completed
            if context.completed_steps.contains_key(&step.id) {
                continue;
            }
            
            // Check if all dependencies are completed
            let deps_completed = step.depends_on.iter().all(|dep| {
                context.completed_steps.contains_key(dep)
            });
            
            if deps_completed {
                return Ok(Some(step.id.clone()));
            }
        }
        
        Ok(None)
    }
}

// Add helper methods to ExecutionContext
impl ExecutionContext {
    fn add_event(&mut self, event: WorkflowEvent) {
        // In real implementation, this would add to a persistent event store
        debug!("Event recorded: {:?}", event.event_type);
    }
}