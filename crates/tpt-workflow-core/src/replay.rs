//! Workflow replay engine for deterministic replay and debugging

use crate::error::{Error, Result};
use crate::workflow::{
    WorkflowDefinition, WorkflowExecution, WorkflowExecutionId, WorkflowEvent, WorkflowEventType,
    WorkflowExecutionStatus, StepId, TaskId,
};
use crate::execution::{WorkflowExecutor, ExecutionContext};
use crate::persistence::PersistenceLayer;
use crate::history::HistoryStore;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Replay mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayMode {
    /// Full replay from the beginning
    Full,
    /// Replay from a specific event ID
    FromEvent(u64),
    /// Replay with modified input
    WithNewInput(Value),
}

/// Replay result
#[derive(Debug, Clone)]
pub struct ReplayResult {
    pub original_execution: WorkflowExecution,
    pub replayed_execution: WorkflowExecution,
    pub events_diverged_at: Option<u64>,
    pub differences: Vec<ReplayDifference>,
}

/// Difference found during replay
#[derive(Debug, Clone)]
pub struct ReplayDifference {
    pub event_id: u64,
    pub event_type: WorkflowEventType,
    pub original_attribute: Value,
    pub replayed_attribute: Value,
    pub difference_type: DifferenceType,
}

/// Type of difference
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DifferenceType {
    AttributeChanged,
    EventAdded,
    EventRemoved,
    OrderChanged,
}

/// Trait for replay engine
#[async_trait]
pub trait ReplayEngine: Send + Sync {
    /// Replay a workflow execution
    async fn replay(&self, request: ReplayRequest) -> Result<ReplayResult>;
    
    /// Compare two executions for debugging
    async fn compare_executions(&self, execution_id_1: &WorkflowExecutionId, execution_id_2: &WorkflowExecutionId) -> Result<Vec<ReplayDifference>>;
}

/// Replay request
#[derive(Debug, Clone)]
pub struct ReplayRequest {
    pub workflow_id: WorkflowExecutionId,
    pub mode: ReplayMode,
    pub new_input: Option<Value>,
    pub max_events: Option<usize>,
}

/// Default replay engine implementation
pub struct DefaultReplayEngine {
    persistence: Arc<dyn PersistenceLayer>,
    history_store: Arc<dyn HistoryStore>,
    executor: Arc<WorkflowExecutor>,
}

impl DefaultReplayEngine {
    pub fn new(
        persistence: Arc<dyn PersistenceLayer>,
        history_store: Arc<dyn HistoryStore>,
        executor: Arc<WorkflowExecutor>,
    ) -> Self {
        Self {
            persistence,
            history_store,
            executor,
        }
    }
}

#[async_trait]
impl ReplayEngine for DefaultReplayEngine {
    async fn replay(&self, request: ReplayRequest) -> Result<ReplayResult> {
        info!("Starting replay for workflow: {} with mode: {:?}", request.workflow_id, request.mode);
        
        // Get original execution
        let original_execution = self.persistence
            .get_workflow_execution(&request.workflow_id)
            .await?
            .ok_or_else(|| Error::WorkflowExecutionNotFound(request.workflow_id.to_string()))?;
        
        // Get workflow definition
        let definition = self.persistence
            .get_workflow_definition(&original_execution.workflow_definition_id, Some(&original_execution.workflow_definition_version))
            .await?
            .ok_or_else(|| Error::WorkflowDefinitionNotFound(original_execution.workflow_definition_id.to_string()))?;
        
        // Get history events
        let events = self.history_store.get_all_events(&request.workflow_id).await?;
        
        // Determine replay start point
        let (start_event_id, new_input) = match request.mode {
            ReplayMode::Full => (0, request.new_input.unwrap_or(original_execution.input.clone())),
            ReplayMode::FromEvent(event_id) => (event_id, request.new_input.unwrap_or(original_execution.input.clone())),
            ReplayMode::WithNewInput(input) => (0, input),
        };
        
        // Filter events from start point
        let replay_events: Vec<WorkflowEvent> = events
            .into_iter()
            .filter(|e| e.event_id > start_event_id)
            .collect();
        
        // Create new execution for replay
        let mut replayed_execution = WorkflowExecution::new(
            WorkflowExecutionId::new(),
            original_execution.workflow_definition_id.clone(),
            original_execution.workflow_definition_version.clone(),
            new_input,
        );
        
        // Create execution context
        let mut context = ExecutionContext::new(
            replayed_execution.workflow_id.clone(),
            Arc::new(definition),
            replayed_execution.input.clone(),
        );
        
        // Replay events
        let mut differences = Vec::new();
        let mut events_diverged_at = None;
        
        for event in replay_events {
            // Apply event to context
            match self.apply_event(&mut context, &event).await {
                Ok(_) => {
                    // Record event in replayed execution
                    replayed_execution.history_length += 1;
                }
                Err(e) => {
                    warn!("Failed to apply event {}: {}", event.event_id, e);
                    differences.push(ReplayDifference {
                        event_id: event.event_id,
                        event_type: event.event_type,
                        original_attribute: event.attributes.clone(),
                        replayed_attribute: serde_json::json!({"error": e.to_string()}),
                        difference_type: DifferenceType::AttributeChanged,
                    });
                    
                    if events_diverged_at.is_none() {
                        events_diverged_at = Some(event.event_id);
                    }
                }
            }
        }
        
        // Update replayed execution status
        replayed_execution.status = WorkflowExecutionStatus::Completed;
        replayed_execution.completed_at = Some(Utc::now());
        replayed_execution.output = context.variables.get("output").cloned().unwrap_or(Value::Null);
        
        // Save replayed execution
        self.persistence.save_workflow_execution(&replayed_execution).await?;
        
        info!("Replay completed for workflow: {}", request.workflow_id);
        
        Ok(ReplayResult {
            original_execution,
            replayed_execution,
            events_diverged_at,
            differences,
        })
    }
    
    async fn compare_executions(&self, execution_id_1: &WorkflowExecutionId, execution_id_2: &WorkflowExecutionId) -> Result<Vec<ReplayDifference>> {
        let exec1 = self.persistence
            .get_workflow_execution(execution_id_1)
            .await?
            .ok_or_else(|| Error::WorkflowExecutionNotFound(execution_id_1.to_string()))?;
        
        let exec2 = self.persistence
            .get_workflow_execution(execution_id_2)
            .await?
            .ok_or_else(|| Error::WorkflowExecutionNotFound(execution_id_2.to_string()))?;
        
        let events1 = self.history_store.get_all_events(execution_id_1).await?;
        let events2 = self.history_store.get_all_events(execution_id_2).await?;
        
        // Compare events
        let mut differences = Vec::new();
        let max_len = events1.len().max(events2.len());
        
        for i in 0..max_len {
            let e1 = events1.get(i);
            let e2 = events2.get(i);
            
            match (e1, e2) {
                (Some(e1), Some(e2)) => {
                    if e1.event_type != e2.event_type || e1.attributes != e2.attributes {
                        differences.push(ReplayDifference {
                            event_id: e1.event_id,
                            event_type: e1.event_type,
                            original_attribute: e1.attributes.clone(),
                            replayed_attribute: e2.attributes.clone(),
                            difference_type: if e1.event_type != e2.event_type {
                                DifferenceType::AttributeChanged
                            } else {
                                DifferenceType::AttributeChanged
                            },
                        });
                    }
                }
                (Some(e1), None) => {
                    differences.push(ReplayDifference {
                        event_id: e1.event_id,
                        event_type: e1.event_type,
                        original_attribute: e1.attributes.clone(),
                        replayed_attribute: Value::Null,
                        difference_type: DifferenceType::EventRemoved,
                    });
                }
                (None, Some(e2)) => {
                    differences.push(ReplayDifference {
                        event_id: e2.event_id,
                        event_type: e2.event_type,
                        original_attribute: Value::Null,
                        replayed_attribute: e2.attributes.clone(),
                        difference_type: DifferenceType::EventAdded,
                    });
                }
                (None, None) => {}
            }
        }
        
        Ok(differences)
    }
}

impl DefaultReplayEngine {
    /// Apply a single event to the execution context
    async fn apply_event(&self, context: &mut ExecutionContext, event: &WorkflowEvent) -> Result<()> {
        match event.event_type {
            WorkflowEventType::WorkflowStarted => {
                // Initialize workflow
                context.variables.insert("workflow_started".to_string(), event.attributes.clone());
            }
            WorkflowEventType::StepStarted => {
                if let Some(step_id) = &event.step_id {
                    context.current_step = Some(step_id.clone());
                    context.variables.insert(format!("step_{}_started", step_id), event.attributes.clone());
                }
            }
            WorkflowEventType::StepCompleted => {
                if let Some(step_id) = &event.step_id {
                    if let Some(output) = event.attributes.get("output") {
                        context.set_step_output(step_id.clone(), output.clone());
                    }
                }
            }
            WorkflowEventType::StepFailed => {
                if let Some(step_id) = &event.step_id {
                    let error = event.attributes.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
                    return Err(Error::ExecutionFailed(format!("Step {} failed: {}", step_id, error)));
                }
            }
            WorkflowEventType::TaskScheduled => {
                if let Some(task_id) = &event.task_id {
                    context.variables.insert(format!("task_{}_scheduled", task_id), event.attributes.clone());
                }
            }
            WorkflowEventType::TaskCompleted => {
                if let Some(task_id) = &event.task_id {
                    if let Some(output) = event.attributes.get("output") {
                        context.variables.insert(format!("task_{}_output", task_id), output.clone());
                    }
                }
            }
            WorkflowEventType::TaskFailed => {
                if let Some(task_id) = &event.task_id {
                    let error = event.attributes.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
                    return Err(Error::ExecutionFailed(format!("Task {} failed: {}", task_id, error)));
                }
            }
            WorkflowEventType::SignalReceived => {
                if let Some(signal_name) = event.attributes.get("signal_name").and_then(|v| v.as_str()) {
                    context.variables.insert(format!("signal_{}", signal_name), event.attributes.clone());
                }
            }
            WorkflowEventType::TimerFired => {
                if let Some(timer_id) = event.attributes.get("timer_id").and_then(|v| v.as_str()) {
                    context.variables.insert(format!("timer_{}", timer_id), event.attributes.clone());
                }
            }
            WorkflowEventType::WorkflowCompleted => {
                if let Some(output) = event.attributes.get("output") {
                    context.variables.insert("output".to_string(), output.clone());
                }
            }
            WorkflowEventType::WorkflowFailed => {
                let error = event.attributes.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
                return Err(Error::ExecutionFailed(error.to_string()));
            }
            _ => {
                // Other events just recorded
                debug!("Replaying event: {:?}", event.event_type);
            }
        }
        
        Ok(())
    }
}

/// Local replay engine for debugging (doesn't persist)
pub struct LocalReplayEngine {
    definition: Arc<WorkflowDefinition>,
    events: Vec<WorkflowEvent>,
}

impl LocalReplayEngine {
    pub fn new(definition: Arc<WorkflowDefinition>, events: Vec<WorkflowEvent>) -> Self {
        Self { definition, events }
    }
    
    /// Replay locally and return the final state
    pub async fn replay(&self, new_input: Option<Value>) -> Result<ExecutionContext> {
        let input = new_input.unwrap_or_else(|| {
            self.events.iter()
                .find(|e| e.event_type == WorkflowEventType::WorkflowStarted)
                .and_then(|e| e.attributes.get("input").cloned())
                .unwrap_or(Value::Null)
        });
        
        let mut context = ExecutionContext::new(
            WorkflowExecutionId::new(),
            self.definition.clone(),
            input,
        );
        
        for event in &self.events {
            self.apply_event(&mut context, event).await?;
        }
        
        Ok(context)
    }
    
    async fn apply_event(&self, context: &mut ExecutionContext, event: &WorkflowEvent) -> Result<()> {
        match event.event_type {
            WorkflowEventType::StepCompleted => {
                if let Some(step_id) = &event.step_id {
                    if let Some(output) = event.attributes.get("output") {
                        context.set_step_output(step_id.clone(), output.clone());
                    }
                }
            }
            WorkflowEventType::TaskCompleted => {
                if let Some(task_id) = &event.task_id {
                    if let Some(output) = event.attributes.get("output") {
                        context.variables.insert(format!("task_{}_output", task_id), output.clone());
                    }
                }
            }
            WorkflowEventType::SignalReceived => {
                if let Some(signal_name) = event.attributes.get("signal_name").and_then(|v| v.as_str()) {
                    context.variables.insert(format!("signal_{}", signal_name), event.attributes.clone());
                }
            }
            WorkflowEventType::TimerFired => {
                if let Some(timer_id) = event.attributes.get("timer_id").and_then(|v| v.as_str()) {
                    context.variables.insert(format!("timer_{}", timer_id), event.attributes.clone());
                }
            }
            WorkflowEventType::WorkflowCompleted => {
                if let Some(output) = event.attributes.get("output") {
                    context.variables.insert("output".to_string(), output.clone());
                }
            }
            _ => {}
        }
        
        Ok(())
    }
}