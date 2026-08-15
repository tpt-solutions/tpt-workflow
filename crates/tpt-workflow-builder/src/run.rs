//! Visual run/step mode.
//!
//! Walks the graph one ready step at a time using the same dependency rule the
//! core executor uses (`get_next_step` in `tpt-workflow-core::execution`): a step
//! is ready when all of its incoming (dependency) edges originate from completed
//! steps. This lets the builder *show* execution order without needing a worker
//! pool or persistence layer.

use crate::graph::GraphModel;
use std::collections::HashSet;
use tpt_workflow_core::workflow::StepId;

/// Per-node visual status while stepping through a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeRunStatus {
    Idle,
    Running,
    Done,
    Error,
}

/// Picks the next step that is ready to "execute" given the completed set.
pub struct Runner;

impl Runner {
    pub fn next_ready(model: &GraphModel, completed: &HashSet<StepId>) -> Option<StepId> {
        for node in &model.nodes {
            if completed.contains(node.id()) {
                continue;
            }
            let ready = model
                .edges
                .iter()
                .filter(|e| e.to_step == *node.id())
                .all(|e| completed.contains(&e.from_step));
            if ready {
                return Some(node.id().clone());
            }
        }
        None
    }
}
