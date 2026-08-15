//! Workflow-specific node-graph domain model.
//!
//! This is the visual builder's single source of truth. Each [`GraphNode`] owns a
//! [`StepDefinition`] from `tpt-workflow-core` plus a canvas position, and each
//! [`GraphEdge`] is a real step-to-step connection. `next_steps` / `error_steps`
//! / `depends_on` on the underlying `StepDefinition` are *derived* from the edges
//! when the graph is exported to a `WorkflowDefinition` (see [`crate::convert`]).

use serde::{Deserialize, Serialize};
use tpt_workflow_core::workflow::{StepDefinition, StepId, StepType};
use uuid::Uuid;

/// Canvas size of a node, in world units.
pub const NODE_W: f32 = 180.0;
/// Canvas size of a node, in world units.
pub const NODE_H: f32 = 80.0;

/// Which port an edge leaves a node from. The input (dependency) port is always
/// the single left-hand port; only outgoing edges are typed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortKind {
    /// Success control flow (right-top port).
    Success,
    /// Error control flow (right-bottom port).
    Error,
}

/// Workflow-level metadata carried by a graph.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphMeta {
    pub name: String,
    pub version: String,
    pub description: String,
}

impl Default for GraphMeta {
    fn default() -> Self {
        Self {
            name: "untitled-workflow".to_string(),
            version: "1.0.0".to_string(),
            description: String::new(),
        }
    }
}

/// A step rendered as a node on the canvas.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphNode {
    pub step: StepDefinition,
    /// Canvas position (world coordinates, top-left of the node).
    pub pos: [f32; 2],
}

impl GraphNode {
    pub fn id(&self) -> &StepId {
        &self.step.id
    }
}

/// A directed connection between two steps.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphEdge {
    pub id: String,
    pub from_step: StepId,
    pub from_port: PortKind,
    pub to_step: StepId,
}

/// The full visual graph: nodes, edges, and metadata.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GraphModel {
    pub meta: GraphMeta,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

impl GraphModel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn node(&self, id: &StepId) -> Option<&GraphNode> {
        self.nodes.iter().find(|n| n.id() == id)
    }

    pub fn node_mut(&mut self, id: &StepId) -> Option<&mut GraphNode> {
        self.nodes.iter_mut().find(|n| n.id() == id)
    }

    /// Add a new node of the given step type at a world position; returns its id.
    pub fn add_node(&mut self, step_type: StepType, pos: [f32; 2]) -> StepId {
        let id = StepId::new();
        let n = self.nodes.len() + 1;
        let name = format!("{:?} {}", step_type, n);
        let step = StepDefinition::new(id.clone(), name, step_type);
        self.nodes.push(GraphNode { step, pos });
        id
    }

    /// Remove a node and every edge that touches it.
    pub fn remove_node(&mut self, id: &StepId) {
        self.nodes.retain(|n| n.id() != id);
        self.edges
            .retain(|e| &e.from_step != id && &e.to_step != id);
    }

    /// Connect `from` -> `to` on the given outgoing port. Returns the new edge id,
    /// or `None` on self-loop, missing endpoint, or duplicate edge.
    pub fn add_edge(&mut self, from: StepId, from_port: PortKind, to: StepId) -> Option<String> {
        if from == to {
            return None;
        }
        if self.node(&from).is_none() || self.node(&to).is_none() {
            return None;
        }
        if self
            .edges
            .iter()
            .any(|e| e.from_step == from && e.from_port == from_port && e.to_step == to)
        {
            return None;
        }
        let id = Uuid::new_v4().to_string();
        self.edges.push(GraphEdge {
            id: id.clone(),
            from_step: from,
            from_port,
            to_step: to,
        });
        Some(id)
    }

    pub fn remove_edge(&mut self, id: &str) {
        self.edges.retain(|e| e.id != id);
    }
}
