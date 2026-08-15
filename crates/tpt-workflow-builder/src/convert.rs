//! Convert between the visual [`GraphModel`] and the core [`WorkflowDefinition`]
//! (the "workflow-as-code" representation).
//!
//! The graph is the source of truth for *topology*: `next_steps` (success),
//! `error_steps` (error), and `depends_on` (incoming) are all derived from the
//! edges. When loading a `WorkflowDefinition` we rebuild the graph and assign a
//! deterministic layered layout so the graph is immediately readable.

use crate::error::{BuilderError, Result};
use crate::graph::{GraphEdge, GraphMeta, GraphModel, GraphNode, PortKind};
use std::collections::{HashMap, VecDeque};
use tpt_workflow_core::workflow::{StepDefinition, StepId, WorkflowDefinition};

impl GraphModel {
    /// Build a core `WorkflowDefinition` from the current graph.
    pub fn to_workflow_definition(&self) -> Result<WorkflowDefinition> {
        if graph_has_cycle(self) {
            return Err(BuilderError::Cycle);
        }

        let mut def = WorkflowDefinition::new(self.meta.name.clone(), self.meta.version.clone());
        def.description = self.meta.description.clone();

        let mut steps: Vec<StepDefinition> = Vec::new();
        for node in &self.nodes {
            let mut step = node.step.clone();
            let mut next = Vec::new();
            let mut err = Vec::new();
            for e in &self.edges {
                if e.from_step == step.id {
                    match e.from_port {
                        PortKind::Success => next.push(e.to_step.clone()),
                        PortKind::Error => err.push(e.to_step.clone()),
                    }
                }
            }
            step.next_steps = next;
            step.error_steps = err;
            steps.push(step);
        }

        for step in &mut steps {
            let deps: Vec<StepId> = self
                .edges
                .iter()
                .filter(|e| e.to_step == step.id)
                .map(|e| e.from_step.clone())
                .collect();
            step.depends_on = deps;
        }

        def.steps = steps;
        def.validate()
            .map_err(|e| BuilderError::Invalid(e.to_string()))?;
        Ok(def)
    }

    /// Build a graph from a core `WorkflowDefinition`, auto-laying-out the nodes.
    pub fn from_workflow_definition(def: &WorkflowDefinition) -> Result<GraphModel> {
        let order = topological_order(def)?;
        let depth = depth_map(def, &order);
        let max_col = depth.values().copied().max().unwrap_or(0);
        let mut col_count = vec![0usize; max_col + 1];
        let mut pos: HashMap<StepId, [f32; 2]> = HashMap::new();
        for id in &order {
            let col = depth.get(id).copied().unwrap_or(0);
            let row = col_count[col];
            col_count[col] += 1;
            pos.insert(
                id.clone(),
                [col as f32 * 260.0 + 60.0, row as f32 * 130.0 + 80.0],
            );
        }

        let mut nodes = Vec::with_capacity(def.steps.len());
        for step in &def.steps {
            let p = pos.get(&step.id).copied().unwrap_or([60.0, 80.0]);
            nodes.push(GraphNode {
                step: step.clone(),
                pos: p,
            });
        }

        let mut edges = Vec::new();
        for step in &def.steps {
            for t in &step.next_steps {
                edges.push(GraphEdge {
                    id: uuid::Uuid::new_v4().to_string(),
                    from_step: step.id.clone(),
                    from_port: PortKind::Success,
                    to_step: t.clone(),
                });
            }
            for t in &step.error_steps {
                edges.push(GraphEdge {
                    id: uuid::Uuid::new_v4().to_string(),
                    from_step: step.id.clone(),
                    from_port: PortKind::Error,
                    to_step: t.clone(),
                });
            }
        }

        Ok(GraphModel {
            meta: GraphMeta {
                name: def.name.clone(),
                version: def.version.clone(),
                description: def.description.clone(),
            },
            nodes,
            edges,
        })
    }

    /// Serialize the full graph (including canvas positions) to a file.
    pub fn save_graph(&self, path: &str) -> Result<()> {
        let s = serde_json::to_string_pretty(self)?;
        std::fs::write(path, s)?;
        Ok(())
    }

    /// Load a previously saved graph from a file.
    pub fn load_graph(path: &str) -> Result<GraphModel> {
        let s = std::fs::read_to_string(path)?;
        let m: GraphModel = serde_json::from_str(&s)?;
        Ok(m)
    }
}

/// Kahn topological order over `depends_on`; errors on cycle.
fn topological_order(def: &WorkflowDefinition) -> Result<Vec<StepId>> {
    let mut indeg: HashMap<StepId, usize> = HashMap::new();
    let mut adj: HashMap<StepId, Vec<StepId>> = HashMap::new();
    for s in &def.steps {
        indeg.entry(s.id.clone()).or_insert(0);
    }
    for s in &def.steps {
        for d in &s.depends_on {
            *indeg.entry(s.id.clone()).or_insert(0) += 1;
            adj.entry(d.clone()).or_default().push(s.id.clone());
        }
    }
    let mut queue: VecDeque<StepId> = indeg
        .iter()
        .filter(|(_, &v)| v == 0)
        .map(|(k, _)| k.clone())
        .collect();
    let mut order = Vec::new();
    while let Some(id) = queue.pop_front() {
        order.push(id.clone());
        if let Some(ns) = adj.get(&id) {
            for n in ns {
                let e = indeg.get_mut(n).unwrap();
                *e -= 1;
                if *e == 0 {
                    queue.push_back(n.clone());
                }
            }
        }
    }
    if order.len() != def.steps.len() {
        return Err(BuilderError::Cycle);
    }
    Ok(order)
}

/// Longest-path depth for each step, used for layered layout.
fn depth_map(def: &WorkflowDefinition, order: &[StepId]) -> HashMap<StepId, usize> {
    let mut depth: HashMap<StepId, usize> = HashMap::new();
    for id in order {
        let mut d = 0usize;
        if let Some(step) = def.get_step(id) {
            for dep in &step.depends_on {
                d = d.max(depth.get(dep).copied().unwrap_or(0) + 1);
            }
        }
        depth.insert(id.clone(), d);
    }
    depth
}

/// True when the directed graph formed by the edges contains a cycle.
fn graph_has_cycle(model: &GraphModel) -> bool {
    let mut indeg: HashMap<StepId, usize> = HashMap::new();
    let mut adj: HashMap<StepId, Vec<StepId>> = HashMap::new();
    for n in &model.nodes {
        indeg.entry(n.id().clone()).or_insert(0);
    }
    for e in &model.edges {
        *indeg.entry(e.to_step.clone()).or_insert(0) += 1;
        adj.entry(e.from_step.clone())
            .or_default()
            .push(e.to_step.clone());
    }
    let mut queue: VecDeque<StepId> = indeg
        .iter()
        .filter(|(_, &v)| v == 0)
        .map(|(k, _)| k.clone())
        .collect();
    let mut count = 0;
    while let Some(id) = queue.pop_front() {
        count += 1;
        if let Some(ns) = adj.get(&id) {
            for n in ns {
                let e = indeg.get_mut(n).unwrap();
                *e -= 1;
                if *e == 0 {
                    queue.push_back(n.clone());
                }
            }
        }
    }
    count != model.nodes.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_workflow_core::workflow::StepType;

    #[test]
    fn round_trips_through_definition() {
        let mut m = GraphModel::new();
        let a = m.add_node(StepType::Task, [0.0, 0.0]);
        let b = m.add_node(StepType::Decision, [200.0, 0.0]);
        let c = m.add_node(StepType::Task, [400.0, 0.0]);
        m.add_edge(a.clone(), PortKind::Success, b.clone()).unwrap();
        m.add_edge(b.clone(), PortKind::Success, c.clone()).unwrap();

        let def = m.to_workflow_definition().unwrap();
        assert_eq!(def.steps.len(), 3);
        assert_eq!(def.steps[0].next_steps.len(), 1);
        assert_eq!(def.steps[1].depends_on.len(), 1);

        let m2 = GraphModel::from_workflow_definition(&def).unwrap();
        let def2 = m2.to_workflow_definition().unwrap();
        assert_eq!(def2.steps.len(), 3);
        assert_eq!(def2.steps[0].next_steps.len(), 1);
    }

    #[test]
    fn rejects_cycles() {
        let mut m = GraphModel::new();
        let a = m.add_node(StepType::Task, [0.0, 0.0]);
        let b = m.add_node(StepType::Task, [200.0, 0.0]);
        m.add_edge(a.clone(), PortKind::Success, b.clone());
        m.add_edge(b.clone(), PortKind::Success, a.clone());
        assert!(m.to_workflow_definition().is_err());
    }

    #[test]
    fn rejects_self_and_duplicate_edges() {
        let mut m = GraphModel::new();
        let a = m.add_node(StepType::Task, [0.0, 0.0]);
        let b = m.add_node(StepType::Task, [200.0, 0.0]);
        assert!(m
            .add_edge(a.clone(), PortKind::Success, a.clone())
            .is_none());
        let first = m.add_edge(a.clone(), PortKind::Success, b.clone());
        assert!(first.is_some());
        assert!(m
            .add_edge(a.clone(), PortKind::Success, b.clone())
            .is_none());
    }

    #[test]
    fn saves_and_loads() {
        let mut m = GraphModel::new();
        m.add_node(StepType::Wait, [10.0, 20.0]);
        let path = std::env::temp_dir().join("tpt_builder_test.json");
        m.save_graph(path.to_str().unwrap()).unwrap();
        let loaded = GraphModel::load_graph(path.to_str().unwrap()).unwrap();
        assert_eq!(loaded.nodes.len(), 1);
        std::fs::remove_file(path).ok();
    }
}
