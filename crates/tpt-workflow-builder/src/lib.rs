//! Visual workflow builder for tpt-workflow.
//!
//! A self-contained `egui`/`eframe` node-graph editor (vendored from
//! `tpt-appfront/examples/node-graph`, which lives outside `tpt-appfront-canvas`
//! by design) plus a workflow-specific domain model that round-trips to and from
//! the core `WorkflowDefinition` (workflow-as-code).

pub mod app;
pub mod convert;
pub mod error;
pub mod graph;
pub mod palette;
pub mod run;
pub mod viewer;

pub use app::BuilderApp;
pub use error::{BuilderError, Result};
pub use graph::{GraphEdge, GraphMeta, GraphModel, GraphNode, PortKind};
pub use palette::{palette_label, PALETTE};
pub use run::{NodeRunStatus, Runner};
