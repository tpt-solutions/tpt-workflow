//! Canvas viewer state and geometry helpers.
//!
//! Vendored from `tpt-appfront/examples/node-graph` (`world_to_screen` /
//! `screen_to_world`, pan-drag, per-node drag, port-to-port wiring), adapted to
//! the workflow domain: nodes carry typed ports (one input on the left, a
//! `Success` port and an `Error` port on the right) and edges connect them.

use crate::graph::{GraphNode, PortKind, NODE_H, NODE_W};
use egui::{Color32, Painter, Pos2, Rect, Vec2};

/// Pan/zoom/interaction state for the infinite canvas.
#[derive(Default, Clone)]
pub struct ViewerState {
    pub pan: Vec2,
    pub zoom: f32,
    /// `(step id, grab offset in world space)` while dragging a node body.
    pub drag_node: Option<(tpt_workflow_core::workflow::StepId, Vec2)>,
    /// Pointer position at drag start, while panning the background.
    pub pan_drag: Option<Pos2>,
    /// `(step id, outgoing port)` while dragging a new wire.
    pub pending_wire: Option<(tpt_workflow_core::workflow::StepId, PortKind)>,
}

impl ViewerState {
    pub fn new() -> Self {
        Self {
            zoom: 1.0,
            ..Default::default()
        }
    }

    /// World (canvas) coordinates -> screen pixels.
    pub fn world_to_screen(&self, p: Pos2) -> Pos2 {
        (Vec2::new(p.x, p.y) * self.zoom + self.pan).to_pos2()
    }

    /// Screen pixels -> world (canvas) coordinates.
    pub fn screen_to_world(&self, p: Pos2) -> Pos2 {
        ((Vec2::new(p.x, p.y) - self.pan) / self.zoom).to_pos2()
    }
}

/// Axis-aligned rect of a node in world space (top-left at `node.pos`).
pub fn node_rect(node: &GraphNode) -> Rect {
    Rect::from_min_size(
        Pos2::new(node.pos[0], node.pos[1]),
        Vec2::new(NODE_W, NODE_H),
    )
}

/// Input (dependency) port, left-center.
pub fn input_port_world(node: &GraphNode) -> Pos2 {
    Pos2::new(node.pos[0], node.pos[1] + NODE_H * 0.5)
}

/// Success port, right-top.
pub fn success_port_world(node: &GraphNode) -> Pos2 {
    Pos2::new(node.pos[0] + NODE_W, node.pos[1] + NODE_H * 0.30)
}

/// Error port, right-bottom.
pub fn error_port_world(node: &GraphNode) -> Pos2 {
    Pos2::new(node.pos[0] + NODE_W, node.pos[1] + NODE_H * 0.70)
}

/// Screen position of a node's outgoing port of the given kind.
pub fn port_screen(viewer: &ViewerState, node: &GraphNode, kind: PortKind) -> Pos2 {
    let world = match kind {
        PortKind::Success => success_port_world(node),
        PortKind::Error => error_port_world(node),
    };
    viewer.world_to_screen(world)
}

/// Draw a small arrowhead at `to` pointing along `from -> to`.
pub fn draw_arrow(painter: &Painter, from: Pos2, to: Pos2, color: Color32) {
    let dir = to - from;
    let len = dir.length();
    if len < 1.0 {
        return;
    }
    let n = dir / len;
    let size = 9.0_f32;
    let left = to - n * size + Vec2::new(-n.y, n.x) * size * 0.6;
    let right = to - n * size + Vec2::new(n.y, -n.x) * size * 0.6;
    let stroke = egui::Stroke::new(2.0, color);
    painter.line_segment([to, left], stroke);
    painter.line_segment([to, right], stroke);
}
