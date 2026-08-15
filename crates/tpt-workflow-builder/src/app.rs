//! `eframe` application wiring the canvas, palette, properties panel, run mode,
//! and save/load + export/import of workflow-as-code.

use crate::graph::{GraphModel, PortKind};
use crate::palette::{palette_label, PALETTE};
use crate::run::{NodeRunStatus, Runner};
use crate::viewer::{
    draw_arrow, error_port_world, input_port_world, node_rect, port_screen, success_port_world,
    ViewerState,
};
use egui::{Align2, Button, Color32, FontId, Painter, Pos2, Rect, Sense, Stroke, StrokeKind, Vec2};
use std::collections::{HashMap, HashSet};
use tpt_workflow_core::workflow::{StepId, StepType, WorkflowDefinition};

const BASE_FILL: Color32 = Color32::from_rgb(235, 238, 245);
const SELECTED_FILL: Color32 = Color32::from_rgb(210, 225, 245);
const SUCCESS_COLOR: Color32 = Color32::from_rgb(60, 120, 220);
const ERROR_COLOR: Color32 = Color32::from_rgb(220, 80, 80);

const PALETTE_WIDTH: f32 = 160.0;
const PROPS_WIDTH: f32 = 260.0;

/// The visual workflow builder application state.
pub struct BuilderApp {
    model: GraphModel,
    viewer: ViewerState,
    /// Step type being dragged out of the palette, awaiting drop on the canvas.
    pending_spawn: Option<StepType>,
    selected: Option<StepId>,
    run_mode: bool,
    run_completed: HashSet<StepId>,
    run_status: HashMap<StepId, NodeRunStatus>,
    path: String,
    io_text: String,
    status: String,
}

impl Default for BuilderApp {
    fn default() -> Self {
        let mut m = GraphModel::new();
        let a = m.add_node(StepType::Task, [80.0, 200.0]);
        let b = m.add_node(StepType::Decision, [320.0, 200.0]);
        let c = m.add_node(StepType::Task, [560.0, 200.0]);
        m.add_edge(a.clone(), PortKind::Success, b.clone());
        m.add_edge(b.clone(), PortKind::Success, c.clone());
        Self {
            model: m,
            viewer: ViewerState::new(),
            pending_spawn: None,
            selected: None,
            run_mode: false,
            run_completed: HashSet::new(),
            run_status: HashMap::new(),
            path: "workflow.tpg.json".to_string(),
            io_text: String::new(),
            status: "Ready".to_string(),
        }
    }
}

impl BuilderApp {
    fn reset_run(&mut self) {
        self.run_completed.clear();
        self.run_status.clear();
    }

    fn step_run(&mut self) {
        if let Some(id) = Runner::next_ready(&self.model, &self.run_completed) {
            self.run_completed.insert(id.clone());
            self.run_status.insert(id.clone(), NodeRunStatus::Done);
            self.status = format!("Stepped: {}", id);
        } else {
            self.status = "Run complete".to_string();
        }
    }

    fn port_at_screen(&self, p: Pos2) -> Option<(StepId, PortKind)> {
        for node in &self.model.nodes {
            let sp = port_screen(&self.viewer, node, PortKind::Success);
            if sp.distance(p) < 10.0 {
                return Some((node.id().clone(), PortKind::Success));
            }
            let ep = port_screen(&self.viewer, node, PortKind::Error);
            if ep.distance(p) < 10.0 {
                return Some((node.id().clone(), PortKind::Error));
            }
        }
        None
    }

    fn node_at_screen(&self, p: Pos2) -> Option<StepId> {
        for node in &self.model.nodes {
            let r = node_rect(node);
            let screen_min = self.viewer.world_to_screen(r.min);
            let screen_rect = Rect::from_min_size(screen_min, r.size() * self.viewer.zoom);
            if screen_rect.contains(p) {
                return Some(node.id().clone());
            }
        }
        None
    }

    fn top_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            if ui.button("New").clicked() {
                *self = BuilderApp::default();
            }
            if ui.button("Save").clicked() {
                match self.model.save_graph(&self.path) {
                    Ok(()) => self.status = format!("Saved to {}", self.path),
                    Err(e) => self.status = format!("Save error: {e}"),
                }
            }
            if ui.button("Open").clicked() {
                match GraphModel::load_graph(&self.path) {
                    Ok(m) => {
                        self.model = m;
                        self.reset_run();
                        self.status = format!("Opened {}", self.path);
                    }
                    Err(e) => self.status = format!("Open error: {e}"),
                }
            }
            if ui.button("Export code").clicked() {
                match self.model.to_workflow_definition() {
                    Ok(def) => match serde_json::to_string_pretty(&def) {
                        Ok(s) => {
                            self.io_text = s;
                            self.status = "Exported workflow-as-code".to_string();
                        }
                        Err(e) => self.status = format!("Serialize error: {e}"),
                    },
                    Err(e) => self.status = format!("Export error: {e}"),
                }
            }
            if ui.button("Import code").clicked() {
                match serde_json::from_str::<WorkflowDefinition>(&self.io_text) {
                    Ok(def) => match GraphModel::from_workflow_definition(&def) {
                        Ok(m) => {
                            self.model = m;
                            self.reset_run();
                            self.status = "Imported workflow-as-code".to_string();
                        }
                        Err(e) => self.status = format!("Build error: {e}"),
                    },
                    Err(e) => self.status = format!("Parse error: {e}"),
                }
            }
            ui.separator();
            if ui
                .button(if self.run_mode { "Edit" } else { "Run" })
                .clicked()
            {
                self.run_mode = !self.run_mode;
                if !self.run_mode {
                    self.reset_run();
                }
            }
            if self.run_mode {
                if ui.button("Step").clicked() {
                    self.step_run();
                }
                if ui.button("Run all").clicked() {
                    while Runner::next_ready(&self.model, &self.run_completed).is_some() {
                        self.step_run();
                    }
                }
                if ui.button("Reset").clicked() {
                    self.reset_run();
                }
            }
        });
        ui.horizontal(|ui| {
            ui.label("Path:");
            ui.text_edit_singleline(&mut self.path);
            ui.separator();
            ui.label(&self.status);
        });
    }

    fn left_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Steps");
        ui.label("Drag onto canvas");
        for t in PALETTE {
            let label = palette_label(*t);
            let response = ui.add(Button::new(label).sense(Sense::drag()));
            if response.drag_started() {
                self.pending_spawn = Some(*t);
            }
            if response.clicked() {
                let world = self.viewer.screen_to_world(Pos2::new(400.0, 300.0));
                self.model.add_node(*t, [world.x, world.y]);
                self.status = format!("Added {} node", label);
            }
        }
    }

    fn right_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Properties");
        if let Some(id) = self.selected.clone() {
            if let Some(node) = self.model.node_mut(&id) {
                ui.label("Name:");
                ui.text_edit_singleline(&mut node.step.name);
                ui.label("Task queue:");
                ui.text_edit_singleline(&mut node.step.task_queue);
                ui.label("Task type:");
                ui.text_edit_singleline(&mut node.step.task_type);
                ui.label("Description:");
                ui.text_edit_multiline(&mut node.step.description);
                if ui.button("Delete node").clicked() {
                    self.model.remove_node(&id);
                    self.selected = None;
                }
            } else {
                self.selected = None;
                ui.label("(none)");
            }
        } else {
            ui.label("Select a node to edit");
        }
        ui.separator();
        ui.heading("Workflow-as-code");
        ui.text_edit_multiline(&mut self.io_text);
    }

    fn show_canvas(&mut self, ui: &mut egui::Ui) {
        let rect = ui.clip_rect();
        let painter = ui.painter();
        let pointer = ui.input(|i| i.pointer.interact_pos()).unwrap_or(rect.min);
        let primary_down = ui.input(|i| i.pointer.primary_down());
        let primary_pressed = ui.input(|i| i.pointer.primary_pressed());
        let released = ui.input(|i| i.pointer.any_released());
        let scroll = ui.input(|i| i.smooth_scroll_delta.y);

        // Zoom toward the cursor on scroll.
        if scroll != 0.0 {
            let world_before = self.viewer.screen_to_world(pointer);
            let factor = (1.0_f32 + scroll * 0.001).clamp(0.4, 3.0);
            self.viewer.zoom *= factor;
            let world_after = self.viewer.screen_to_world(pointer);
            self.viewer.pan += (world_after - world_before) * self.viewer.zoom;
        }

        // Palette spawn: drop a new node where the cursor is released.
        let spawn = self.pending_spawn;
        if let Some(step_type) = spawn {
            if released {
                if rect.contains(pointer) {
                    let world = self.viewer.screen_to_world(pointer);
                    self.model.add_node(step_type, [world.x, world.y]);
                    self.status = format!("Added {:?} node", step_type);
                }
                self.pending_spawn = None;
            } else {
                painter.circle_filled(pointer, 6.0, SUCCESS_COLOR);
            }
            render_graph(
                &self.model,
                &self.viewer,
                &self.run_status,
                &self.selected,
                painter,
                pointer,
                rect,
            );
            return;
        }

        // Begin a drag: output port (wire), node body (move), or background (pan).
        if primary_pressed
            && self.viewer.pending_wire.is_none()
            && self.viewer.drag_node.is_none()
            && self.viewer.pan_drag.is_none()
        {
            if let Some((id, port)) = self.port_at_screen(pointer) {
                self.viewer.pending_wire = Some((id, port));
            } else if let Some(id) = self.node_at_screen(pointer) {
                let node = self.model.node(&id).unwrap();
                let world = self.viewer.screen_to_world(pointer);
                self.viewer.drag_node = Some((
                    id.clone(),
                    Vec2::new(world.x - node.pos[0], world.y - node.pos[1]),
                ));
                self.selected = Some(id);
            } else {
                self.viewer.pan_drag = Some(pointer);
                self.selected = None;
            }
        }

        // Apply background pan.
        if let Some(start) = self.viewer.pan_drag {
            if released || !primary_down {
                self.viewer.pan_drag = None;
            } else {
                self.viewer.pan = pointer - start;
            }
        }

        // Apply node body drag.
        if let Some((id, offset)) = self.viewer.drag_node.clone() {
            if released {
                self.viewer.drag_node = None;
            } else if let Some(node) = self.model.node_mut(&id) {
                let world = self.viewer.screen_to_world(pointer) - offset;
                node.pos = [world.x, world.y];
            }
        }

        // Finish a pending wire on release over a target node.
        if released {
            if let Some((from, port)) = self.viewer.pending_wire.take() {
                if let Some(target) = self.node_at_screen(pointer) {
                    if target != from {
                        match self.model.add_edge(from.clone(), port, target.clone()) {
                            Some(_) => self.status = "Connected".to_string(),
                            None => self.status = "Could not connect".to_string(),
                        }
                    }
                }
            }
        }

        render_graph(
            &self.model,
            &self.viewer,
            &self.run_status,
            &self.selected,
            painter,
            pointer,
            rect,
        );
    }
}

impl eframe::App for BuilderApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ui, |ui| {
            ui.vertical(|ui| {
                self.top_bar(ui);
                ui.separator();
                ui.horizontal(|ui| {
                    ui.allocate_ui(Vec2::new(PALETTE_WIDTH, ui.available_height()), |ui| {
                        self.left_panel(ui)
                    });
                    self.show_canvas(ui);
                    ui.allocate_ui(Vec2::new(PROPS_WIDTH, ui.available_height()), |ui| {
                        self.right_panel(ui)
                    });
                });
            });
        });
    }
}

/// Draw the whole graph: wires beneath nodes, then nodes, the in-progress wire,
/// and a status line. Pure read of the model so it can be called from either
/// branch of the canvas handler.
fn render_graph(
    model: &GraphModel,
    viewer: &ViewerState,
    run_status: &HashMap<StepId, NodeRunStatus>,
    selected: &Option<StepId>,
    painter: &Painter,
    pointer: Pos2,
    rect: Rect,
) {
    // Wires (under nodes).
    for edge in &model.edges {
        let (Some(a), Some(b)) = (model.node(&edge.from_step), model.node(&edge.to_step)) else {
            continue;
        };
        let p1 = match edge.from_port {
            PortKind::Success => viewer.world_to_screen(success_port_world(a)),
            PortKind::Error => viewer.world_to_screen(error_port_world(a)),
        };
        let p2 = viewer.world_to_screen(input_port_world(b));
        let color = match edge.from_port {
            PortKind::Success => SUCCESS_COLOR,
            PortKind::Error => ERROR_COLOR,
        };
        painter.line_segment([p1, p2], Stroke::new(2.0, color));
        draw_arrow(painter, p1, p2, color);
    }

    // In-progress wire trails the cursor.
    if let Some((from, port)) = &viewer.pending_wire {
        if let Some(node) = model.node(from) {
            let p1 = match port {
                PortKind::Success => viewer.world_to_screen(success_port_world(node)),
                PortKind::Error => viewer.world_to_screen(error_port_world(node)),
            };
            let color = match port {
                PortKind::Success => SUCCESS_COLOR,
                PortKind::Error => ERROR_COLOR,
            };
            painter.line_segment([p1, pointer], Stroke::new(2.0, color));
        }
    }

    // Nodes.
    for node in &model.nodes {
        let screen_min = viewer.world_to_screen(node_rect(node).min);
        let size = node_rect(node).size() * viewer.zoom;
        let r = Rect::from_min_size(screen_min, size);
        let right_s = viewer.world_to_screen(success_port_world(node));
        let right_e = viewer.world_to_screen(error_port_world(node));
        let left = viewer.world_to_screen(input_port_world(node));

        let fill = if let Some(st) = run_status.get(node.id()) {
            match st {
                NodeRunStatus::Running => Color32::from_rgb(255, 230, 150),
                NodeRunStatus::Done => Color32::from_rgb(180, 230, 180),
                NodeRunStatus::Error => Color32::from_rgb(240, 170, 170),
                NodeRunStatus::Idle => BASE_FILL,
            }
        } else if selected.as_ref() == Some(node.id()) || r.contains(pointer) {
            SELECTED_FILL
        } else {
            BASE_FILL
        };

        painter.rect_filled(r, 6.0 * viewer.zoom, fill);
        painter.rect_stroke(
            r,
            6.0 * viewer.zoom,
            Stroke::new(1.0, Color32::from_gray(110)),
            StrokeKind::Middle,
        );
        painter.text(
            r.min + Vec2::new(10.0 * viewer.zoom, 18.0 * viewer.zoom),
            Align2::LEFT_CENTER,
            &node.step.name,
            FontId::proportional(14.0 * viewer.zoom),
            Color32::BLACK,
        );
        painter.text(
            r.min + Vec2::new(10.0 * viewer.zoom, 36.0 * viewer.zoom),
            Align2::LEFT_CENTER,
            format!("{:?}", node.step.step_type),
            FontId::proportional(11.0 * viewer.zoom),
            Color32::from_gray(90),
        );
        painter.circle_filled(left, 5.0, Color32::from_gray(90));
        painter.circle_filled(right_s, 5.0, SUCCESS_COLOR);
        painter.circle_filled(right_e, 5.0, ERROR_COLOR);
    }

    painter.text(
        rect.min + Vec2::new(10.0, 14.0),
        Align2::LEFT_TOP,
        format!(
            "nodes: {}  edges: {}  zoom: {:.2}x",
            model.nodes.len(),
            model.edges.len(),
            viewer.zoom
        ),
        FontId::proportional(14.0),
        Color32::from_gray(120),
    );
}
