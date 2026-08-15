//! Draggable palette of step types the user can drop onto the canvas.

use tpt_workflow_core::workflow::StepType;

/// Step types offered in the palette, in display order.
pub const PALETTE: &[StepType] = &[
    StepType::Task,
    StepType::Decision,
    StepType::Parallel,
    StepType::Sequence,
    StepType::Wait,
    StepType::Signal,
    StepType::ChildWorkflow,
    StepType::ContinueAsNew,
];

/// Human-readable label for a palette entry.
pub fn palette_label(t: StepType) -> &'static str {
    match t {
        StepType::Task => "Task",
        StepType::Decision => "Decision",
        StepType::Parallel => "Parallel",
        StepType::Sequence => "Sequence",
        StepType::Wait => "Wait",
        StepType::Signal => "Signal",
        StepType::ChildWorkflow => "Child Workflow",
        StepType::ContinueAsNew => "Continue As New",
    }
}
