//! Binary entry point for the tpt-workflow visual builder.

fn main() -> eframe::Result {
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "tpt-workflow Visual Builder",
        options,
        Box::new(|_cc| Ok(Box::new(tpt_workflow_builder::BuilderApp::default()))),
    )
}
