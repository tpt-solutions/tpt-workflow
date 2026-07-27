fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = std::path::PathBuf::from("proto");
    
    prost_build::Config::new()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(".google.protobuf.Timestamp", "::prost_types::Timestamp")
        .extern_path(".google.protobuf.Duration", "::prost_types::Duration")
        .extern_path(".google.protobuf.Struct", "::prost_types::Struct")
        .extern_path(".google.protobuf.Value", "::prost_types::Value")
        .extern_path(".google.protobuf.ListValue", "::prost_types::ListValue")
        .compile_protos(
            &[
                proto_dir.join("tpt/workflow/v1/workflow.proto"),
                proto_dir.join("tpt/workflow/v1/task.proto"),
                proto_dir.join("tpt/workflow/v1/worker.proto"),
                proto_dir.join("tpt/workflow/v1/engine.proto"),
            ],
            &[proto_dir.clone()],
        )?;
    
    println!("cargo:rerun-if-changed=proto/");
    Ok(())
}
