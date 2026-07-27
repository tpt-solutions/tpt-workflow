fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = std::path::PathBuf::from("proto");

    tonic_build::configure()
        .extern_path(".google.protobuf.Timestamp", "::prost_types::Timestamp")
        .extern_path(".google.protobuf.Duration", "::prost_types::Duration")
        .extern_path(".google.protobuf.Struct", "::prost_types::Struct")
        .extern_path(".google.protobuf.Value", "::prost_types::Value")
        .extern_path(".google.protobuf.ListValue", "::prost_types::ListValue")
        .compile(
            &[
                proto_dir.join("tpt/workflow/v1/workflow.proto"),
                proto_dir.join("tpt/workflow/v1/task.proto"),
                proto_dir.join("tpt/workflow/v1/worker.proto"),
                proto_dir.join("tpt/workflow/v1/engine.proto"),
            ],
            std::slice::from_ref(&proto_dir),
        )?;

    println!("cargo:rerun-if-changed=proto/");
    Ok(())
}
