//! Protobuf definitions for tpt-workflow

pub mod tpt {
    pub mod workflow {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/tpt.workflow.v1.rs"));
        }
    }
}
