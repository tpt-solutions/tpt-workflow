//! Protobuf definitions for tpt-workflow

pub mod tpt {
    pub mod workflow {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/tpt.workflow.v1.rs"));
        }
    }
}

/// Vendored gRPC types for the tpt-synapse `MatchingService` task-queue facade.
///
/// tpt-workflow uses these purely as the gRPC **client** target; the server is
/// implemented in the cross-repo `tpt-synapse/tpt-workflow` crate. Generated
/// from `proto/synapse/workflow/v1/workflow.proto`.
pub mod synapse {
    pub mod workflow {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/synapse.workflow.v1.rs"));
        }
    }
}
