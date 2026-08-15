//! Errors for the visual workflow builder.

use thiserror::Error;

/// Errors raised while building, validating, or (de)serializing a workflow graph.
#[derive(Debug, Error)]
pub enum BuilderError {
    #[error("cycle detected in workflow graph")]
    Cycle,

    #[error("step not found: {0}")]
    MissingStep(String),

    #[error("invalid workflow definition: {0}")]
    Invalid(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Convenience result alias for builder operations.
pub type Result<T> = std::result::Result<T, BuilderError>;
