//! Error types for tpt-workflow-sandbox

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("WASM module error: {0}")]
    Module(String),
    
    #[error("WASM execution error: {0}")]
    Execution(String),
    
    #[error("Resource limit exceeded: {0}")]
    ResourceLimit(String),
    
    #[error("Host function error: {0}")]
    HostFunction(String),
    
    #[error("Module not found: {0}")]
    ModuleNotFound(String),
    
    #[error("Function not found: {0}")]
    FunctionNotFound(String),
    
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("Internal error: {0}")]
    Internal(String),
    
    #[error("Not implemented: {0}")]
    NotImplemented(String),
    
    #[error("Timeout: {0}")]
    Timeout(String),
    
    #[error("WASM trap: {0}")]
    Trap(String),
}

pub type Result<T> = std::result::Result<T, Error>;