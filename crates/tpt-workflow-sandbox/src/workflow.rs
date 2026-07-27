//! Workflow execution in WASM sandbox

use crate::engine::{WasmEngine, WasmEngineConfig, WasmInstance};
use crate::error::{Error, Result};
use crate::limits::ResourceLimits;
use crate::module::WasmModule;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// WASM workflow executor
pub struct WasmWorkflowExecutor {
    engine: WasmEngine,
    module: WasmModule,
}

impl WasmWorkflowExecutor {
    /// Create a new workflow executor
    pub async fn new(module: WasmModule, config: WasmEngineConfig) -> Result<Self> {
        let engine = WasmEngine::new(config)?;
        Ok(Self { engine, module })
    }
    
    /// Execute a workflow step
    pub async fn execute_step(&self, step_name: &str, input: serde_json::Value) -> Result<serde_json::Value> {
        let mut instance = self.engine.create_instance(&self.module)?;
        
        // Prepare arguments
        let input_str = serde_json::to_string(&input)?;
        let args = vec![
            wasmtime::Val::String(step_name.into()),
            wasmtime::Val::String(input_str.into()),
        ];
        
        // Call the step function
        let results = instance.call_function("execute_step", args).await?;
        
        // Parse result
        if let Some(wasmtime::Val::String(output)) = results.first() {
            let output_str = output.to_str().unwrap_or("{}");
            serde_json::from_str(output_str)
                .map_err(|e| Error::Internal(format!("Failed to parse step output: {}", e)))
        } else {
            Err(Error::Internal("Invalid step output".to_string()))
        }
    }
    
    /// Execute a workflow decision
    pub async fn execute_decision(&self, decision_name: &str, context: serde_json::Value) -> Result<bool> {
        let mut instance = self.engine.create_instance(&self.module)?;
        
        let context_str = serde_json::to_string(&context)?;
        let args = vec![
            wasmtime::Val::String(decision_name.into()),
            wasmtime::Val::String(context_str.into()),
        ];
        
        let results = instance.call_function("execute_decision", args).await?;
        
        if let Some(wasmtime::Val::I32(result)) = results.first() {
            Ok(*result != 0)
        } else {
            Err(Error::Internal("Invalid decision output".to_string()))
        }
    }
}

/// Workflow module trait for WASM modules
pub trait WasmWorkflowModule: Send + Sync {
    /// Get the module name
    fn name(&self) -> &str;
    
    /// Get the module version
    fn version(&self) -> &str;
    
    /// Execute a step
    fn execute_step(&self, step_name: &str, input: serde_json::Value) -> Result<serde_json::Value>;
    
    /// Execute a decision
    fn execute_decision(&self, decision_name: &str, context: serde_json::Value) -> Result<bool>;
    
    /// Initialize the module
    fn initialize(&mut self) -> Result<()>;
    
    /// Cleanup the module
    fn cleanup(&mut self) -> Result<()>;
}

/// Default implementation using WASM engine
pub struct DefaultWasmWorkflowModule {
    executor: WasmWorkflowExecutor,
    initialized: bool,
}

impl DefaultWasmWorkflowModule {
    pub async fn new(module: WasmModule, config: WasmEngineConfig) -> Result<Self> {
        let executor = WasmWorkflowExecutor::new(module, config).await?;
        Ok(Self {
            executor,
            initialized: false,
        })
    }
}

impl WasmWorkflowModule for DefaultWasmWorkflowModule {
    fn name(&self) -> &str {
        self.executor.module.name()
    }
    
    fn version(&self) -> &str {
        self.executor.module.version()
    }
    
    fn execute_step(&self, step_name: &str, input: serde_json::Value) -> Result<serde_json::Value> {
        // For async functions, we need to use a runtime
        tokio::runtime::Handle::current().block_on(async {
            self.executor.execute_step(step_name, input).await
        })
    }
    
    fn execute_decision(&self, decision_name: &str, context: serde_json::Value) -> Result<bool> {
        tokio::runtime::Handle::current().block_on(async {
            self.executor.execute_decision(decision_name, context).await
        })
    }
    
    fn initialize(&mut self) -> Result<()> {
        if !self.initialized {
            self.initialized = true;
            info!("Initialized WASM workflow module: {}", self.name());
        }
        Ok(())
    }
    
    fn cleanup(&mut self) -> Result<()> {
        if self.initialized {
            self.initialized = false;
            info!("Cleaned up WASM workflow module: {}", self.name());
        }
        Ok(())
    }
}