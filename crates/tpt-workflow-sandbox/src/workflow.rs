use crate::engine::WasmEngine;
use crate::error::{Error, Result};
use crate::module::WasmModule;
use tracing::info;
use wasmtime::Val;

pub struct WasmWorkflowExecutor {
    engine: WasmEngine,
    module: WasmModule,
}

impl WasmWorkflowExecutor {
    pub fn new(engine: WasmEngine, module: WasmModule) -> Self {
        Self { engine, module }
    }

    pub async fn execute_step(
        &self,
        _step_name: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let _input_str = serde_json::to_string(&input)?;
        let args = vec![Val::I32(0); 2];

        let mut instance = self.engine.create_instance_from_module(&self.module)?;
        let results = instance.call_function("execute_step", args).await?;

        if let Some(Val::I32(_output)) = results.first() {
            Ok(serde_json::Value::Null)
        } else {
            Err(Error::Internal("Invalid step output".to_string()))
        }
    }

    pub async fn execute_decision(
        &self,
        _decision_name: &str,
        context: serde_json::Value,
    ) -> Result<bool> {
        let _context_str = serde_json::to_string(&context)?;
        let args = vec![Val::I32(0); 2];

        let mut instance = self.engine.create_instance_from_module(&self.module)?;
        let results = instance.call_function("execute_decision", args).await?;

        if let Some(Val::I32(result)) = results.first() {
            Ok(*result != 0)
        } else {
            Err(Error::Internal("Invalid decision output".to_string()))
        }
    }

    pub fn name(&self) -> &str {
        self.module.name()
    }

    pub fn version(&self) -> &str {
        self.module.version()
    }
}

pub trait WasmWorkflowModule: Send + Sync {
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn execute_step(&self, step_name: &str, input: serde_json::Value) -> Result<serde_json::Value>;
    fn execute_decision(&self, decision_name: &str, context: serde_json::Value) -> Result<bool>;
    fn initialize(&mut self) -> Result<()>;
    fn cleanup(&mut self) -> Result<()>;
}

pub struct DefaultWasmWorkflowModule {
    executor: WasmWorkflowExecutor,
    initialized: bool,
}

impl DefaultWasmWorkflowModule {
    pub fn new(engine: WasmEngine, module: WasmModule) -> Self {
        let executor = WasmWorkflowExecutor::new(engine, module);
        Self {
            executor,
            initialized: false,
        }
    }
}

impl WasmWorkflowModule for DefaultWasmWorkflowModule {
    fn name(&self) -> &str {
        self.executor.name()
    }

    fn version(&self) -> &str {
        self.executor.version()
    }

    fn execute_step(&self, step_name: &str, input: serde_json::Value) -> Result<serde_json::Value> {
        tokio::runtime::Handle::current()
            .block_on(async { self.executor.execute_step(step_name, input).await })
    }

    fn execute_decision(&self, decision_name: &str, context: serde_json::Value) -> Result<bool> {
        tokio::runtime::Handle::current()
            .block_on(async { self.executor.execute_decision(decision_name, context).await })
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
