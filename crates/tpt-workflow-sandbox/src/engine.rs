use crate::error::{Error, Result};
use crate::host_functions::HostFunctionRegistry;
use crate::limits::ResourceLimits;
use crate::module::{ModuleLoader, WasmModule};
use tracing::info;
use wasmtime::*;

#[derive(Debug, Clone)]
pub struct WasmEngineConfig {
    pub resource_limits: ResourceLimits,
    pub enable_fuel: bool,
    pub initial_fuel: u64,
    pub cache_modules: bool,
    pub max_instances: usize,
}

impl Default for WasmEngineConfig {
    fn default() -> Self {
        Self {
            resource_limits: ResourceLimits::default(),
            enable_fuel: true,
            initial_fuel: 10_000_000,
            cache_modules: true,
            max_instances: 100,
        }
    }
}

pub struct WasmEngine {
    engine: Engine,
    config: WasmEngineConfig,
    module_loader: ModuleLoader,
    host_functions: HostFunctionRegistry,
}

impl WasmEngine {
    pub fn new(config: WasmEngineConfig) -> Result<Self> {
        let mut wasm_config = Config::new();

        if config.enable_fuel {
            wasm_config.consume_fuel(true);
        }

        wasm_config.max_wasm_stack(config.resource_limits.max_stack_size);

        let engine = Engine::new(&wasm_config)
            .map_err(|e| Error::Internal(format!("Failed to create WASM engine: {}", e)))?;

        let host_functions = HostFunctionRegistry::new();
        let module_loader = ModuleLoader::new(engine.clone());

        Ok(Self {
            engine,
            config,
            module_loader,
            host_functions,
        })
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn register_host_function<F>(&mut self, name: &str, func: F) -> Result<()>
    where
        F: Fn(&[Val], &mut [Val]) -> std::result::Result<(), wasmtime::Error>
            + Send
            + Sync
            + 'static,
    {
        self.host_functions.register(name, func)
    }

    pub async fn load_module(&self, name: &str, version: &str, bytes: &[u8]) -> Result<WasmModule> {
        let module = self
            .module_loader
            .load_from_bytes(name, version, bytes)
            .await?;

        info!("Loaded WASM module: {} v{}", name, version);
        Ok(WasmModule::new(
            module,
            name.to_string(),
            version.to_string(),
            self.config.resource_limits.clone(),
        ))
    }

    pub fn create_instance(&self, module: &wasmtime::Module) -> Result<WasmInstance> {
        let mut linker = Linker::new(&self.engine);

        self.host_functions.link(&mut linker)?;

        let mut store = Store::new(&self.engine, ());

        let instance = linker
            .instantiate(&mut store, module)
            .map_err(|e| Error::Internal(format!("Failed to instantiate WASM module: {}", e)))?;

        Ok(WasmInstance { store, instance })
    }

    pub fn create_instance_from_module(&self, wasm_module: &WasmModule) -> Result<WasmInstance> {
        self.create_instance(wasm_module.module())
    }

    pub async fn execute_function(
        &self,
        module_name: &str,
        version: &str,
        function_name: &str,
        args: Vec<Val>,
    ) -> Result<Vec<Val>> {
        let module = self
            .module_loader
            .get_module(module_name, version)
            .ok_or_else(|| {
                Error::ModuleNotFound(format!("Module {}@{} not loaded", module_name, version))
            })?;

        let mut instance = self.create_instance(&module)?;
        instance.call_function(function_name, args).await
    }

    pub fn module_loader(&self) -> &ModuleLoader {
        &self.module_loader
    }
}

pub struct WasmInstance {
    store: Store<()>,
    instance: Instance,
}

impl WasmInstance {
    pub async fn call_function(&mut self, name: &str, args: Vec<Val>) -> Result<Vec<Val>> {
        let func = self
            .instance
            .get_func(&mut self.store, name)
            .ok_or_else(|| Error::FunctionNotFound(format!("Function not found: {}", name)))?;

        let mut results = vec![Val::I32(0); func.ty(&self.store).results().len()];

        func.call(&mut self.store, &args, &mut results)
            .map_err(|e| Error::Internal(format!("Function call failed: {}", e)))?;

        Ok(results)
    }

    pub fn get_memory(&mut self, name: &str) -> Option<Memory> {
        self.instance.get_memory(&mut self.store, name)
    }

    pub fn fuel_consumed(&self) -> Result<u64> {
        Ok(0)
    }

    pub fn add_fuel(&mut self, _fuel: u64) -> Result<()> {
        Ok(())
    }
}
