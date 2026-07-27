//! WASM engine for executing sandboxed code

use crate::error::{Error, Result};
use crate::host_functions::HostFunctionRegistry;
use crate::limits::{ResourceLimits, ResourceUsage};
use crate::module::WasmModule;
use std::collections::HashMap;
use std::sync::Arc;
use wasmtime::*;
use tracing::{debug, info, warn};

/// Configuration for WASM engine
#[derive(Debug, Clone)]
pub struct WasmEngineConfig {
    /// Resource limits for each module instance
    pub resource_limits: ResourceLimits,
    /// Whether to enable fuel consumption
    pub enable_fuel: bool,
    /// Initial fuel amount
    pub initial_fuel: u64,
    /// Whether to cache compiled modules
    pub cache_modules: bool,
    /// Maximum number of concurrent instances
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

/// WASM execution engine
pub struct WasmEngine {
    engine: Engine,
    config: WasmEngineConfig,
    module_cache: Arc<parking_lot::RwLock<HashMap<String, Module>>>,
    host_functions: HostFunctionRegistry,
}

impl WasmEngine {
    /// Create a new WASM engine
    pub fn new(config: WasmEngineConfig) -> Result<Self> {
        let mut wasm_config = Config::new();
        
        // Enable fuel if configured
        if config.enable_fuel {
            wasm_config.consume_fuel(true);
        }
        
        // Set resource limits
        wasm_config.max_wasm_stack(config.resource_limits.max_stack_size as u64);
        
        let engine = Engine::new(&wasm_config)
            .map_err(|e| Error::Internal(format!("Failed to create WASM engine: {}", e)))?;
        
        let host_functions = HostFunctionRegistry::new();
        
        Ok(Self {
            engine,
            config,
            module_cache: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            host_functions,
        })
    }
    
    /// Get the underlying wasmtime engine
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
    
    /// Register a host function
    pub fn register_host_function<F>(&mut self, name: &str, func: F) -> Result<()>
    where
        F: Fn(Caller<'_, ()>, Vec<Val>) -> Result<Vec<Val>, Trap> + Send + Sync + 'static,
    {
        self.host_functions.register(name, func)
    }
    
    /// Load a WASM module from bytes
    pub async fn load_module(&self, name: &str, bytes: &[u8]) -> Result<WasmModule> {
        // Check cache first
        if self.config.cache_modules {
            if let Some(module) = self.module_cache.read().get(name) {
                debug!("Using cached module: {}", name);
                return Ok(WasmModule::new(module.clone(), self.engine()));
            }
        }
        
        // Compile module
        let module = Module::new(self.engine(), bytes)
            .map_err(|e| Error::Internal(format!("Failed to compile WASM module: {}", e)))?;
        
        // Cache if enabled
        if self.config.cache_modules {
            self.module_cache.write().insert(name.to_string(), module.clone());
        }
        
        info!("Loaded WASM module: {}", name);
        Ok(WasmModule::new(module, self.engine()))
    }
    
    /// Create a new module instance
    pub fn create_instance(&self, module: &WasmModule) -> Result<WasmInstance> {
        let mut linker = Linker::new(self.engine());
        
        // Add host functions
        self.host_functions.link(&mut linker)?;
        
        // Create store with fuel
        let mut store = Store::new(self.engine(), ());
        if self.config.enable_fuel {
            store.add_fuel(self.config.initial_fuel)
                .map_err(|e| Error::Internal(format!("Failed to add fuel: {}", e)))?;
        }
        
        // Set resource limits
        store.limiter(|_| &self.config.resource_limits);
        
        // Instantiate module
        let instance = linker.instantiate(&mut store, module.module())
            .map_err(|e| Error::Internal(format!("Failed to instantiate WASM module: {}", e)))?;
        
        Ok(WasmInstance {
            store,
            instance,
            module: module.clone(),
        })
    }
    
    /// Execute a function in a module
    pub async fn execute_function(
        &self,
        module_name: &str,
        function_name: &str,
        args: Vec<Val>,
    ) -> Result<Vec<Val>> {
        // Load module if not cached
        let module = if self.config.cache_modules {
            self.module_cache.read().get(module_name).cloned()
        } else {
            None
        };
        
        let module = match module {
            Some(m) => WasmModule::new(m, self.engine()),
            None => return Err(Error::NotImplemented("Module not loaded".to_string())),
        };
        
        let instance = self.create_instance(&module)?;
        instance.call_function(function_name, args).await
    }
}

/// WASM module instance
pub struct WasmInstance {
    store: Store<()>,
    instance: Instance,
    module: WasmModule,
}

impl WasmInstance {
    /// Call a function on the instance
    pub async fn call_function(&mut self, name: &str, args: Vec<Val>) -> Result<Vec<Val>> {
        let func = self.instance.get_func(&mut self.store, name)
            .ok_or_else(|| Error::NotFound(format!("Function not found: {}", name)))?;
        
        let mut results = vec![Val::I32(0); func.ty(&self.store).results().len()];
        
        func.call(&mut self.store, &args, &mut results)
            .map_err(|e| Error::Internal(format!("Function call failed: {}", e)))?;
        
        Ok(results)
    }
    
    /// Get exported memory
    pub fn get_memory(&mut self, name: &str) -> Option<Memory> {
        self.instance.get_memory(&mut self.store, name)
    }
    
    /// Get fuel consumption
    pub fn fuel_consumed(&mut self) -> Result<u64> {
        if self.config.enable_fuel {
            self.store.fuel_consumed()
                .map_err(|e| Error::Internal(format!("Failed to get fuel: {}", e)))
        } else {
            Ok(0)
        }
    }
    
    /// Add more fuel
    pub fn add_fuel(&mut self, fuel: u64) -> Result<()> {
        self.store.add_fuel(fuel)
            .map_err(|e| Error::Internal(format!("Failed to add fuel: {}", e)))
    }
}

/// WASM module wrapper
pub struct WasmModule {
    module: Module,
    engine: Engine,
}

impl WasmModule {
    fn new(module: Module, engine: Engine) -> Self {
        Self { module, engine }
    }
    
    pub fn module(&self) -> &Module {
        &self.module
    }
    
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}

impl Clone for WasmModule {
    fn clone(&self) -> Self {
        Self {
            module: self.module.clone(),
            engine: self.engine.clone(),
        }
    }
}