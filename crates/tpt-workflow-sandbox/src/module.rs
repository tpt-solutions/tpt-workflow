//! WASM module loading and management

use crate::error::{Error, Result};
use crate::limits::ResourceLimits;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};
use wasmtime::{Engine, Module};

/// Module loader for WASM modules
pub struct ModuleLoader {
    engine: Engine,
    module_cache: Arc<parking_lot::RwLock<HashMap<String, Arc<Module>>>>,
    version_metadata: Arc<parking_lot::RwLock<HashMap<String, ModuleVersionInfo>>>,
}

impl ModuleLoader {
    pub fn new(engine: Engine) -> Self {
        Self {
            engine,
            module_cache: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            version_metadata: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    /// Load a module from bytes
    pub async fn load_from_bytes(
        &self,
        name: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<Arc<Module>> {
        let cache_key = format!("{}@{}", name, version);

        // Check cache
        if let Some(module) = self.module_cache.read().get(&cache_key) {
            debug!("Using cached module: {}", cache_key);
            return Ok(module.clone());
        }

        // Validate module
        self.validate_module(bytes)?;

        // Compile module
        let module = Module::new(&self.engine, bytes)
            .map_err(|e| Error::Internal(format!("Failed to compile WASM module: {}", e)))?;

        let module_arc = Arc::new(module);

        // Cache module
        self.module_cache
            .write()
            .insert(cache_key.clone(), module_arc.clone());

        // Store version metadata
        self.version_metadata.write().insert(
            cache_key.clone(),
            ModuleVersionInfo {
                name: name.to_string(),
                version: version.to_string(),
                size_bytes: bytes.len(),
                loaded_at: chrono::Utc::now(),
                hash: self.compute_hash(bytes),
            },
        );

        info!(
            "Loaded module: {} v{} ({} bytes)",
            name,
            version,
            bytes.len()
        );
        Ok(module_arc)
    }

    /// Load a module from file
    pub async fn load_from_file(
        &self,
        name: &str,
        version: &str,
        path: &Path,
    ) -> Result<Arc<Module>> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read module file: {}", e)))?;
        self.load_from_bytes(name, version, &bytes).await
    }

    /// Get a cached module
    pub fn get_module(&self, name: &str, version: &str) -> Option<Arc<Module>> {
        let cache_key = format!("{}@{}", name, version);
        self.module_cache.read().get(&cache_key).cloned()
    }

    /// Get latest version of a module
    pub fn get_latest_version(&self, name: &str) -> Option<String> {
        let metadata = self.version_metadata.read();
        metadata
            .values()
            .filter(|m| m.name == name)
            .max_by_key(|m| &m.version)
            .map(|m| m.version.clone())
    }

    /// List all loaded modules
    pub fn list_modules(&self) -> Vec<ModuleVersionInfo> {
        self.version_metadata.read().values().cloned().collect()
    }

    /// Unload a module version
    pub fn unload_module(&self, name: &str, version: &str) -> bool {
        let cache_key = format!("{}@{}", name, version);
        self.module_cache.write().remove(&cache_key).is_some()
    }

    /// Validate module bytes
    fn validate_module(&self, bytes: &[u8]) -> Result<()> {
        // Basic validation - check magic bytes
        if bytes.len() < 8 || &bytes[0..4] != b"\0asm" {
            return Err(Error::InvalidInput(
                "Invalid WASM module: missing magic bytes".to_string(),
            ));
        }

        // Check version
        let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if version != 1 {
            return Err(Error::InvalidInput(format!(
                "Unsupported WASM version: {}",
                version
            )));
        }

        Ok(())
    }

    /// Compute hash of module bytes
    fn compute_hash(&self, bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }
}

/// Module version information
#[derive(Debug, Clone)]
pub struct ModuleVersionInfo {
    pub name: String,
    pub version: String,
    pub size_bytes: usize,
    pub loaded_at: chrono::DateTime<chrono::Utc>,
    pub hash: String,
}

/// WASM module with metadata
pub struct WasmModule {
    module: Arc<Module>,
    name: String,
    version: String,
    resource_limits: ResourceLimits,
}

impl WasmModule {
    pub fn new(
        module: Arc<Module>,
        name: String,
        version: String,
        resource_limits: ResourceLimits,
    ) -> Self {
        Self {
            module,
            name,
            version,
            resource_limits,
        }
    }

    pub fn module(&self) -> &Module {
        &self.module
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn resource_limits(&self) -> &ResourceLimits {
        &self.resource_limits
    }

    /// Create an instance of this module
    pub fn instantiate(
        &self,
        store: &mut wasmtime::Store<()>,
        linker: &wasmtime::Linker<()>,
    ) -> Result<wasmtime::Instance> {
        linker
            .instantiate(store, &self.module)
            .map_err(|e| Error::Internal(format!("Failed to instantiate module: {}", e)))
    }

    /// Get exports
    pub fn exports(&self) -> Vec<(String, String)> {
        self.module
            .exports()
            .map(|e| (e.name().to_string(), format!("{:?}", e.ty())))
            .collect()
    }

    /// Get imports
    pub fn imports(&self) -> Vec<(String, String, String)> {
        self.module
            .imports()
            .map(|i| {
                (
                    i.module().to_string(),
                    i.name().to_string(),
                    format!("{:?}", i.ty()),
                )
            })
            .collect()
    }
}

/// Module registry for managing multiple modules
pub struct ModuleRegistry {
    loader: ModuleLoader,
    default_limits: ResourceLimits,
}

impl ModuleRegistry {
    pub fn new(engine: Engine, default_limits: ResourceLimits) -> Self {
        Self {
            loader: ModuleLoader::new(engine),
            default_limits,
        }
    }

    /// Register a module
    pub async fn register(&self, name: &str, version: &str, bytes: &[u8]) -> Result<WasmModule> {
        let module = self.loader.load_from_bytes(name, version, bytes).await?;
        Ok(WasmModule::new(
            module,
            name.to_string(),
            version.to_string(),
            self.default_limits.clone(),
        ))
    }

    /// Register a module with custom limits
    pub async fn register_with_limits(
        &self,
        name: &str,
        version: &str,
        bytes: &[u8],
        limits: ResourceLimits,
    ) -> Result<WasmModule> {
        let module = self.loader.load_from_bytes(name, version, bytes).await?;
        Ok(WasmModule::new(
            module,
            name.to_string(),
            version.to_string(),
            limits,
        ))
    }

    /// Get a module
    pub fn get(&self, name: &str, version: &str) -> Option<WasmModule> {
        self.loader.get_module(name, version).map(|m| {
            WasmModule::new(
                m,
                name.to_string(),
                version.to_string(),
                self.default_limits.clone(),
            )
        })
    }

    /// Get latest version
    pub fn get_latest(&self, name: &str) -> Option<WasmModule> {
        self.loader
            .get_latest_version(name)
            .and_then(|v| self.get(name, &v))
    }

    /// List all modules
    pub fn list(&self) -> Vec<ModuleVersionInfo> {
        self.loader.list_modules()
    }

    /// Unload a module
    pub fn unload(&self, name: &str, version: &str) -> bool {
        self.loader.unload_module(name, version)
    }
}
