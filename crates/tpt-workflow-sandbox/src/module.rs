//! WASM module loading, versioning, and management.
//!
//! Modules are addressed by `name@version`. Versions are immutable: once a
//! version is registered its bytes cannot change. The loader is
//! content-addressed, so re-registering identical bytes is idempotent. Version
//! resolution (exact pin, latest, or semver constraint) lives in
//! [`crate::module_version`].

use crate::error::{Error, Result};
use crate::limits::ResourceLimits;
use crate::module_version::{
    content_hash, resolve_version, ApiFingerprint, CompatibilityPolicy, ModuleVersion,
    ModuleVersionInfo, VersionSelector,
};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};
use wasmtime::{Engine, Module};

/// A compiled module plus its version metadata, stored by the loader.
struct LoadedModule {
    module: Arc<Module>,
    info: ModuleVersionInfo,
}

/// Module loader for WASM modules.
///
/// Stores `name -> (version -> LoadedModule)`. `BTreeMap` keeps versions
/// ordered so "latest" resolution is a single `last_key_value` lookup.
pub struct ModuleLoader {
    engine: Engine,
    modules: Arc<parking_lot::RwLock<HashMap<String, BTreeMap<ModuleVersion, LoadedModule>>>>,
}

impl ModuleLoader {
    pub fn new(engine: Engine) -> Self {
        Self {
            engine,
            modules: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    /// Load (or fetch cached) a module from bytes under an exact version.
    ///
    /// Enforces version immutability: if the version already exists and the
    /// bytes hash differs, returns [`Error::VersionConflict`]. Re-registering
    /// identical bytes is idempotent.
    pub async fn load_from_bytes(
        &self,
        name: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<Arc<Module>> {
        let version = ModuleVersion::parse(version)?;
        let hash = content_hash(bytes);

        // Fast path: already loaded with identical bytes.
        {
            let guard = self.modules.read();
            if let Some(versions) = guard.get(name) {
                if let Some(existing) = versions.get(&version) {
                    if existing.info.hash == hash {
                        debug!("Using cached module: {}@{}", name, version);
                        return Ok(existing.module.clone());
                    }
                    return Err(Error::VersionConflict(format!(
                        "module {}@{} already registered with different bytes (existing hash {}, new hash {})",
                        name, version, existing.info.hash, hash
                    )));
                }
            }
        }

        self.validate_module(bytes)?;

        let module = Module::new(&self.engine, bytes)
            .map_err(|e| Error::Internal(format!("Failed to compile WASM module: {}", e)))?;
        let module_arc = Arc::new(module);

        let fingerprint = ApiFingerprint::from_exports_imports(
            &exports_of(&module_arc),
            &imports_of(&module_arc),
        );

        let info = ModuleVersionInfo {
            name: name.to_string(),
            version: version.clone(),
            size_bytes: bytes.len(),
            loaded_at: chrono::Utc::now(),
            hash,
            fingerprint,
        };

        self.modules
            .write()
            .entry(name.to_string())
            .or_default()
            .insert(
                version.clone(),
                LoadedModule {
                    module: module_arc.clone(),
                    info,
                },
            );

        info!(
            "Loaded module: {}@{} ({} bytes)",
            name,
            version,
            bytes.len()
        );
        Ok(module_arc)
    }

    /// Load a module from a file.
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

    /// Get a cached module by exact version.
    pub fn get_module(&self, name: &str, version: &str) -> Option<Arc<Module>> {
        let version = ModuleVersion::parse(version).ok()?;
        self.modules
            .read()
            .get(name)
            .and_then(|versions| versions.get(&version))
            .map(|m| m.module.clone())
    }

    /// Resolve a module version from a [`VersionSelector`].
    pub fn resolve_module(&self, name: &str, selector: &VersionSelector) -> Option<Arc<Module>> {
        let guard = self.modules.read();
        let versions = guard.get(name)?;
        let available: Vec<ModuleVersion> = versions.keys().cloned().collect();
        let chosen = resolve_version(&available, selector)?;
        versions.get(&chosen).map(|m| m.module.clone())
    }

    /// Get the latest version of a module (semver-correct, prefers stable).
    pub fn get_latest_version(&self, name: &str) -> Option<ModuleVersion> {
        let guard = self.modules.read();
        let versions = guard.get(name)?;
        let available: Vec<ModuleVersion> = versions.keys().cloned().collect();
        resolve_version(&available, &VersionSelector::Latest)
    }

    /// List all versions of a module, sorted ascending.
    pub fn list_module_versions(&self, name: &str) -> Vec<ModuleVersion> {
        self.modules
            .read()
            .get(name)
            .map(|versions| versions.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Get rich metadata for a specific version.
    pub fn get_module_info(&self, name: &str, version: &str) -> Option<ModuleVersionInfo> {
        let version = ModuleVersion::parse(version).ok()?;
        self.modules
            .read()
            .get(name)
            .and_then(|versions| versions.get(&version))
            .map(|m| m.info.clone())
    }

    /// List metadata for all loaded module versions.
    pub fn list_modules(&self) -> Vec<ModuleVersionInfo> {
        self.modules
            .read()
            .values()
            .flat_map(|versions| versions.values().map(|m| m.info.clone()))
            .collect()
    }

    /// Unload a specific module version. Returns `true` if it was present.
    pub fn unload_module(&self, name: &str, version: &str) -> bool {
        let version = match ModuleVersion::parse(version) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let mut guard = self.modules.write();
        if let Some(versions) = guard.get_mut(name) {
            let removed = versions.remove(&version).is_some();
            if versions.is_empty() {
                guard.remove(name);
            }
            removed
        } else {
            false
        }
    }

    /// Check whether `candidate` is safe to swap in for the currently-loaded
    /// `existing` version, using the default compatibility policy.
    pub fn is_compatible(&self, name: &str, existing: &str, candidate: &str) -> Result<bool> {
        let existing = ModuleVersion::parse(existing)?;
        let candidate = ModuleVersion::parse(candidate)?;
        let policy = CompatibilityPolicy::default();
        let guard = self.modules.read();
        let fp = |v: &ModuleVersion| {
            guard
                .get(name)
                .and_then(|versions| versions.get(v))
                .map(|m| m.info.fingerprint.clone())
        };
        Ok(policy.is_compatible(
            &existing,
            &candidate,
            fp(&existing).as_ref(),
            fp(&candidate).as_ref(),
        ))
    }

    /// Validate module bytes (magic + WASM version 1).
    fn validate_module(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() < 8 || &bytes[0..4] != b"\0asm" {
            return Err(Error::InvalidInput(
                "Invalid WASM module: missing magic bytes".to_string(),
            ));
        }
        let wasm_version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if wasm_version != 1 {
            return Err(Error::InvalidInput(format!(
                "Unsupported WASM version: {}",
                wasm_version
            )));
        }
        Ok(())
    }
}

fn exports_of(module: &Module) -> Vec<(String, String)> {
    module
        .exports()
        .map(|e| (e.name().to_string(), format!("{:?}", e.ty())))
        .collect()
}

fn imports_of(module: &Module) -> Vec<(String, String, String)> {
    module
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

/// WASM module with metadata.
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

    /// Create an instance of this module.
    pub fn instantiate(
        &self,
        store: &mut wasmtime::Store<()>,
        linker: &wasmtime::Linker<()>,
    ) -> Result<wasmtime::Instance> {
        linker
            .instantiate(store, &self.module)
            .map_err(|e| Error::Internal(format!("Failed to instantiate module: {}", e)))
    }

    /// Get exports as `(name, type)` pairs.
    pub fn exports(&self) -> Vec<(String, String)> {
        exports_of(&self.module)
    }

    /// Get imports as `(module, name, type)` triples.
    pub fn imports(&self) -> Vec<(String, String, String)> {
        imports_of(&self.module)
    }
}

/// Registry for managing multiple modules with shared default resource limits.
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

    pub fn loader(&self) -> &ModuleLoader {
        &self.loader
    }

    /// Register a module, returning a handle with the default resource limits.
    pub async fn register(&self, name: &str, version: &str, bytes: &[u8]) -> Result<WasmModule> {
        let module = self.loader.load_from_bytes(name, version, bytes).await?;
        Ok(WasmModule::new(
            module,
            name.to_string(),
            normalize_version(version)?,
            self.default_limits.clone(),
        ))
    }

    /// Register a module with custom resource limits.
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
            normalize_version(version)?,
            limits,
        ))
    }

    /// Get a module by exact version.
    pub fn get(&self, name: &str, version: &str) -> Option<WasmModule> {
        self.loader.get_module(name, version).map(|m| {
            WasmModule::new(
                m,
                name.to_string(),
                normalize_version(version).unwrap_or_else(|_| version.to_string()),
                self.default_limits.clone(),
            )
        })
    }

    /// Resolve a module version via a selector.
    pub fn get_selector(&self, name: &str, selector: &VersionSelector) -> Option<WasmModule> {
        let module = self.loader.resolve_module(name, selector)?;
        let version = match selector {
            VersionSelector::Exact(v) => v.to_string(),
            _ => self
                .loader
                .get_latest_version(name)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "latest".to_string()),
        };
        Some(WasmModule::new(
            module,
            name.to_string(),
            version,
            self.default_limits.clone(),
        ))
    }

    /// Get the latest version of a module.
    pub fn get_latest(&self, name: &str) -> Option<WasmModule> {
        let version = self.loader.get_latest_version(name)?;
        self.get(name, &version.to_string())
    }

    /// List all module versions.
    pub fn list(&self) -> Vec<ModuleVersionInfo> {
        self.loader.list_modules()
    }

    /// Unload a module version.
    pub fn unload(&self, name: &str, version: &str) -> bool {
        self.loader.unload_module(name, version)
    }
}

/// Normalize a version string into its canonical form (used for `WasmModule`
/// display). Falls back to the original string if it cannot be parsed.
fn normalize_version(version: &str) -> Result<String> {
    Ok(ModuleVersion::parse(version)?.to_string())
}
