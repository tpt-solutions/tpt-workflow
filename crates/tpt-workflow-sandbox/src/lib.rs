//! WASM sandbox for tpt-workflow

pub mod engine;
pub mod limits;
pub mod module;
pub mod host_functions;
pub mod error;
pub mod workflow;

pub use engine::{WasmEngine, WasmEngineConfig, WasmInstance, WasmModule};
pub use limits::{ResourceLimits, ResourceUsage, build_store_limits};
pub use module::{ModuleLoader, ModuleRegistry, WasmModule, ModuleVersionInfo};
pub use host_functions::{HostFunctionRegistry, wrap_host_func, val_to_string, val_to_i64, val_to_bool, string_to_val, i64_to_val, bool_to_val};