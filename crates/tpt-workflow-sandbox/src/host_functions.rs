//! Host functions for WASM modules

use crate::error::{Error, Result};
use crate::workflow::{WorkflowExecutionId, StepId, TaskId};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use wasmtime::*;
use tracing::{debug, info, warn};

/// Host function trait
pub trait HostFunction: Send + Sync {
    fn name(&self) -> &str;
    fn call(&self, caller: Caller<'_, ()>, args: Vec<Val>) -> Result<Vec<Val>>;
}

/// Host function registry
pub struct HostFunctionRegistry {
    functions: HashMap<String, Arc<dyn HostFunction>>,
}

impl HostFunctionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };
        
        // Register built-in host functions
        registry.register_builtin_functions();
        registry
    }
    
    /// Register a host function
    pub fn register<F>(&mut self, name: &str, func: F) -> Result<()>
    where
        F: Fn(Caller<'_, ()>, Vec<Val>) -> Result<Vec<Val>> + Send + Sync + 'static,
    {
        struct FuncWrapper<F>(F);
        impl<F> HostFunction for FuncWrapper<F>
        where
            F: Fn(Caller<'_, ()>, Vec<Val>) -> Result<Vec<Val>> + Send + Sync + 'static,
        {
            fn name(&self) -> &str {
                "wrapped"
            }
            
            fn call(&self, caller: Caller<'_, ()>, args: Vec<Val>) -> Result<Vec<Val>> {
                self.0(caller, args)
            }
        }
        
        if self.functions.contains_key(name) {
            return Err(Error::Internal(format!("Host function already registered: {}", name)));
        }
        
        self.functions.insert(name.to_string(), Arc::new(FuncWrapper(func)));
        debug!("Registered host function: {}", name);
        Ok(())
    }
    
    /// Link all functions to a linker
    pub fn link(&self, linker: &mut Linker<()>) -> Result<()> {
        for (name, func) in &self.functions {
            let name = name.clone();
            let func = func.clone();
            
            linker.func_wrap("", &name, move |caller: Caller<'_, ()>, args: Vec<Val>| {
                let result = func.call(caller, args);
                match result {
                    Ok(vals) => vals,
                    Err(e) => {
                        warn!("Host function {} failed: {}", name, e);
                        vec![Val::I32(-1)] // Error indicator
                    }
                }
            })
            .map_err(|e| Error::Internal(format!("Failed to link host function {}: {}", name, e)))?;
        }
        
        Ok(())
    }
    
    /// Get list of registered functions
    pub fn list_functions(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }
    
    /// Register built-in host functions
    fn register_builtin_functions(&mut self) {
        // Logging functions
        self.register("log_info", |_caller, args| {
            if let Some(Val::String(msg)) = args.first() {
                info!("WASM: {}", msg.to_str().unwrap_or(""));
            }
            Ok(vec![])
        }).ok();
        
        self.register("log_warn", |_caller, args| {
            if let Some(Val::String(msg)) = args.first() {
                warn!("WASM: {}", msg.to_str().unwrap_or(""));
            }
            Ok(vec![])
        }).ok();
        
        self.register("log_error", |_caller, args| {
            if let Some(Val::String(msg)) = args.first() {
                tracing::error!("WASM: {}", msg.to_str().unwrap_or(""));
            }
            Ok(vec![])
        }).ok();
        
        // Time functions
        self.register("now", |_caller, _args| {
            let now = chrono::Utc::now().timestamp_millis();
            Ok(vec![Val::I64(now)])
        }).ok();
        
        self.register("sleep", |_caller, args| {
            if let Some(Val::I64(ms)) = args.first() {
                std::thread::sleep(std::time::Duration::from_millis(*ms as u64));
            }
            Ok(vec![])
        }).ok();
        
        // JSON functions
        self.register("json_parse", |_caller, args| {
            if let Some(Val::String(s)) = args.first() {
                match serde_json::from_str::<Value>(s.to_str().unwrap_or("")) {
                    Ok(v) => {
                        let json_str = serde_json::to_string(&v).unwrap_or_default();
                        Ok(vec![Val::String(json_str.into())])
                    }
                    Err(e) => Ok(vec![Val::String(format!("Error: {}", e).into())])
                }
            } else {
                Ok(vec![Val::String("Invalid argument".into())])
            }
        }).ok();
        
        self.register("json_stringify", |_caller, args| {
            if let Some(Val::String(s)) = args.first() {
                match serde_json::from_str::<Value>(s.to_str().unwrap_or("")) {
                    Ok(v) => {
                        let json_str = serde_json::to_string(&v).unwrap_or_default();
                        Ok(vec![Val::String(json_str.into())])
                    }
                    Err(e) => Ok(vec![Val::String(format!("Error: {}", e).into())])
                }
            } else {
                Ok(vec![Val::String("Invalid argument".into())])
            }
        }).ok();
        
        // Workflow context functions
        self.register("get_workflow_id", |caller, _args| {
            // In real implementation, get from caller data
            Ok(vec![Val::String("workflow-123".into())])
        }).ok();
        
        self.register("get_step_id", |caller, _args| {
            Ok(vec![Val::String("step-456".into())])
        }).ok();
        
        self.register("get_task_id", |caller, _args| {
            Ok(vec![Val::String("task-789".into())])
        }).ok();
        
        // State management
        self.register("set_variable", |caller, args| {
            if args.len() >= 2 {
                if let (Some(Val::String(key)), Some(Val::String(value))) = (args.get(0), args.get(1)) {
                    // Store in caller data
                    debug!("Set variable: {} = {}", key.to_str().unwrap_or(""), value.to_str().unwrap_or(""));
                }
            }
            Ok(vec![])
        }).ok();
        
        self.register("get_variable", |caller, args| {
            if let Some(Val::String(key)) = args.first() {
                // Retrieve from caller data
                debug!("Get variable: {}", key.to_str().unwrap_or(""));
                Ok(vec![Val::String("value".into())])
            } else {
                Ok(vec![Val::String("".into())])
            }
        }).ok();
        
        // HTTP client (simplified)
        self.register("http_get", |_caller, args| {
            if let Some(Val::String(url)) = args.first() {
                debug!("HTTP GET: {}", url.to_str().unwrap_or(""));
                // In real implementation, use async HTTP client
                Ok(vec![Val::String(r#"{"status":200,"body":"ok"}"#.into())])
            } else {
                Ok(vec![Val::String(r#"{"error":"Invalid URL"}"#.into())])
            }
        }).ok();
        
        self.register("http_post", |_caller, args| {
            if args.len() >= 2 {
                if let (Some(Val::String(url)), Some(Val::String(body))) = (args.get(0), args.get(1)) {
                    debug!("HTTP POST: {} - {}", url.to_str().unwrap_or(""), body.to_str().unwrap_or(""));
                    Ok(vec![Val::String(r#"{"status":200,"body":"ok"}"#.into())])
                } else {
                    Ok(vec![Val::String(r#"{"error":"Invalid arguments"}"#.into())])
                }
            } else {
                Ok(vec![Val::String(r#"{"error":"Invalid arguments"}"#.into())])
            }
        }).ok();
        
        // Crypto functions
        self.register("sha256", |_caller, args| {
            if let Some(Val::String(data)) = args.first() {
                use sha2::{Sha256, Digest};
                let mut hasher = Sha256::new();
                hasher.update(data.to_str().unwrap_or("").as_bytes());
                let result = format!("{:x}", hasher.finalize());
                Ok(vec![Val::String(result.into())])
            } else {
                Ok(vec![Val::String("".into())])
            }
        }).ok();
        
        self.register("uuid_generate", |_caller, _args| {
            let uuid = uuid::Uuid::new_v4().to_string();
            Ok(vec![Val::String(uuid.into())])
        }).ok();
        
        // Math functions
        self.register("random", |_caller, args| {
            let min = args.get(0).and_then(|v| match v { Val::I64(n) => Some(*n), _ => None }).unwrap_or(0);
            let max = args.get(1).and_then(|v| match v { Val::I64(n) => Some(*n), _ => None }).unwrap_or(100);
            let val = rand::random::<i64>() % (max - min + 1) + min;
            Ok(vec![Val::I64(val)])
        }).ok();
        
        // Task queue functions
        self.register("schedule_task", |_caller, args| {
            if args.len() >= 3 {
                if let (Some(Val::String(queue)), Some(Val::String(task_type)), Some(Val::String(input))) = 
                    (args.get(0), args.get(1), args.get(2)) {
                    debug!("Schedule task: queue={}, type={}, input={}", 
                        queue.to_str().unwrap_or(""), task_type.to_str().unwrap_or(""), input.to_str().unwrap_or(""));
                    let task_id = uuid::Uuid::new_v4().to_string();
                    Ok(vec![Val::String(task_id.into())])
                } else {
                    Ok(vec![Val::String("".into())])
                }
            } else {
                Ok(vec![Val::String("".into())])
            }
        }).ok();
        
        self.register("complete_task", |_caller, args| {
            if args.len() >= 2 {
                if let (Some(Val::String(task_token)), Some(Val::String(output))) = (args.get(0), args.get(1)) {
                    debug!("Complete task: token={}, output={}", 
                        task_token.to_str().unwrap_or(""), output.to_str().unwrap_or(""));
                    Ok(vec![Val::I32(1)])
                } else {
                    Ok(vec![Val::I32(0)])
                }
            } else {
                Ok(vec![Val::I32(0)])
            }
        }).ok();
    }
}

impl Default for HostFunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrap a function for use as a WASM host function
pub fn wrap_host_func<F, Args, Rets>(func: F) -> Func
where
    F: Fn(Args) -> Rets + Send + Sync + 'static,
    Args: WasmParams,
    Rets: WasmResults,
{
    // This is a placeholder - actual implementation would use wasmtime's Func::wrap
    unimplemented!("Use Func::wrap directly with the store")
}

/// Convert WASM values to Rust types
pub fn val_to_string(val: &Val) -> Option<String> {
    match val {
        Val::String(s) => Some(s.to_str().unwrap_or("").to_string()),
        _ => None,
    }
}

pub fn val_to_i64(val: &Val) -> Option<i64> {
    match val {
        Val::I64(n) => Some(*n),
        Val::I32(n) => Some(*n as i64),
        _ => None,
    }
}

pub fn val_to_bool(val: &Val) -> Option<bool> {
    match val {
        Val::I32(n) => Some(*n != 0),
        _ => None,
    }
}

/// Convert Rust types to WASM values
pub fn string_to_val(s: String) -> Val {
    Val::String(s.into())
}

pub fn i64_to_val(n: i64) -> Val {
    Val::I64(n)
}

pub fn bool_to_val(b: bool) -> Val {
    Val::I32(if b { 1 } else { 0 })
}