use crate::error::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;
use wasmtime::*;

pub trait HostFunction: Send + Sync {
    fn name(&self) -> &str;
    fn call(&self, args: &[Val], results: &mut [Val]) -> std::result::Result<(), wasmtime::Error>;
}

pub struct HostFunctionRegistry {
    functions: HashMap<String, Arc<dyn HostFunction>>,
}

impl HostFunctionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };
        registry.register_builtin_functions();
        registry
    }

    pub fn register<F>(&mut self, name: &str, func: F) -> Result<()>
    where
        F: Fn(&[Val], &mut [Val]) -> std::result::Result<(), wasmtime::Error>
            + Send
            + Sync
            + 'static,
    {
        struct FuncWrapper<F>(F);
        impl<F> HostFunction for FuncWrapper<F>
        where
            F: Fn(&[Val], &mut [Val]) -> std::result::Result<(), wasmtime::Error>
                + Send
                + Sync
                + 'static,
        {
            fn name(&self) -> &str {
                "wrapped"
            }
            fn call(
                &self,
                args: &[Val],
                results: &mut [Val],
            ) -> std::result::Result<(), wasmtime::Error> {
                self.0(args, results)
            }
        }

        if self.functions.contains_key(name) {
            return Err(Error::Internal(format!(
                "Host function already registered: {}",
                name
            )));
        }

        self.functions
            .insert(name.to_string(), Arc::new(FuncWrapper(func)));
        debug!("Registered host function: {}", name);
        Ok(())
    }

    pub fn link(&self, linker: &mut Linker<()>) -> Result<()> {
        for (name, func) in &self.functions {
            let name = name.clone();
            let func = func.clone();

            linker
                .func_new(
                    "",
                    &name,
                    FuncType::new(
                        linker.engine(),
                        std::iter::repeat_n(ValType::I32, 8),
                        std::iter::once(ValType::I32),
                    ),
                    move |_caller, args, results| func.call(args, results),
                )
                .map_err(|e| {
                    Error::Internal(format!("Failed to link host function {}: {}", name, e))
                })?;
        }

        Ok(())
    }

    pub fn list_functions(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    fn register_builtin_functions(&mut self) {
        self.register("log_info", |_args, _results| {
            debug!("WASM log_info called");
            Ok(())
        })
        .ok();

        self.register("log_warn", |_args, _results| {
            debug!("WASM log_warn called");
            Ok(())
        })
        .ok();

        self.register("log_error", |_args, _results| {
            debug!("WASM log_error called");
            Ok(())
        })
        .ok();

        self.register("now", |_args, results| {
            let now = chrono::Utc::now().timestamp_millis();
            results[0] = Val::I64(now);
            Ok(())
        })
        .ok();

        self.register("sleep", |_args, _results| {
            debug!("WASM sleep called (stub)");
            Ok(())
        })
        .ok();

        self.register("json_parse", |_args, results| {
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("json_stringify", |_args, results| {
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("get_workflow_id", |_args, results| {
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("get_step_id", |_args, results| {
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("get_task_id", |_args, results| {
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("set_variable", |_args, _results| {
            debug!("WASM set_variable called (stub)");
            Ok(())
        })
        .ok();

        self.register("get_variable", |_args, results| {
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("http_get", |_args, results| {
            debug!("WASM http_get called (stub)");
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("http_post", |_args, results| {
            debug!("WASM http_post called (stub)");
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("sha256", |_args, results| {
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("uuid_generate", |_args, results| {
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("random", |_args, results| {
            use rand::Rng;
            let val = rand::thread_rng().gen::<i64>();
            results[0] = Val::I64(val);
            Ok(())
        })
        .ok();

        self.register("schedule_task", |_args, results| {
            debug!("WASM schedule_task called (stub)");
            results[0] = Val::I32(0);
            Ok(())
        })
        .ok();

        self.register("complete_task", |_args, results| {
            debug!("WASM complete_task called (stub)");
            results[0] = Val::I32(1);
            Ok(())
        })
        .ok();
    }
}

impl Default for HostFunctionRegistry {
    fn default() -> Self {
        Self::new()
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

pub fn i64_to_val(n: i64) -> Val {
    Val::I64(n)
}

pub fn bool_to_val(b: bool) -> Val {
    Val::I32(if b { 1 } else { 0 })
}
