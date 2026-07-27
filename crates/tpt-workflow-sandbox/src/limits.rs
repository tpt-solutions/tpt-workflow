use std::sync::Arc;
use tracing::warn;
use wasmtime::{ResourceLimiter, StoreLimits, StoreLimitsBuilder};

#[derive(Debug, Clone)]
pub struct ResourceLimits {
    pub max_memory: usize,
    pub max_stack_size: usize,
    pub max_tables: usize,
    pub max_table_size: usize,
    pub max_memories: usize,
    pub max_instances: usize,
    pub max_fuel: Option<u64>,
    pub execution_timeout_ms: Option<u64>,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024,
            max_stack_size: 1024 * 1024,
            max_tables: 10,
            max_table_size: 10000,
            max_memories: 1,
            max_instances: 100,
            max_fuel: Some(100_000_000),
            execution_timeout_ms: Some(30_000),
        }
    }
}

impl ResourceLimits {
    pub fn strict() -> Self {
        Self {
            max_memory: 16 * 1024 * 1024,
            max_stack_size: 512 * 1024,
            max_tables: 5,
            max_table_size: 1000,
            max_memories: 1,
            max_instances: 10,
            max_fuel: Some(10_000_000),
            execution_timeout_ms: Some(5_000),
        }
    }

    pub fn relaxed() -> Self {
        Self {
            max_memory: 256 * 1024 * 1024,
            max_stack_size: 4 * 1024 * 1024,
            max_tables: 50,
            max_table_size: 100000,
            max_memories: 5,
            max_instances: 500,
            max_fuel: Some(1_000_000_000),
            execution_timeout_ms: Some(300_000),
        }
    }

    pub fn for_task_type(task_type: &str) -> Self {
        match task_type {
            "cpu_intensive" => Self {
                max_fuel: Some(500_000_000),
                execution_timeout_ms: Some(120_000),
                ..Default::default()
            },
            "io_bound" => Self {
                max_memory: 32 * 1024 * 1024,
                execution_timeout_ms: Some(60_000),
                ..Default::default()
            },
            "quick" => Self {
                max_fuel: Some(1_000_000),
                execution_timeout_ms: Some(1_000),
                ..Default::default()
            },
            _ => Self::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ResourceUsage {
    pub memory_used: usize,
    pub fuel_consumed: u64,
    pub execution_time_ms: u64,
    pub function_calls: u64,
    pub host_calls: u64,
}

pub struct WasmResourceLimiter {
    limits: ResourceLimits,
    usage: Arc<parking_lot::RwLock<ResourceUsage>>,
}

impl WasmResourceLimiter {
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            limits,
            usage: Arc::new(parking_lot::RwLock::new(ResourceUsage::default())),
        }
    }

    pub fn get_usage(&self) -> ResourceUsage {
        self.usage.read().clone()
    }

    pub fn reset_usage(&self) {
        *self.usage.write() = ResourceUsage::default();
    }
}

impl ResourceLimiter for WasmResourceLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        if desired > self.limits.max_memory {
            warn!(
                "Memory limit exceeded: current={}, desired={}, max={}",
                current, desired, self.limits.max_memory
            );
            return Ok(false);
        }
        let mut usage = self.usage.write();
        usage.memory_used = desired;
        Ok(true)
    }

    fn table_growing(
        &mut self,
        current: u32,
        desired: u32,
        _maximum: Option<u32>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        if desired as usize > self.limits.max_table_size {
            warn!(
                "Table size limit exceeded: current={}, desired={}, max={}",
                current, desired, self.limits.max_table_size
            );
            return Ok(false);
        }
        Ok(true)
    }
}

pub fn build_store_limits(limits: &ResourceLimits) -> StoreLimits {
    StoreLimitsBuilder::new()
        .memories(limits.max_memories)
        .tables(limits.max_tables)
        .table_elements(limits.max_table_size as u32)
        .instances(limits.max_instances)
        .build()
}
