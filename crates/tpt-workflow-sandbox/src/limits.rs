//! Resource limits for WASM execution

use crate::error::{Error, Result};
use std::sync::Arc;
use wasmtime::{ResourceLimiter, StoreLimits, StoreLimitsBuilder};
use tracing::{debug, warn};

/// Resource limits for WASM modules
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum memory in bytes
    pub max_memory: usize,
    /// Maximum stack size in bytes
    pub max_stack_size: usize,
    /// Maximum number of tables
    pub max_tables: usize,
    /// Maximum table size
    pub max_table_size: usize,
    /// Maximum number of memories
    pub max_memories: usize,
    /// Maximum number of instances
    pub max_instances: usize,
    /// Maximum fuel (instructions)
    pub max_fuel: Option<u64>,
    /// Execution timeout in milliseconds
    pub execution_timeout_ms: Option<u64>,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024,      // 64 MB
            max_stack_size: 1024 * 1024,        // 1 MB
            max_tables: 10,
            max_table_size: 10000,
            max_memories: 1,
            max_instances: 100,
            max_fuel: Some(100_000_000),
            execution_timeout_ms: Some(30_000), // 30 seconds
        }
    }
}

impl ResourceLimits {
    /// Create strict limits for untrusted code
    pub fn strict() -> Self {
        Self {
            max_memory: 16 * 1024 * 1024,       // 16 MB
            max_stack_size: 512 * 1024,         // 512 KB
            max_tables: 5,
            max_table_size: 1000,
            max_memories: 1,
            max_instances: 10,
            max_fuel: Some(10_000_000),
            execution_timeout_ms: Some(5_000),  // 5 seconds
        }
    }
    
    /// Create relaxed limits for trusted code
    pub fn relaxed() -> Self {
        Self {
            max_memory: 256 * 1024 * 1024,      // 256 MB
            max_stack_size: 4 * 1024 * 1024,    // 4 MB
            max_tables: 50,
            max_table_size: 100000,
            max_memories: 5,
            max_instances: 500,
            max_fuel: Some(1_000_000_000),
            execution_timeout_ms: Some(300_000), // 5 minutes
        }
    }
    
    /// Create limits for a specific task type
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

/// Resource usage tracking
#[derive(Debug, Clone, Default)]
pub struct ResourceUsage {
    /// Memory used in bytes
    pub memory_used: usize,
    /// Fuel consumed
    pub fuel_consumed: u64,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Number of function calls
    pub function_calls: u64,
    /// Number of host calls
    pub host_calls: u64,
}

/// Custom resource limiter for wasmtime
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
    fn memory_growing(&mut self, current: usize, desired: usize) -> Result<bool> {
        if desired > self.limits.max_memory {
            warn!("Memory limit exceeded: current={}, desired={}, max={}", 
                current, desired, self.limits.max_memory);
            return Ok(false);
        }
        
        let mut usage = self.usage.write();
        usage.memory_used = desired;
        Ok(true)
    }
    
    fn table_growing(&mut self, current: u32, desired: u32) -> Result<bool> {
        if desired as usize > self.limits.max_table_size {
            warn!("Table size limit exceeded: current={}, desired={}, max={}", 
                current, desired, self.limits.max_table_size);
            return Ok(false);
        }
        Ok(true)
    }
}

/// Build store limits from resource limits
pub fn build_store_limits(limits: &ResourceLimits) -> StoreLimits {
    let mut builder = StoreLimitsBuilder::new();
    
    builder = builder
        .memory_pages(limits.max_memory / 65536)
        .tables(limits.max_tables)
        .table_elements(limits.max_table_size)
        .memories(limits.max_memories)
        .instances(limits.max_instances);
    
    if let Some(fuel) = limits.max_fuel {
        builder = builder.fuel(fuel);
    }
    
    builder.build()
}