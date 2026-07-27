# CLAUDE.md - AI Agent Guidance for tpt-workflow

## Project Overview

`tpt-workflow` is a durable execution and Business Process Management (BPM) engine. It orchestrates long-running, stateful business processes that survive server crashes, pause for human approval, and execute complex logic without creating spaghetti code.

## Architecture

### Core Components

1. **tpt-workflow-core** - Durable execution engine with event sourcing, workflow definitions, and replay
2. **tpt-workflow-proto** - Protobuf definitions for gRPC communication
3. **tpt-workflow-sandbox** - WASM sandbox for executing user-defined workflow logic

### Integration Points

- **tpt-keystone-db** - Durable state store (PostgreSQL wire protocol) for workflow event history
- **tpt-synapse** - Task dispatch and async completion via message queues
- **tpt-identity** - Step-level permissions (role/attribute based)
- **tpt-appfront** - Visual workflow builder (blocked on appfront upgrade)

## Key Design Principles

### Deterministic Replay
- Workflows are written as ordinary Rust/TypeScript functions
- Engine automatically persists state at every step
- On crash, workflow replays from last durable event
- No non-deterministic operations allowed in workflow code

### Event Sourcing
- Append-only workflow history log
- Replay from any event N for debugging/recovery
- Events include: workflow started/ended, step started/ended, task scheduled/completed/failed

### Sandboxed Execution
- User code runs in WASM (Wasmtime) with strict resource limits
- Memory isolation per workflow task
- Fuel/memory caps prevent resource starvation

## Development Guidelines

### Code Style
- Use `cargo fmt` and `cargo clippy` before committing
- Follow workspace lint configurations in Cargo.toml
- Use structured logging with `tracing`

### Adding New Features
1. Define protobuf messages in `crates/tpt-workflow-proto/proto/tpt/workflow/v1/`
2. Run `cargo build` to generate Rust code
3. Implement core logic in `crates/tpt-workflow-core`
4. Add gRPC services in `crates/tpt-workflow-proto/proto/tpt/workflow/v1/engine.proto`
5. Write tests in the appropriate crate

### Testing
- Unit tests in each crate
- Integration tests in `tests/` directory (to be created)
- Local dev harness: run workflow end-to-end in-process (no external deps)

### Common Patterns

#### Workflow Definition
```rust
let definition = WorkflowDefinition::new("my-workflow".to_string(), "1.0.0".to_string())
    .add_step(StepDefinition::task(
        StepId::new(), 
        "process-data".to_string(), 
        "queue-name".to_string(), 
        "task-type".to_string()
    ));
```

#### Starting Execution
```rust
let execution = executor.start_workflow(
    WorkflowExecutionId::new(),
    Arc::new(definition),
    input,
).await?;
```

#### Replay for Debugging
```rust
let result = replay_engine.replay(ReplayRequest {
    workflow_id: execution_id,
    mode: ReplayMode::Full,
    new_input: None,
    max_events: None,
}).await?;
```

## File Structure

```
tpt-workflow/
├── Cargo.toml                 # Workspace manifest
├── CLAUDE.md                  # This file
├── spec.txt                   # Design specification
├── TODO.md                    # Phased build plan
├── scripts/
│   ├── ci.sh                  # Unix CI script
│   └── ci.ps1                 # Windows CI script
├── .githooks/
│   └── pre-commit             # Git hooks
└── crates/
    ├── tpt-workflow-core/     # Core engine
    │   ├── src/
    │   │   ├── lib.rs         # Public exports
    │   │   ├── error.rs       # Error types
    │   │   ├── workflow.rs    # Workflow definitions
    │   │   ├── execution.rs   # Execution engine
    │   │   ├── history.rs     # Event store
    │   │   ├── task_queue.rs  # Task queue management
    │   │   ├── worker.rs      # Worker registry
    │   │   ├── persistence.rs # Persistence layer
    │   │   └── replay.rs      # Replay engine
    │   └── Cargo.toml
    ├── tpt-workflow-proto/    # Protobuf definitions
    │   ├── proto/
    │   │   └── tpt/workflow/v1/
    │   │       ├── workflow.proto
    │   │       ├── task.proto
    │   │       ├── worker.proto
    │   │       └── engine.proto
    │   ├── src/lib.rs
    │   ├── build.rs
    │   └── Cargo.toml
    └── tpt-workflow-sandbox/  # WASM sandbox
        ├── src/
        │   ├── lib.rs
        │   ├── engine.rs
        │   ├── limits.rs
        │   ├── module.rs
        │   ├── host_functions.rs
        │   ├── workflow.rs
        │   └── error.rs
        └── Cargo.toml
```

## Phase Progress (from TODO.md)

- [x] Phase 0: Repo Scaffold (Cargo workspace, proto crate, CI scripts)
- [ ] Phase 1: Durable Execution Core
- [ ] Phase 2: Sandboxed WASM Workers
- [ ] Phase 3: gRPC & Protobuf Protocol
- [ ] Phase 4: tpt-keystone-db Integration
- [ ] Phase 5: tpt-synapse Integration
- [ ] Phase 6: Step-Level Permissions
- [ ] Phase 7: Visual Workflow Builder (blocked)

## Commands

```bash
# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Format code
cargo fmt --all

# Lint
cargo clippy --workspace -- -D warnings

# Generate protobuf
cargo build -p tpt-workflow-proto
```

## Notes for AI Agents

- Always read `spec.txt` and `TODO.md` before making changes
- Follow the phased approach - don't skip phases
- Use the workspace dependencies from root Cargo.toml
- Prefer in-memory implementations first (for testing), then add Postgres
- Keep deterministic replay in mind - avoid non-deterministic operations in workflow code
- The sandbox crate is for user-defined logic; core crate is for engine infrastructure