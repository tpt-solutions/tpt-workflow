# tpt-workflow

**Orchestrating the complex, durable logic of your ecosystem.**

`tpt-workflow` is a durable execution and Business Process Management (BPM) engine. While [tpt-synapse](https://github.com/tpt-solutions/tpt-synapse) handles fast, instantaneous event firing, enterprise ecosystems also need long-running, stateful business processes — a permit application, a multi-stage supply chain route, a healthcare intake process — that can take days or weeks to complete.

`tpt-workflow` takes the disparate microservices of your ecosystem and orchestrates them into cohesive, unbreakable workflows that survive server crashes, pause for human approval, and execute complex logic without creating spaghetti code.

## Core Architecture

- **Durable Execution** — Workflows are written as standard code (Rust/TypeScript). The engine automatically persists workflow state at every step, so if a server crashes, `tpt-workflow` replays the workflow from the exact point of failure with no state lost.
- **Visual Workflow Builder** — A drag-and-drop interface for non-developers (e.g. government clerks or hospital admins) to design, test, and modify business processes visually without touching code.
- **Sandboxed Workers** — Isolated environments that execute workflow tasks, so a memory leak or bug in one workflow can't crash the entire engine.

## Tech Stack

- **Workflow Engine Core**: Rust, using deterministic replay and event sourcing (inspired by [Temporal.io](https://temporal.io)'s architecture).
- **Sandboxed Execution**: WebAssembly (WASM) via Wasmtime/Wasmer, to safely run user-defined workflow logic and third-party plugins in strictly isolated memory spaces.
- **Communication**: gRPC & Protobuf for high-performance, strictly typed, low-latency communication between the engine and worker nodes.
- **Visual Builder UI**: [tpt-appfront](https://github.com/tpt-solutions/tpt-appfront), rendering an interactive, infinite-canvas node graph for designing workflows, running natively on desktop and web.

## Ecosystem Integration

- **[tpt-keystone-db](https://github.com/tpt-solutions/tpt-keystone-db)** — durable state store, event-sourcing the workflow history and supporting complex queries over historical workflow data.
- **[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)** — dispatches tasks to workers and listens for asynchronous task completions.
- **[tpt-identity](https://github.com/tpt-solutions/tpt-identity)** — manages step-level permissions (e.g. "Only a Level 3 Manager with the 'Finance' attribute can approve this specific workflow step").

## Status

Early scaffold stage — see [TODO.md](TODO.md) for the phased build-out plan and [spec.txt](spec.txt) for the full design spec.
