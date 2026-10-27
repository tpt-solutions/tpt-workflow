# tpt-workflow TODO

Durable execution / BPM engine. Phases build on each other; later phases may be
reordered once earlier ones reveal constraints.

## Phase 0 — Repo Scaffold
- [ ] README.md (points to spec.txt, summarizes purpose + architecture)
- [ ] CLAUDE.md (AI-agent guidance, points to spec.txt/TODO.md, matches sibling convention)
- [ ] Cargo workspace skeleton (engine core crate, worker-sandbox crate, proto crate)
- [ ] scripts/ci.sh / ci.ps1 (no GitHub Actions, matches tpt-synapse convention)
- [ ] .githooks/ pre-commit (fmt/clippy/test)

## Phase 1 — Durable Execution Core (Rust)
- [ ] Workflow-as-code execution model (deterministic replay semantics)
- [ ] Event-sourcing model: append-only workflow history log, replay-from-event-N
- [ ] Workflow definition API (write workflows as ordinary Rust/TS functions)
- [ ] Crash-recovery: resume workflow from last durably-recorded event
- [ ] Local dev harness: run a workflow end-to-end in-process (no external deps) for testing

## Phase 2 — Sandboxed WASM Workers
- [ ] Wasmtime/Wasmer runtime embedding for user-defined workflow logic + plugins
- [ ] Strict memory isolation per workflow task execution
- [ ] Resource limits (fuel/memory caps) so one bad workflow can't starve the engine
- [ ] WASM module loading/versioning for third-party plugin workflows

## Phase 3 — gRPC & Protobuf Engine↔Worker Protocol
- [ ] Define .proto schemas: task dispatch, task result, heartbeat, cancellation
- [ ] Engine-side gRPC server (task assignment, completion ingestion)
- [ ] Worker-side gRPC client stub/SDK

## Phase 4 — tpt-keystone-db Integration (durable state + history)
- [ ] Postgres wire-protocol client: durable append of workflow event history
- [ ] Postgres-backed historical query API (list/filter past workflow runs)
- [ ] Flux gRPC streaming client: real-time CDC subscription for replay-on-crash / live status
- [ ] Schema/migration for workflow event table(s)

## Phase 5 — tpt-synapse Integration (task dispatch + completion)
- [ ] Define tpt-workflow's task-queue naming convention (workflow_id/task-queue scheme)
- [ ] Integrate against synapse-native-client queue+ack primitive for task dispatch
- [ ] Async completion listening (consumer group per task queue)
- [ ] **[cross-repo, tpt-synapse]** gRPC task-queue facade over queue+ack primitive
- [ ] **[cross-repo, tpt-synapse]** Visibility-timeout + redelivery for crashed/slow workers
- [ ] **[cross-repo, tpt-synapse]** Idempotent ack / dedupe by task token
- [ ] **[cross-repo, tpt-synapse]** Long-poll or streaming pull RPC for worker efficiency

## Phase 6 — Step-Level Permissions (tpt-identity stub)
- [ ] Define AuthorizationCheck trait (workflow step, actor, required attributes/level)
- [ ] Allow-all/no-op default implementation (unblocks everything else)
- [ ] Wire permission check call site into workflow step execution
- [ ] Swap in real tpt-identity client once that repo exists (tracked, not started)

## Phase 7 — Visual Workflow Builder (tpt-appfront) — BLOCKED on appfront upgrade
- [ ] (blocked) Confirm appfront-canvas exposes node-graph/plugin extension points
- [ ] Infinite-canvas node graph view for designing workflows visually
- [ ] Drag-and-drop step/task node palette
- [ ] Visual test/run mode (step through a workflow visually)
- [ ] Save/load: visual graph <-> workflow-as-code representation
