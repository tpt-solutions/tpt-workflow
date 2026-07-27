# tpt-workflow TODO

Durable execution / BPM engine. Phases build on each other; later phases may be
reordered once earlier ones reveal constraints.

## Phase 0 — Repo Scaffold
- [x] README.md (points to spec.txt, summarizes purpose + architecture)
- [x] CLAUDE.md (AI-agent guidance, points to spec.txt/TODO.md, matches sibling convention)
- [x] Cargo workspace skeleton (engine core crate, worker-sandbox crate, proto crate)
- [x] scripts/ci.sh / ci.ps1 (no GitHub Actions, matches tpt-synapse convention)
- [x] .githooks/ pre-commit (fmt/clippy/test)

## Phase 1 — Durable Execution Core (Rust)
- [x] Workflow-as-code execution model (deterministic replay semantics) — `workflow.rs`, `execution.rs`
- [x] Event-sourcing model: append-only workflow history log, replay-from-event-N — `history.rs`
- [x] Workflow definition API (write workflows as ordinary Rust/TS functions) — `workflow.rs`
- [x] Crash-recovery: resume workflow from last durably-recorded event — `replay.rs`/`persistence.rs` scaffolded, end-to-end test added
- [x] Local dev harness: run a workflow end-to-end in-process (no external deps) for testing — `dev_harness.rs`

## Phase 2 — Sandboxed WASM Workers
- [x] Wasmtime/Wasmer runtime embedding for user-defined workflow logic + plugins — `engine.rs`, `module.rs`
- [x] Strict memory isolation per workflow task execution — `limits.rs`
- [x] Resource limits (fuel/memory caps) so one bad workflow can't starve the engine — `limits.rs`
- [ ] WASM module loading/versioning for third-party plugin workflows — basic loading in `module.rs`, versioning not yet designed

## Phase 3 — gRPC & Protobuf Engine↔Worker Protocol
- [x] Define .proto schemas: task dispatch, task result, heartbeat, cancellation — `proto/tpt/workflow/v1/*.proto`
- [x] Engine-side gRPC server (task assignment, completion ingestion) — `grpc_server.rs` with tonic service impl
- [x] Worker-side gRPC client stub/SDK — `worker_client.rs` with tonic client wrapper

## Phase 4 — tpt-keystone-db Integration (durable state + history)
- [ ] Postgres wire-protocol client: durable append of workflow event history
- [ ] Postgres-backed historical query API (list/filter past workflow runs)
- [ ] Flux gRPC streaming client: real-time CDC subscription for replay-on-crash / live status
- [ ] Schema/migration for workflow event table(s)

## Phase 5 — tpt-synapse Integration (task dispatch + completion)
- [ ] Define tpt-workflow's task-queue naming convention (workflow_id/task-queue scheme)
- [ ] Integrate against synapse-native-client queue+ack primitive for task dispatch
- [ ] Async completion listening (consumer group per task queue)
- [x] **[cross-repo, tpt-synapse]** gRPC task-queue facade over queue+ack primitive — already exists: `tpt-synapse/tpt-workflow/` companion crate, `MatchingService` in `service.rs`
- [x] **[cross-repo, tpt-synapse]** Visibility-timeout + redelivery for crashed/slow workers — `TaskQueueManager` in `dispatch.rs`
- [x] **[cross-repo, tpt-synapse]** Idempotent ack / dedupe by task token — `AckResult::Duplicate` in `dispatch.rs`
- [x] **[cross-repo, tpt-synapse]** Long-poll or streaming pull RPC for worker efficiency — `PollTask` / `StreamTasks` in `service.rs`
  > tpt-workflow should depend on/vendor `tpt-synapse/tpt-workflow` as the gRPC **client** target rather than reimplement — the deleted stray `src/server.rs` was apparently copy-pasted from this crate's server side by mistake.

## Phase 6 — Step-Level Permissions (built internally, not tpt-identity)
- [x] Define AuthorizationCheck trait (workflow step, actor, required attributes/level) — `permissions.rs`
- [x] Allow-all/no-op default implementation (unblocks everything else) — `AllowAllAuth`
- [x] Wire permission check call site into workflow step execution — `execution.rs`
- [x] Build a real internal authorization implementation (role/attribute/clearance-level rules engine) in `tpt-workflow-core` — decided against depending on tpt-identity: it's DID/VC/OIDC/consent-focused and has no actor+resource->allow/deny decision API of the shape we need
- [x] Define storage/config for role & attribute assignments (in-memory first, matching Phase 1 conventions) — `ActorStore` in `permissions.rs`
- [ ] ~~Swap in real tpt-identity client~~ — dropped; revisit only if tpt-identity later grows a policy-decision API

## Phase 7 — Visual Workflow Builder — DEFERRED (build via appfront's node-graph escape hatch, not the main canvas, by design)
- [x] Confirm appfront-canvas exposes node-graph/plugin extension points — it does not, and by design: pan/zoom/drag/node-graph primitives exist and work in `tpt-appfront/examples/node-graph/src/main.rs` (`world_to_screen`/`screen_to_world`, pan-drag, per-node drag, port-to-port wiring), but deliberately built outside `tpt-appfront-canvas`'s `UITree`/taffy pipeline. This is a permanent architectural boundary in appfront, not a temporary gap: `UITree`/`NodeMeta` is one shared AST consumed portably by `-dom`/`-html`/`-ai-schema`/`-tui`/`-canvas`, and world-space/absolute positioning would only ever be honored by `-canvas` — a backend-specific leak into the shared core that breaks that portability. There is no planned `Canvas`/`Absolute` layout mode upstream, and none should be expected.
- [ ] Vendor/fork the node-graph example's transform/drag/wiring logic (`world_to_screen`/`screen_to_world`, pan-drag, per-node drag, port-to-port wire creation) directly into a new tpt-workflow builder crate — own it there, same as appfront's own example does, no dependency on appfront core changing
- [ ] Build a workflow-specific node-graph domain model on top of that (step/task nodes with typed ports, edges as real step-to-step connections) — replacing the example's toy `Node{id,label,pos}`/`Wire{from,to}` structs
- [ ] Drag-and-drop step/task node palette
- [ ] Visual test/run mode (step through a workflow visually)
- [ ] Save/load: visual graph <-> workflow-as-code representation
