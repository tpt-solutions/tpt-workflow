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
- [x] WASM module loading/versioning for third-party plugin workflows — versioning designed in `module_version.rs`: semver `ModuleVersion`, `VersionSelector` (exact/`latest`/constraint), `resolve_version`, content-addressed `content_hash` (idempotent reload + tamper detection), `ApiFingerprint` + `CompatibilityPolicy` (same-major / identical-surface / declared-group). `ModuleLoader` enforces version immutability and `BTreeMap`-backed latest resolution; `ModuleRegistry::get_selector` resolves by selector.

## Phase 3 — gRPC & Protobuf Engine↔Worker Protocol
- [x] Define .proto schemas: task dispatch, task result, heartbeat, cancellation — `proto/tpt/workflow/v1/*.proto`
- [x] Engine-side gRPC server (task assignment, completion ingestion) — `grpc_server.rs` with tonic service impl
- [x] Worker-side gRPC client stub/SDK — `worker_client.rs` with tonic client wrapper

## Phase 4 — tpt-keystone-db Integration (durable state + history)
- [x] Postgres wire-protocol client: durable append of workflow event history — `PostgresPersistence` + `PostgresHistoryStore` (in `persistence.rs`/`history.rs`, feature `postgres`) append to the `workflow_events` table inside a transaction with server-assigned sequential `event_id`s.
- [x] Postgres-backed historical query API (list/filter past workflow runs) — `get_*`/`list_*` implementations incl. filtered/paginated `list_workflow_executions` (status `ANY(...)`, time ranges) and `list_workflow_definitions`.
- [x] Flux gRPC streaming client: real-time CDC subscription for replay-on-crash / live status — `keystone_flux.rs` (feature `flux`): `FluxCdcClient` trait + `GrpcFluxCdcClient` over a generated `Flux` tonic service (local `flux.proto`, package `tpt.workflow.v1`). NOTE: `flux.proto` message shapes must be aligned with the upstream keystone Flux service (the `tpt-keystone-db` repo is not present locally, so the contract is a scaffold).
- [x] Schema/migration for workflow event table(s) — `migrations/0001_init.sql` (`workflow_definitions`, `workflow_executions`, `workflow_events`, `task_states`); applied via `sqlx::migrate!` from `PostgresPersistence::connect`/`migrate` and `PostgresHistoryStore::connect`/`migrate`.

## Phase 5 — tpt-synapse Integration (task dispatch + completion)
- [x] Define tpt-workflow's task-queue naming convention (workflow_id/task-queue scheme) — `task_queue_naming.rs`: canonical `<scope>/<logical-queue>` where scope is `exec:<execution-id>` or `def:<definition-id>:<version>`; `TaskQueueName` parse/format/validate, `QueueScope`, and `TaskQueueManager::build_queue_name`/`schedule_task_scoped` wired in.
- [x] Integrate against synapse-native-client queue+ack primitive for task dispatch — `synapse.rs`: `SynapseTaskQueue` implements the engine `TaskQueue` over the vendored `MatchingService` gRPC client (`AddTask` / `PollTask` / `RespondTaskCompleted` / `RespondTaskFailed`)
- [x] Async completion listening (consumer group per task queue) — `synapse.rs`: `SynapseTaskCompletionListener` streams `StreamTasks` and acks via a `TaskHandler`; `ConsumerGroup` + `ConsumerGroupRegistry` give a named consumer group per task queue
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
- [x] Vendor/fork the node-graph example's transform/drag/wiring logic (`world_to_screen`/`screen_to_world`, pan-drag, per-node drag, port-to-port wire creation) into a new `tpt-workflow-builder` crate — `crates/tpt-workflow-builder/src/viewer.rs` owns the canvas state + geometry, no dependency on appfront core (matches appfront's own example boundary)
- [x] Build a workflow-specific node-graph domain model on top of that (`graph.rs`: `GraphNode{step,pos}` / `GraphEdge{from_step,from_port,to_step}` with typed `PortKind::{Success,Error}`), replacing the example's toy `Node{id,label,pos}`/`Wire{from,to}` structs
- [x] Drag-and-drop step/task node palette (`app.rs` left panel; drag a `StepType` onto the canvas, or click to drop at view center)
- [x] Visual test/run mode (step through a workflow visually) — `run.rs` walks the graph by the same dependency rule as `core::execution::get_next_step`; `Step`/`Run all`/`Reset` controls highlight nodes Done
- [x] Save/load: visual graph <-> workflow-as-code representation — `convert.rs` `to_workflow_definition`/`from_workflow_definition` (with auto layout + cycle detection) plus `save_graph`/`load_graph` (full graph incl. positions) and Export/Import-code buttons
