-- tpt-workflow durable state schema (Phase 4: tpt-keystone-db integration).
-- Applied via sqlx migrations on the Postgres wire-protocol client.

-- One row per (workflow definition id, version).
CREATE TABLE IF NOT EXISTS workflow_definitions (
    id                   TEXT NOT NULL,
    version              TEXT NOT NULL,
    name                 TEXT NOT NULL,
    description          TEXT NOT NULL DEFAULT '',
    steps                JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL,
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    search_attributes    JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, version)
);

CREATE INDEX IF NOT EXISTS idx_wf_def_name ON workflow_definitions (name);

-- One row per workflow execution run.
CREATE TABLE IF NOT EXISTS workflow_executions (
    workflow_id                  TEXT PRIMARY KEY,
    workflow_definition_id       TEXT NOT NULL,
    workflow_definition_version  TEXT NOT NULL,
    status                       TEXT NOT NULL,
    input                        JSONB NOT NULL DEFAULT '{}'::jsonb,
    output                       JSONB NOT NULL DEFAULT '{}'::jsonb,
    search_attributes            JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata                     JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at                   TIMESTAMPTZ NOT NULL,
    completed_at                 TIMESTAMPTZ,
    updated_at                   TIMESTAMPTZ NOT NULL,
    error_message                TEXT,
    error_type                   TEXT,
    history_length               BIGINT NOT NULL DEFAULT 0,
    history_size_bytes           BIGINT NOT NULL DEFAULT 0,
    parent_workflow_id           TEXT,
    parent_step_id               TEXT,
    current_step                 TEXT
);

CREATE INDEX IF NOT EXISTS idx_wf_exec_def ON workflow_executions (workflow_definition_id);
CREATE INDEX IF NOT EXISTS idx_wf_exec_status ON workflow_executions (status);
CREATE INDEX IF NOT EXISTS idx_wf_exec_started ON workflow_executions (started_at DESC);

-- Append-only workflow history log. Shared by both the PersistenceLayer
-- (WorkflowEvent) and the HistoryStore (HistoryEvent), which are structurally
-- identical once scoped by workflow_id.
CREATE TABLE IF NOT EXISTS workflow_events (
    workflow_id  TEXT NOT NULL,
    event_id     BIGINT NOT NULL,
    event_type   TEXT NOT NULL,
    timestamp    TIMESTAMPTZ NOT NULL,
    step_id      TEXT,
    task_id      TEXT,
    attributes   JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (workflow_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_wf_events_wf ON workflow_events (workflow_id, event_id);

-- Arbitrary task state blobs (used by the persistence layer).
CREATE TABLE IF NOT EXISTS task_states (
    task_id  TEXT PRIMARY KEY,
    state    JSONB NOT NULL DEFAULT '{}'::jsonb
);
