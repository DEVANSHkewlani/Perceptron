-- Phase 11: Task delegation table
CREATE TABLE IF NOT EXISTS agent_tasks (
    id             TEXT        PRIMARY KEY,
    agent_id       TEXT        NOT NULL,
    plan_id        TEXT,
    action         TEXT        NOT NULL,
    parameters     JSONB       DEFAULT '{}',
    status         TEXT        NOT NULL DEFAULT 'pending',
    priority       INTEGER     DEFAULT 5,
    created_at     TIMESTAMPTZ DEFAULT now(),
    assigned_at    TIMESTAMPTZ,
    completed_at   TIMESTAMPTZ,
    result         JSONB
);

CREATE INDEX IF NOT EXISTS idx_agent_tasks_agent_status
    ON agent_tasks(agent_id, status);

CREATE INDEX IF NOT EXISTS idx_agent_tasks_plan
    ON agent_tasks(plan_id);

-- Phase 11: Add agent_id to cognitive_events for namespace routing
ALTER TABLE cognitive_events
    ADD COLUMN IF NOT EXISTS agent_id TEXT;

CREATE INDEX IF NOT EXISTS idx_episodic_agent
    ON cognitive_events(agent_id);
