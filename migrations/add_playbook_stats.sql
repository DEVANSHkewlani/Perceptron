-- Phase 10: Add feedback tracking columns to playbooks table
ALTER TABLE playbooks
  ADD COLUMN IF NOT EXISTS success_count         INTEGER       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS failure_count         INTEGER       DEFAULT 0,
  ADD COLUMN IF NOT EXISTS success_rate          FLOAT         DEFAULT 0.5,
  ADD COLUMN IF NOT EXISTS recommended_action    TEXT,
  ADD COLUMN IF NOT EXISTS updated_at            TIMESTAMPTZ   DEFAULT now();

-- Index for PlaybookUpdater lookups
CREATE INDEX IF NOT EXISTS idx_playbooks_action
  ON playbooks(recommended_action);

-- Phase 10: Add outcome tracking to cognitive_events (corrected table name)
ALTER TABLE cognitive_events
  ADD COLUMN IF NOT EXISTS outcome              TEXT,         -- success|failure|partial|null
  ADD COLUMN IF NOT EXISTS verified_at          TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS anomalies_before     INTEGER,
  ADD COLUMN IF NOT EXISTS anomalies_after      INTEGER,
  ADD COLUMN IF NOT EXISTS updated_at           TIMESTAMPTZ   DEFAULT now();

-- Index for FeedbackConsumer plan_id lookups
CREATE INDEX IF NOT EXISTS idx_cognitive_plan_id
  ON cognitive_events ((payload->>'plan_id'));

-- Seed or update the test playbook with the recommended_action value
UPDATE playbooks
SET recommended_action = 'restart_connection_pool'
WHERE id = 'pb_conn_pool';
