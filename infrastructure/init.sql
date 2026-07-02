-- Episodic memory: stores every CognitiveEvent forever
CREATE TABLE IF NOT EXISTS cognitive_events (
  event_id      TEXT         NOT NULL,
  timestamp     TIMESTAMPTZ  NOT NULL,
  ingested_at   TIMESTAMPTZ  NOT NULL,
  source_type   TEXT         NOT NULL,
  source_id     TEXT         NOT NULL,
  event_type    TEXT         NOT NULL,
  severity      TEXT         NOT NULL,
  payload       JSONB,
  entity_refs   TEXT[],
  confidence    FLOAT,
  tags          TEXT[],
  outcome       TEXT,
  verified_at   TIMESTAMPTZ,
  anomalies_before INTEGER,
  anomalies_after INTEGER,
  updated_at    TIMESTAMPTZ  DEFAULT NOW(),
  PRIMARY KEY (event_id, timestamp)
);

-- Convert to TimescaleDB hypertable (auto-partitions by time)
SELECT create_hypertable('cognitive_events', 'timestamp', if_not_exists => TRUE);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_events_type_time ON cognitive_events (event_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_source_time ON cognitive_events (source_id,  timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_severity_time ON cognitive_events (severity,   timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_events_entity_refs ON cognitive_events USING GIN (entity_refs);
CREATE INDEX IF NOT EXISTS idx_events_payload ON cognitive_events USING GIN (payload);
CREATE INDEX IF NOT EXISTS idx_cognitive_plan_id ON cognitive_events ((payload->>'plan_id'));

-- Procedural memory: playbooks and strategies
CREATE TABLE IF NOT EXISTS playbooks (
  id              TEXT         PRIMARY KEY,
  name            TEXT         NOT NULL,
  trigger_event   TEXT         NOT NULL,
  trigger_severity TEXT,
  steps           JSONB        NOT NULL,
  success_count   INT          DEFAULT 0,
  failure_count   INT          DEFAULT 0,
  success_rate    FLOAT        DEFAULT 0.5,
  recommended_action TEXT,
  confidence      FLOAT        DEFAULT 0.5,
  created_at      TIMESTAMPTZ  DEFAULT NOW(),
  last_used_at    TIMESTAMPTZ,
  updated_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_playbooks_action ON playbooks (recommended_action);

-- Seed one example playbook
INSERT INTO playbooks (id, name, trigger_event, trigger_severity, steps, confidence)
VALUES (
  'pb_conn_pool',
  'Handle connection pool exhaustion',
  'connection_pool_exhausted',
  'critical',
  '[
    {"step":1,"action":"alert_team","params":{"channel":"#incidents"}},
    {"step":2,"action":"check_slow_queries","params":{"threshold_ms":1000}},
    {"step":3,"action":"restart_connection_pool","params":{"service":"auto"}}
  ]',
  0.85
) ON CONFLICT (id) DO NOTHING;
