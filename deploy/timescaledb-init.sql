CREATE SCHEMA IF NOT EXISTS monitor_ts;

CREATE TABLE IF NOT EXISTS monitor_ts.node_metrics (
  ts TIMESTAMPTZ NOT NULL,
  message_id TEXT NOT NULL,
  topology_epoch TEXT,
  node_id TEXT NOT NULL,
  cpu_ratio DOUBLE PRECISION,
  mem_ratio DOUBLE PRECISION,
  conn_count DOUBLE PRECISION,
  tx_bps DOUBLE PRECISION,
  rx_bps DOUBLE PRECISION,
  status TEXT,
  payload JSONB NOT NULL,
  CONSTRAINT uq_node_metrics UNIQUE (ts, node_id, message_id)
);

CREATE TABLE IF NOT EXISTS monitor_ts.link_metrics (
  ts TIMESTAMPTZ NOT NULL,
  message_id TEXT NOT NULL,
  topology_epoch TEXT,
  link_id TEXT NOT NULL,
  src_node_id TEXT,
  dst_node_id TEXT,
  state TEXT,
  loss_rate DOUBLE PRECISION,
  rtt_ms DOUBLE PRECISION,
  jitter_ms DOUBLE PRECISION,
  ber DOUBLE PRECISION,
  snr_db DOUBLE PRECISION,
  payload JSONB NOT NULL,
  CONSTRAINT uq_link_metrics UNIQUE (ts, link_id, message_id)
);

CREATE TABLE IF NOT EXISTS monitor_ts.flows (
  ts TIMESTAMPTZ NOT NULL,
  message_id TEXT NOT NULL,
  topology_epoch TEXT,
  flow_id TEXT NOT NULL,
  src_node_id TEXT,
  dst_node_id TEXT,
  bps DOUBLE PRECISION,
  pkt_rate DOUBLE PRECISION,
  priority INTEGER,
  path JSONB,
  payload JSONB NOT NULL,
  CONSTRAINT uq_flows UNIQUE (ts, flow_id, message_id)
);

CREATE TABLE IF NOT EXISTS monitor_ts.alarms (
  ts TIMESTAMPTZ NOT NULL,
  message_id TEXT NOT NULL,
  topology_epoch TEXT,
  alarm_id TEXT NOT NULL,
  severity TEXT,
  scope_type TEXT,
  scope_id TEXT,
  title TEXT,
  detail TEXT,
  fingerprint TEXT,
  payload JSONB NOT NULL,
  CONSTRAINT uq_alarms UNIQUE (ts, alarm_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_node_metrics_node_id_ts ON monitor_ts.node_metrics (node_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_link_metrics_link_id_ts ON monitor_ts.link_metrics (link_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_flows_flow_id_ts ON monitor_ts.flows (flow_id, ts DESC);
