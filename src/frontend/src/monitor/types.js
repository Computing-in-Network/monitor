export const MONITOR_SCHEMA_VERSION = 'monitor.v1';

export const MONITOR_EVENT_KIND = Object.freeze({
  NODE_METRIC: 'node_metric',
  LINK_METRIC: 'link_metric',
  FLOW: 'flow',
  ALARM: 'alarm'
});

export const MONITOR_KIND_ALIAS = Object.freeze({
  'node-metric': MONITOR_EVENT_KIND.NODE_METRIC,
  node_metric: MONITOR_EVENT_KIND.NODE_METRIC,
  'link-metric': MONITOR_EVENT_KIND.LINK_METRIC,
  link_metric: MONITOR_EVENT_KIND.LINK_METRIC,
  flow: MONITOR_EVENT_KIND.FLOW,
  alarm: MONITOR_EVENT_KIND.ALARM
});

export const MONITOR_ERROR_CODE = Object.freeze({
  OK: 'OK',
  DUPLICATE: 'DUPLICATE',
  INVALID_PAYLOAD: 'INVALID_PAYLOAD',
  INVALID_KIND: 'INVALID_KIND',
  UNAUTHORIZED: 'UNAUTHORIZED',
  NATS_UNAVAILABLE: 'NATS_UNAVAILABLE',
  EPOCH_MAPPING_NOT_FOUND: 'EPOCH_MAPPING_NOT_FOUND',
  UNKNOWN_NODE_UID: 'UNKNOWN_NODE_UID',
  UNKNOWN_LINK_UID: 'UNKNOWN_LINK_UID'
});

export function normalizeMonitorKind(kind) {
  return MONITOR_KIND_ALIAS[kind] || null;
}

export function isValidSchemaVersion(schemaVersion) {
  return schemaVersion === MONITOR_SCHEMA_VERSION;
}
