import { MONITOR_EVENT_KIND, MONITOR_SCHEMA_VERSION } from './types.js';
import { getMonitorThresholds } from './config.js';

function metricLevel(value, threshold) {
  if (value == null || Number.isNaN(value)) {
    return 'unknown';
  }
  if (value > threshold.critical) {
    return 'critical';
  }
  if (value >= threshold.warning) {
    return 'warning';
  }
  return 'normal';
}

function maxLevel(levels) {
  if (levels.includes('critical')) {
    return 'critical';
  }
  if (levels.includes('warning')) {
    return 'warning';
  }
  if (levels.includes('normal')) {
    return 'normal';
  }
  return 'unknown';
}

function pickFirstNumber(...candidates) {
  for (const value of candidates) {
    if (value == null) {
      continue;
    }
    const n = Number(value);
    if (Number.isFinite(n)) {
      return n;
    }
  }
  return null;
}

function normalizeRatio(...candidates) {
  const value = pickFirstNumber(...candidates);
  if (value == null) {
    return null;
  }
  if (value >= 0 && value <= 1) {
    return value;
  }
  if (value > 1 && value <= 100) {
    return value / 100;
  }
  return null;
}

function normalizeNodeMetric(event, threshold) {
  const nodeId = event.node_id || event.node_uid || event.docker_name || '';
  if (!nodeId) {
    return null;
  }
  const cpuRatio = normalizeRatio(event.cpu_ratio, event.cpu_percent, event.cpu_usage, event.cpu);
  const memRatio = normalizeRatio(event.mem_ratio, event.memory_ratio, event.mem_percent, event.memory_percent, event.mem_usage, event.mem);
  const txBps = pickFirstNumber(event.tx_bps, event.tx_rate_bps, event.tx_rate, event.tx_bytes_per_sec);
  const rxBps = pickFirstNumber(event.rx_bps, event.rx_rate_bps, event.rx_rate, event.rx_bytes_per_sec);
  const levels = [
    metricLevel(cpuRatio, threshold.cpu_ratio),
    metricLevel(memRatio, threshold.mem_ratio)
  ];
  return {
    nodeId,
    nodeUid: event.node_uid || event.docker_name || nodeId,
    topoNodeId: event.topo_node_id || nodeId,
    dockerName: event.docker_name || '',
    dockerIp: event.docker_ip || event.ip || '',
    cpuRatio,
    memRatio,
    txBps,
    rxBps,
    status: event.status || 'UNKNOWN',
    health: maxLevel(levels)
  };
}

function normalizeLinkMetric(event, threshold) {
  const srcNodeId = event.src_node_id || event.src_node_uid || event.src || event.source || '';
  const dstNodeId = event.dst_node_id || event.dst_node_uid || event.dst || event.target || '';
  const linkId = event.link_id || event.link_uid || (srcNodeId && dstNodeId ? `${srcNodeId}<->${dstNodeId}` : '');
  if (!linkId) {
    return null;
  }
  const lossRate = normalizeRatio(event.loss_rate, event.loss_ratio, event.packet_loss_rate, event.loss_percent);
  const rttMs = pickFirstNumber(event.rtt_ms, event.latency_ms, event.delay_ms);
  const jitterMs = pickFirstNumber(event.jitter_ms, event.jitter);
  const levels = [
    metricLevel(lossRate, threshold.loss_rate),
    metricLevel(rttMs, threshold.rtt_ms),
    metricLevel(jitterMs, threshold.jitter_ms)
  ];
  return {
    linkId,
    linkUid: event.link_uid || linkId,
    srcNodeId,
    dstNodeId,
    srcNodeUid: event.src_node_uid || srcNodeId,
    dstNodeUid: event.dst_node_uid || dstNodeId,
    state: event.state || 'UNKNOWN',
    lossRate,
    rttMs,
    jitterMs,
    health: maxLevel(levels)
  };
}

export function createEmptyMonitorSnapshot() {
  return {
    updatedAt: null,
    health: 'unknown',
    nodeCount: 0,
    linkCount: 0,
    flowCount: 0,
    alarmCount: 0,
    criticalAlarmCount: 0,
    warningAlarmCount: 0,
    topAlarms: [],
    byNode: {},
    byLink: {},
    byFlow: {}
  };
}

function upsertAlarm(snapshot, alarm) {
  const severity = alarm.severity || 'info';
  const normalized = {
    id: alarm.alarm_id || alarm.message_id || `alarm-${Date.now()}`,
    severity,
    title: alarm.title || '未命名告警',
    scopeType: alarm.scope_type || 'unknown',
    scopeUid: alarm.scope_uid || '',
    scopeId: alarm.scope_id || '-',
    detail: alarm.detail || '',
    timestamp: alarm.timestamp || new Date().toISOString()
  };
  const exists = snapshot.topAlarms.some((item) => item.id === normalized.id);
  if (!exists) {
    snapshot.topAlarms.unshift(normalized);
  }
  snapshot.topAlarms = snapshot.topAlarms.slice(0, 20);
}

export function applyMonitorEvent(snapshot, inputEvent, options = {}) {
  const threshold = options.thresholds || getMonitorThresholds();
  if (!inputEvent || typeof inputEvent !== 'object') {
    return snapshot;
  }
  const event = { ...inputEvent };
  if (event.schema_version && event.schema_version !== MONITOR_SCHEMA_VERSION) {
    return snapshot;
  }
  const next = {
    ...snapshot,
    byNode: { ...snapshot.byNode },
    byLink: { ...snapshot.byLink },
    byFlow: { ...snapshot.byFlow },
    topAlarms: [...snapshot.topAlarms]
  };
  next.updatedAt = event.timestamp || new Date().toISOString();

  if (event.kind === MONITOR_EVENT_KIND.NODE_METRIC) {
    const normalized = normalizeNodeMetric(event, threshold);
    if (!normalized) {
      return snapshot;
    }
    next.byNode[normalized.nodeId] = normalized;
  }

  if (event.kind === MONITOR_EVENT_KIND.LINK_METRIC) {
    const normalized = normalizeLinkMetric(event, threshold);
    if (!normalized) {
      return snapshot;
    }
    next.byLink[normalized.linkId] = normalized;
  }

  if (event.kind === MONITOR_EVENT_KIND.FLOW) {
    if (!event.flow_id) {
      return snapshot;
    }
    next.byFlow[event.flow_id] = {
      flowId: event.flow_id,
      srcNodeId: event.src_node_id || '',
      dstNodeId: event.dst_node_id || '',
      srcNodeUid: event.src_node_uid || event.src_node_id || '',
      dstNodeUid: event.dst_node_uid || event.dst_node_id || '',
      bps: event.bps ?? null,
      path: Array.isArray(event.path) ? event.path : [],
      priority: event.priority || ''
    };
  }

  if (event.kind === MONITOR_EVENT_KIND.ALARM) {
    upsertAlarm(next, event);
  }

  const nodeList = Object.values(next.byNode);
  const linkList = Object.values(next.byLink);
  const flowList = Object.values(next.byFlow);
  const criticalAlarmCount = next.topAlarms.filter((alarm) => alarm.severity === 'critical').length;
  const warningAlarmCount = next.topAlarms.filter((alarm) => alarm.severity === 'warning').length;

  next.nodeCount = nodeList.length;
  next.linkCount = linkList.length;
  next.flowCount = flowList.length;
  next.alarmCount = next.topAlarms.length;
  next.criticalAlarmCount = criticalAlarmCount;
  next.warningAlarmCount = warningAlarmCount;

  const overallLevels = [
    ...nodeList.map((item) => item.health),
    ...linkList.map((item) => item.health),
    criticalAlarmCount > 0 ? 'critical' : 'normal',
    warningAlarmCount > 0 ? 'warning' : 'normal'
  ];
  next.health = maxLevel(overallLevels);
  return next;
}

export function applyMonitorSnapshot(snapshot, rawSnapshot, options = {}) {
  const threshold = options.thresholds || getMonitorThresholds();
  if (!rawSnapshot || typeof rawSnapshot !== 'object') {
    return snapshot;
  }
  const monitor = rawSnapshot.monitor && typeof rawSnapshot.monitor === 'object'
    ? rawSnapshot.monitor
    : rawSnapshot;

  const next = createEmptyMonitorSnapshot();
  next.updatedAt = monitor.updated_at || new Date().toISOString();

  const nodes = monitor.nodes || {};
  const links = monitor.links || {};
  const alarms = Array.isArray(monitor.alarms) ? monitor.alarms : [];
  const nodeEntries = Array.isArray(nodes) ? nodes : Object.values(nodes);
  const linkEntries = Array.isArray(links) ? links : Object.values(links);

  for (const item of nodeEntries) {
    if (!item) {
      continue;
    }
    const normalized = normalizeNodeMetric(item, threshold);
    if (!normalized) {
      continue;
    }
    next.byNode[normalized.nodeId] = normalized;
  }

  for (const item of linkEntries) {
    if (!item) {
      continue;
    }
    const normalized = normalizeLinkMetric(item, threshold);
    if (!normalized) {
      continue;
    }
    next.byLink[normalized.linkId] = normalized;
  }

  for (const alarm of alarms) {
    upsertAlarm(next, alarm);
  }

  const nodeList = Object.values(next.byNode);
  const linkList = Object.values(next.byLink);
  next.nodeCount = nodeList.length;
  next.linkCount = linkList.length;
  next.flowCount = Object.keys(next.byFlow).length;
  next.alarmCount = next.topAlarms.length;
  next.criticalAlarmCount = next.topAlarms.filter((item) => item.severity === 'critical').length;
  next.warningAlarmCount = next.topAlarms.filter((item) => item.severity === 'warning').length;
  next.health = maxLevel([
    ...nodeList.map((item) => item.health),
    ...linkList.map((item) => item.health),
    next.criticalAlarmCount > 0 ? 'critical' : 'normal',
    next.warningAlarmCount > 0 ? 'warning' : 'normal'
  ]);
  return next;
}
