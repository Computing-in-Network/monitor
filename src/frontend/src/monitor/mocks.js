import { MONITOR_EVENT_KIND, MONITOR_SCHEMA_VERSION } from './types.js';

function nowIso(step) {
  return new Date(Date.now() + step * 1000).toISOString();
}

export function generateMockMonitorEvents(step = 0) {
  const cpu = 0.45 + 0.35 * Math.abs(Math.sin(step / 7));
  const mem = 0.50 + 0.30 * Math.abs(Math.cos(step / 11));
  const loss = 0.003 + 0.04 * Math.abs(Math.sin(step / 13));
  const rtt = 40 + Math.round(120 * Math.abs(Math.cos(step / 9)));
  const jitter = 8 + Math.round(45 * Math.abs(Math.sin(step / 10)));
  const severity = loss > 0.05 || rtt > 150 ? 'critical' : (loss > 0.01 || rtt > 80 ? 'warning' : 'info');

  const common = {
    schema_version: MONITOR_SCHEMA_VERSION,
    topology_epoch: Math.floor(Date.now() / 1000)
  };

  return [
    {
      ...common,
      kind: MONITOR_EVENT_KIND.NODE_METRIC,
      message_id: `mock-node-${step}`,
      timestamp: nowIso(step),
      node_id: 'N-001',
      cpu_ratio: Number(cpu.toFixed(3)),
      mem_ratio: Number(mem.toFixed(3)),
      tx_bps: 18000 + step * 10,
      rx_bps: 12000 + step * 8,
      status: cpu > 0.9 ? 'DEGRADED' : 'UP'
    },
    {
      ...common,
      kind: MONITOR_EVENT_KIND.LINK_METRIC,
      message_id: `mock-link-${step}`,
      timestamp: nowIso(step),
      link_id: 'N-001->N-002',
      src_node_id: 'N-001',
      dst_node_id: 'N-002',
      state: loss > 0.05 ? 'DEGRADED' : 'UP',
      loss_rate: Number(loss.toFixed(4)),
      rtt_ms: rtt,
      jitter_ms: jitter
    },
    {
      ...common,
      kind: MONITOR_EVENT_KIND.FLOW,
      message_id: `mock-flow-${step}`,
      timestamp: nowIso(step),
      flow_id: 'F-001',
      src_node_id: 'N-001',
      dst_node_id: 'N-050',
      path: ['N-001', 'S-001', 'N-050'],
      bps: 5200 + step * 15,
      priority: 'high'
    },
    {
      ...common,
      kind: MONITOR_EVENT_KIND.ALARM,
      message_id: `mock-alarm-${step}`,
      timestamp: nowIso(step),
      alarm_id: `A-${step}`,
      severity,
      scope_type: 'link',
      scope_id: 'N-001->N-002',
      title: severity === 'critical' ? '链路质量严重退化' : '链路质量波动',
      detail: `loss=${loss.toFixed(4)}, rtt=${rtt}ms`
    }
  ];
}
