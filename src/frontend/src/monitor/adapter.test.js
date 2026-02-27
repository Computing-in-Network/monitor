import test from 'node:test';
import assert from 'node:assert/strict';
import { applyMonitorEvent, applyMonitorSnapshot, createEmptyMonitorSnapshot } from './adapter.js';
import { MONITOR_EVENT_KIND, MONITOR_SCHEMA_VERSION } from './types.js';

test('applyMonitorEvent uses injected thresholds', () => {
  const snapshot = createEmptyMonitorSnapshot();
  const next = applyMonitorEvent(snapshot, {
    kind: MONITOR_EVENT_KIND.NODE_METRIC,
    schema_version: MONITOR_SCHEMA_VERSION,
    node_id: 'N-001',
    cpu_ratio: 0.82,
    mem_ratio: 0.5
  }, {
    thresholds: {
      cpu_ratio: { warning: 0.8, critical: 0.95 },
      mem_ratio: { warning: 0.75, critical: 0.9 },
      loss_rate: { warning: 0.01, critical: 0.05 },
      rtt_ms: { warning: 80, critical: 150 },
      jitter_ms: { warning: 20, critical: 50 }
    }
  });
  assert.equal(next.byNode['N-001'].health, 'warning');
});

test('applyMonitorSnapshot parses nodes links and alarms', () => {
  const next = applyMonitorSnapshot(createEmptyMonitorSnapshot(), {
    monitor: {
      nodes: {
        'N-001': {
          node_id: 'N-001',
          cpu_ratio: 0.2,
          mem_ratio: 0.3
        }
      },
      links: {
        'N-001->N-002': {
          link_id: 'N-001->N-002',
          src_node_id: 'N-001',
          dst_node_id: 'N-002',
          loss_rate: 0.001,
          rtt_ms: 10,
          jitter_ms: 1
        }
      },
      alarms: [
        {
          alarm_id: 'A-1',
          severity: 'warning',
          scope_type: 'link',
          scope_id: 'N-001->N-002',
          timestamp: '2026-02-25T00:00:00Z'
        }
      ],
      updated_at: '2026-02-25T00:00:00Z'
    }
  });
  assert.equal(next.nodeCount, 1);
  assert.equal(next.linkCount, 1);
  assert.equal(next.alarmCount, 1);
  assert.equal(next.warningAlarmCount, 1);
});

test('applyMonitorSnapshot supports alternate node metric fields', () => {
  const next = applyMonitorSnapshot(createEmptyMonitorSnapshot(), {
    monitor: {
      nodes: [
        {
          node_uid: 'alpha',
          docker_name: 'alpha',
          cpu_percent: 35,
          memory_percent: 72,
          tx_rate: 128000,
          rx_rate: 64000
        }
      ]
    }
  });
  assert.equal(next.nodeCount, 1);
  assert.equal(next.byNode.alpha.cpuRatio, 0.35);
  assert.equal(next.byNode.alpha.memRatio, 0.72);
  assert.equal(next.byNode.alpha.txBps, 128000);
  assert.equal(next.byNode.alpha.rxBps, 64000);
});

test('applyMonitorSnapshot supports alternate link metric fields', () => {
  const next = applyMonitorSnapshot(createEmptyMonitorSnapshot(), {
    monitor: {
      links: [
        {
          link_uid: 'alpha<->bravo',
          src_node_uid: 'alpha',
          dst_node_uid: 'bravo',
          latency_ms: 21,
          loss_percent: 2,
          jitter: 3.4
        }
      ]
    }
  });
  assert.equal(next.linkCount, 1);
  assert.equal(next.byLink['alpha<->bravo'].rttMs, 21);
  assert.equal(next.byLink['alpha<->bravo'].lossRate, 0.02);
  assert.equal(next.byLink['alpha<->bravo'].jitterMs, 3.4);
});
