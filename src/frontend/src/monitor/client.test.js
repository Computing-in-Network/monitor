import test from 'node:test';
import assert from 'node:assert/strict';
import { MonitorApiClient, MonitorApiError } from './client.js';

test('getSnapshot sends topology_epoch query', async () => {
  let capturedUrl = '';
  let capturedHeaders = {};
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async (url, options = {}) => {
      capturedUrl = url;
      capturedHeaders = options.headers || {};
      return {
        ok: true,
        status: 200,
        headers: { get: () => 'W/"1708848000-10"' },
        async json() {
          return { monitor: {} };
        }
      };
    }
  });
  const res = await client.getSnapshot({ topologyEpoch: 1708848000, etag: 'W/"1708848000-9"' });
  assert.equal(capturedUrl, 'http://collector/api/v1/bff/snapshot?topology_epoch=1708848000');
  assert.equal(capturedHeaders['If-None-Match'], 'W/"1708848000-9"');
  assert.equal(res.notModified, false);
  assert.equal(res.etag, 'W/"1708848000-10"');
});

test('getSnapshot maps backend error_code', async () => {
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async () => ({
      ok: false,
      status: 401,
      async json() {
        return {
          status: 'error',
          error_code: 'UNAUTHORIZED',
          error_message: 'bad token'
        };
      }
    })
  });
  await assert.rejects(
    () => client.getSnapshot(),
    (err) => {
      assert.ok(err instanceof MonitorApiError);
      assert.equal(err.code, 'UNAUTHORIZED');
      assert.equal(err.status, 401);
      return true;
    }
  );
});

test('getSnapshot handles 304 not modified', async () => {
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async () => ({
      ok: false,
      status: 304,
      headers: { get: () => 'W/"1708848000-11"' },
      async json() {
        return {};
      }
    })
  });
  const res = await client.getSnapshot({ topologyEpoch: 1708848000, etag: 'W/"1708848000-10"' });
  assert.equal(res.notModified, true);
  assert.equal(res.status, 304);
  assert.equal(res.etag, 'W/"1708848000-11"');
});

test('queryPathAnalysis uses bff forecast endpoint', async () => {
  let capturedPath = '';
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async (url, options = {}) => {
      capturedPath = url;
      assert.equal(options.method, 'GET');
      return {
        ok: true,
        status: 200,
        async json() {
          return { status: 'ok', result: { points: [] } };
        }
      };
    }
  });
  await client.queryPathAnalysis({
    event_type: 'link_metric',
    metric: 'rtt_ms',
    entity_id: 'A<->B',
    strategy: 'fallback',
    horizon: 12,
    window: 12
  });
  assert.equal(capturedPath, 'http://collector/api/v1/bff/forecast/lstm?event_type=link_metric&metric=rtt_ms&entity_id=A%3C-%3EB&strategy=fallback&horizon=12&window=12');
});

test('analyzeFaultSpread posts to bff endpoint', async () => {
  let capturedPath = '';
  let capturedBody = '';
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async (url, options = {}) => {
      capturedPath = url;
      capturedBody = options.body;
      return {
        ok: true,
        status: 200,
        async json() {
          return { status: 'ok', result: { impacted_nodes: [] } };
        }
      };
    }
  });
  await client.analyzeFaultSpread({ alarm_nodes: ['A'] });
  assert.equal(capturedPath, 'http://collector/api/v1/bff/fault/spread');
  assert.equal(capturedBody, JSON.stringify({ alarm_nodes: ['A'] }));
});

test('getSeries sends query params', async () => {
  let capturedPath = '';
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async (url) => {
      capturedPath = url;
      return {
        ok: true,
        status: 200,
        async json() {
          return { status: 'ok', data: { points: [] } };
        }
      };
    }
  });
  await client.getSeries({
    eventType: 'link_metric',
    metric: 'rtt_ms',
    entityId: 'A<->B',
    limit: 120
  });
  assert.equal(capturedPath, 'http://collector/api/v1/bff/series?event_type=link_metric&metric=rtt_ms&entity_id=A%3C-%3EB&limit=120');
});

test('analyzeTaskImpact posts to bff endpoint', async () => {
  let capturedPath = '';
  let capturedBody = '';
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async (url, options = {}) => {
      capturedPath = url;
      capturedBody = options.body;
      return {
        ok: true,
        status: 200,
        async json() {
          return { status: 'ok', result: { impacted_tasks: [] } };
        }
      };
    }
  });
  await client.analyzeTaskImpact({ alarm_nodes: ['A'] });
  assert.equal(capturedPath, 'http://collector/api/v1/bff/fault/task-impact');
  assert.equal(capturedBody, JSON.stringify({ alarm_nodes: ['A'] }));
});

test('analyzeOverview posts to single analysis contract endpoint', async () => {
  let capturedPath = '';
  let capturedBody = '';
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async (url, options = {}) => {
      capturedPath = url;
      capturedBody = options.body;
      return {
        ok: true,
        status: 200,
        async json() {
          return { status: 'ok', contract_version: 'analysis.v1', summary: {} };
        }
      };
    }
  });
  await client.analyzeOverview({ alarm_nodes: ['A'], links: [], tasks: [], link_metrics: {} });
  assert.equal(capturedPath, 'http://collector/api/v1/bff/analysis/overview');
  assert.equal(capturedBody, JSON.stringify({ alarm_nodes: ['A'], links: [], tasks: [], link_metrics: {} }));
});

test('analyzeOverview falls back to spread + task-impact when overview is unavailable', async () => {
  const calls = [];
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async (url, options = {}) => {
      calls.push({ url, method: options.method, body: options.body });
      if (url.endsWith('/api/v1/bff/analysis/overview')) {
        return {
          ok: false,
          status: 404,
          async json() {
            return { detail: 'Not Found' };
          }
        };
      }
      if (url.endsWith('/api/v1/bff/fault/spread')) {
        return {
          ok: true,
          status: 200,
          async json() {
            return {
              status: 'ok',
              result: {
                seeds: ['A'],
                impacted_nodes: ['A', 'B'],
                impacted_links: ['A<->B'],
                boundary_nodes: []
              }
            };
          }
        };
      }
      if (url.endsWith('/api/v1/bff/fault/task-impact')) {
        return {
          ok: true,
          status: 200,
          async json() {
            return {
              status: 'ok',
              result: {
                tasks: [
                  {
                    task_id: 't1',
                    status: 'degraded',
                    priority_score: 72,
                    alert_item: { task_id: 't1', title: 'x' }
                  }
                ]
              }
            };
          }
        };
      }
      return {
        ok: false,
        status: 500,
        async json() {
          return { status: 'error', error_message: 'unexpected' };
        }
      };
    }
  });
  const res = await client.analyzeOverview({
    alarm_nodes: ['A'],
    links: [{ src: 'A', dst: 'B' }],
    tasks: [{ task_id: 't1' }],
    link_metrics: { 'A<->B': { rtt_ms: 20, loss_rate: 0.01 } }
  });
  assert.equal(calls[0].url, 'http://collector/api/v1/bff/analysis/overview');
  assert.equal(calls[1].url, 'http://collector/api/v1/bff/fault/spread');
  assert.equal(calls[2].url, 'http://collector/api/v1/bff/fault/task-impact');
  assert.equal(res.contract_version, 'analysis.v1');
  assert.equal(res.summary.risk_level, 'warning');
  assert.equal(res.topology_impact.impacted_nodes.length, 2);
  assert.equal(res.tasks.length, 1);
  assert.equal(res.alerts.length, 1);
});

test('analyzeRun posts to run endpoint', async () => {
  let capturedPath = '';
  let capturedBody = '';
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async (url, options = {}) => {
      capturedPath = url;
      capturedBody = options.body;
      return {
        ok: true,
        status: 200,
        async json() {
          return { status: 'ok', contract_version: 'analysis.v1', summary: {} };
        }
      };
    }
  });
  await client.analyzeRun({ mode: 'global', scope_type: 'network', scope_id: 'all', topology_epoch: '1708848000' });
  assert.equal(capturedPath, 'http://collector/api/v1/bff/analysis/run');
  assert.equal(capturedBody, JSON.stringify({ mode: 'global', scope_type: 'network', scope_id: 'all', topology_epoch: '1708848000' }));
});

test('simulation api methods use expected endpoints', async () => {
  const paths = [];
  const client = new MonitorApiClient({
    baseUrl: 'http://collector',
    fetchImpl: async (url, options = {}) => {
      paths.push(`${options.method || 'GET'} ${url}`);
      return {
        ok: true,
        status: 200,
        async json() {
          return { status: 'ok', simulation_id: 'sim-1', timeline: [] };
        }
      };
    }
  });
  await client.createSimulation({ scenario_type: 'link_down' });
  await client.stepSimulation('sim-1', {});
  await client.getSimulationTimeline('sim-1');
  assert.equal(paths[0], 'POST http://collector/api/v1/bff/simulation/create');
  assert.equal(paths[1], 'POST http://collector/api/v1/bff/simulation/sim-1/step');
  assert.equal(paths[2], 'GET http://collector/api/v1/bff/simulation/sim-1/timeline');
});
