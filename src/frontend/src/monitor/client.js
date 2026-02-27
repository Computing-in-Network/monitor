import {
  MONITOR_ERROR_CODE,
  MONITOR_SCHEMA_VERSION,
  normalizeMonitorKind
} from './types.js';

function joinUrl(baseUrl, path) {
  if (!baseUrl) {
    return path;
  }
  return `${baseUrl.replace(/\/+$/, '')}${path}`;
}

function deriveRiskLevelFromTasks(tasks = []) {
  const statuses = tasks.map((t) => t?.status).filter(Boolean);
  if (statuses.includes('disconnected')) {
    return 'critical';
  }
  if (statuses.includes('degraded') || statuses.includes('latency_anomaly')) {
    return 'warning';
  }
  return 'normal';
}

function buildOverviewFromLegacy(spread = {}, impact = {}) {
  const spreadResult = spread?.result && typeof spread.result === 'object' ? spread.result : spread;
  const impactResult = impact?.result && typeof impact.result === 'object' ? impact.result : impact;
  const tasks = Array.isArray(impactResult?.tasks) ? impactResult.tasks : [];
  const taskStatusCounts = {
    normal: 0,
    latency_anomaly: 0,
    degraded: 0,
    disconnected: 0
  };
  let highPriorityTasks = 0;
  let scoreSum = 0;
  let scoreCount = 0;
  for (const task of tasks) {
    const status = task?.status;
    if (status && Object.prototype.hasOwnProperty.call(taskStatusCounts, status)) {
      taskStatusCounts[status] += 1;
    }
    const score = Number(task?.priority_score);
    if (Number.isFinite(score)) {
      scoreSum += score;
      scoreCount += 1;
      if (score >= 70) {
        highPriorityTasks += 1;
      }
    }
  }
  return {
    status: 'ok',
    contract_version: 'analysis.v1',
    summary: {
      risk_level: deriveRiskLevelFromTasks(tasks),
      task_total: tasks.length,
      task_status_counts: taskStatusCounts,
      high_priority_tasks: highPriorityTasks,
      average_priority_score: scoreCount > 0 ? Number((scoreSum / scoreCount).toFixed(2)) : 0
    },
    topology_impact: {
      seed_nodes: spreadResult?.seeds || [],
      impacted_nodes: spreadResult?.impacted_nodes || [],
      impacted_links: spreadResult?.impacted_links || [],
      boundary_nodes: spreadResult?.boundary_nodes || []
    },
    tasks,
    alerts: tasks
      .map((item) => item?.alert_item)
      .filter((item) => item && typeof item === 'object')
  };
}

export class MonitorApiError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'MonitorApiError';
    this.code = details.code || MONITOR_ERROR_CODE.INVALID_PAYLOAD;
    this.status = details.status || 0;
    this.payload = details.payload;
  }
}

export class MonitorApiClient {
  constructor(options = {}) {
    this.baseUrl = options.baseUrl || '';
    this.token = options.token || '';
    this.fetchImpl = options.fetchImpl || window.fetch.bind(window);
  }

  async _request(path, options = {}) {
    const token = options.token || this.token;
    const headers = {
      ...(options.json ? { 'Content-Type': 'application/json' } : {}),
      ...(token ? { 'x-api-token': token } : {}),
      ...(options.headers || {})
    };
    const res = await this.fetchImpl(joinUrl(this.baseUrl, path), {
      method: options.method || 'GET',
      headers,
      body: options.body
    });
    let data = {};
    try {
      data = await res.json();
    } catch {
      data = {};
    }
    if (!res.ok || data.status === 'error') {
      throw new MonitorApiError(data.error_message || data.message || `HTTP ${res.status}`, {
        code: data.error_code || data.code || MONITOR_ERROR_CODE.INVALID_PAYLOAD,
        status: res.status,
        payload: data
      });
    }
    return { res, data };
  }

  async ingest(kind, event, options = {}) {
    const normalizedKind = normalizeMonitorKind(kind);
    if (!normalizedKind) {
      throw new MonitorApiError(`Invalid kind: ${kind}`, {
        code: MONITOR_ERROR_CODE.INVALID_KIND
      });
    }
    if (!event || typeof event !== 'object') {
      throw new MonitorApiError('Invalid payload: event must be object', {
        code: MONITOR_ERROR_CODE.INVALID_PAYLOAD
      });
    }
    const payload = {
      schema_version: MONITOR_SCHEMA_VERSION,
      ...event
    };
    const { data } = await this._request(`/api/v1/ingest/${normalizedKind}`, {
      method: 'POST',
      json: true,
      body: JSON.stringify(payload),
      token: options.token
    });
    return data;
  }

  async getSnapshot(options = {}) {
    const params = new URLSearchParams();
    if (options.topologyEpoch != null) {
      params.set('topology_epoch', String(options.topologyEpoch));
    }
    const query = params.toString();
    const path = `/api/v1/bff/snapshot${query ? `?${query}` : ''}`;
    const token = options.token || this.token;
    const headers = {
      ...(token ? { 'x-api-token': token } : {})
    };
    if (options.etag) {
      headers['If-None-Match'] = options.etag;
    }
    const res = await this.fetchImpl(joinUrl(this.baseUrl, path), {
      method: 'GET',
      headers
    });
    if (res.status === 304) {
      return {
        notModified: true,
        status: 304,
        etag: res.headers?.get?.('etag') || options.etag || '',
        data: null
      };
    }
    let data = {};
    try {
      data = await res.json();
    } catch {
      data = {};
    }
    if (!res.ok || data.status === 'error') {
      throw new MonitorApiError(data.error_message || data.message || `HTTP ${res.status}`, {
        code: data.error_code || data.code || MONITOR_ERROR_CODE.INVALID_PAYLOAD,
        status: res.status,
        payload: data
      });
    }
    return {
      notModified: false,
      status: res.status,
      etag: res.headers?.get?.('etag') || '',
      data
    };
  }

  async getHealth(options = {}) {
    const { data } = await this._request('/health', {
      method: 'GET',
      token: options.token
    });
    return data;
  }

  async getMetrics(options = {}) {
    const token = options.token || this.token;
    try {
      const { data } = await this._request('/api/v1/ops/slo', {
        method: 'GET',
        token
      });
      return data;
    } catch (err) {
      if (![404, 405, 501].includes(err?.status)) {
        throw err;
      }
      const { data } = await this._request('/metrics', {
        method: 'GET',
        token
      });
      return data;
    }
  }

  async ingestFaultControlAck(payload, options = {}) {
    const { data } = await this._request('/api/v1/ops/fault-injection/control-ack', {
      method: 'POST',
      json: true,
      body: JSON.stringify(payload || {}),
      token: options.token
    });
    return data;
  }

  async getSeries(options = {}) {
    const params = new URLSearchParams();
    if (options.eventType) {
      params.set('event_type', String(options.eventType));
    }
    if (options.metric) {
      params.set('metric', String(options.metric));
    }
    if (options.entityId) {
      params.set('entity_id', String(options.entityId));
    }
    if (options.limit != null) {
      params.set('limit', String(options.limit));
    }
    const query = params.toString();
    const path = `/api/v1/bff/series${query ? `?${query}` : ''}`;
    const { data } = await this._request(path, {
      method: 'GET',
      token: options.token
    });
    return data;
  }

  async getForecastLstm(options = {}) {
    const params = new URLSearchParams();
    if (options.eventType) {
      params.set('event_type', String(options.eventType));
    }
    if (options.metric) {
      params.set('metric', String(options.metric));
    }
    if (options.entityId) {
      params.set('entity_id', String(options.entityId));
    }
    if (options.strategy) {
      params.set('strategy', String(options.strategy));
    }
    if (options.horizon != null) {
      params.set('horizon', String(options.horizon));
    }
    if (options.window != null) {
      params.set('window', String(options.window));
    }
    const query = params.toString();
    const path = `/api/v1/bff/forecast/lstm${query ? `?${query}` : ''}`;
    const { data } = await this._request(path, {
      method: 'GET',
      token: options.token
    });
    return data;
  }

  async queryPathAnalysis(payload, options = {}) {
    const entityId = options.entityId
      || payload?.entity_id
      || payload?.link_uid
      || payload?.link_id
      || Object.keys(payload?.metrics || {})[0]
      || '';
    const token = options.token || this.token;
    try {
      return await this.getForecastLstm({
        eventType: payload?.event_type || 'link_metric',
        metric: payload?.metric || 'rtt_ms',
        entityId,
        strategy: payload?.strategy || 'fallback',
        horizon: payload?.horizon ?? 12,
        window: payload?.window ?? 12,
        token
      });
    } catch (err) {
      if (![404, 405, 501].includes(err?.status)) {
        throw err;
      }
      const { data } = await this._request('/api/v1/analysis/path/query', {
        method: 'POST',
        json: true,
        body: JSON.stringify(payload || {}),
        token
      });
      return data;
    }
  }

  async analyzeFaultSpread(payload, options = {}) {
    const token = options.token || this.token;
    try {
      const { data } = await this._request('/api/v1/bff/fault/spread', {
        method: 'POST',
        json: true,
        body: JSON.stringify(payload || {}),
        token
      });
      return data;
    } catch (err) {
      if (![404, 405, 501].includes(err?.status)) {
        throw err;
      }
      const { data } = await this._request('/api/v1/fault/spread/analyze', {
        method: 'POST',
        json: true,
        body: JSON.stringify(payload || {}),
        token
      });
      return data;
    }
  }

  async analyzeTaskImpact(payload, options = {}) {
    const { data } = await this._request('/api/v1/bff/fault/task-impact', {
      method: 'POST',
      json: true,
      body: JSON.stringify(payload || {}),
      token: options.token
    });
    return data;
  }

  async analyzeOverview(payload, options = {}) {
    const token = options.token || this.token;
    const body = JSON.stringify(payload || {});
    try {
      const { data } = await this._request('/api/v1/bff/analysis/overview', {
        method: 'POST',
        json: true,
        body,
        token
      });
      return data;
    } catch (err) {
      if (![404, 405, 501].includes(err?.status)) {
        throw err;
      }
      const [spread, impact] = await Promise.all([
        this.analyzeFaultSpread(payload || {}, { token }),
        this.analyzeTaskImpact({
          tasks: payload?.tasks || [],
          link_metrics: payload?.link_metrics || {}
        }, { token })
      ]);
      return buildOverviewFromLegacy(spread, impact);
    }
  }

  async analyzeRun(payload, options = {}) {
    const token = options.token || this.token;
    const body = JSON.stringify(payload || {});
    try {
      const { data } = await this._request('/api/v1/bff/analysis/run', {
        method: 'POST',
        json: true,
        body,
        token
      });
      return data;
    } catch (err) {
      if (![404, 405, 501].includes(err?.status)) {
        throw err;
      }
      return this.analyzeOverview(payload, { token });
    }
  }

  async analyzeGlobalImpact(payload, options = {}) {
    const { data } = await this._request('/api/v1/bff/analysis/global-impact', {
      method: 'POST',
      json: true,
      body: JSON.stringify(payload || {}),
      token: options.token
    });
    return data;
  }

  async createSimulation(payload, options = {}) {
    const { data } = await this._request('/api/v1/bff/simulation/create', {
      method: 'POST',
      json: true,
      body: JSON.stringify(payload || {}),
      token: options.token
    });
    return data;
  }

  async stepSimulation(simulationId, payload = {}, options = {}) {
    const { data } = await this._request(`/api/v1/bff/simulation/${encodeURIComponent(simulationId)}/step`, {
      method: 'POST',
      json: true,
      body: JSON.stringify(payload || {}),
      token: options.token
    });
    return data;
  }

  async getSimulationTimeline(simulationId, options = {}) {
    const { data } = await this._request(`/api/v1/bff/simulation/${encodeURIComponent(simulationId)}/timeline`, {
      method: 'GET',
      token: options.token
    });
    return data;
  }
}
