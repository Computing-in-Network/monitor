import { useEffect, useRef, useState } from 'react';
import {
  Cartesian2,
  Cartesian3,
  Color,
  EllipsoidTerrainProvider,
  HorizontalOrigin,
  LabelStyle,
  OpenStreetMapImageryProvider,
  PolylineDashMaterialProperty,
  ScreenSpaceEventHandler,
  ScreenSpaceEventType,
  TileMapServiceImageryProvider,
  VerticalOrigin,
  defined,
  Viewer
} from 'cesium';
import {
  applyMonitorEvent,
  applyMonitorSnapshot,
  createEmptyMonitorSnapshot,
  generateMockMonitorEvents,
  MonitorApiClient
} from './monitor';

const defaultWsUrl = (() => {
  const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
  return `${scheme}://${window.location.hostname}:8765`;
})();
const WS_URL = import.meta.env.VITE_TOPO_WS_URL || defaultWsUrl;
const LAYER_PREFS_KEY = 'topo_layer_prefs_v1';
const TRAIL_LEN_BY_TYPE = {
  leo: 180,
  aircraft: 260,
  ship: 300
};
const SAT_ORBIT_SAMPLES = 72;

const typeColor = {
  leo: Color.fromCssColorString('#ffb703'),
  aircraft: Color.fromCssColorString('#2a9d8f'),
  ship: Color.fromCssColorString('#00b4d8')
};
const orbitColor = {
  polar: Color.fromCssColorString('#7dff00').withAlpha(0.42),
  inclined: Color.fromCssColorString('#4df8b5').withAlpha(0.36)
};
const linkStyle = {
  sat_sat: {
    color: Color.fromCssColorString('#ff4fd8').withAlpha(0.86),
    width: 2.0
  },
  sat_mobile: {
    color: Color.fromCssColorString('#ff7f11').withAlpha(0.9),
    width: 2.2
  },
  other: {
    color: Color.fromCssColorString('#8da9c4').withAlpha(0.18),
    width: 1.0
  }
};
const defaultLayerPrefs = {
  nodeLeo: true,
  nodeAircraft: true,
  nodeShip: true,
  linkSatSat: true,
  linkSatMobile: true,
  linkOther: true,
  showTrails: true,
  showLabels: true,
  showOrbits: true
};
const SELECTED_NODE_COLOR = Color.fromCssColorString('#fff176');
const DAMAGED_NODE_COLOR = Color.fromCssColorString('#ff595e');
const SELECTED_LINK_COLOR = Color.fromCssColorString('#f94144').withAlpha(0.95);
const FAULT_LINK_COLOR = Color.fromCssColorString('#ff3b30').withAlpha(0.95);
const FLOW_HIGHLIGHT_COLOR = Color.fromCssColorString('#4cc9f0').withAlpha(0.95);
const STALE_WARN_MS = 2500;
const STALE_ERROR_MS = 5000;
const INGEST_FPS_WARN = 0.7;
const FRAME_QUEUE_MAX = 600;
const SPEED_OPTIONS = [0.5, 1, 2];
const ORBIT_UPDATE_INTERVAL_TICKS = 4;
const HISTORY_MAX_POINTS = 300;
const HISTORY_RETENTION_MS = 20 * 60 * 1000;
const TREND_WINDOW_OPTIONS = [60, 300, 900];
const SNAPSHOT_STALE_MS = 10_000;
const ENABLE_MONITOR_MOCK = import.meta.env.VITE_MONITOR_MOCK === '1';
const defaultMonitorApiUrl = '/monitor-api';
const MONITOR_API_URL = import.meta.env.VITE_MONITOR_API_URL || defaultMonitorApiUrl;
const MONITOR_API_TOKEN = import.meta.env.VITE_MONITOR_API_TOKEN || '';
const AUTO_CLEAR_FAULTS_ON_CONNECT = import.meta.env.VITE_AUTO_CLEAR_FAULTS_ON_CONNECT !== '0';

function parseScopedLink(scopeId) {
  if (!scopeId || typeof scopeId !== 'string') {
    return null;
  }
  const clean = scopeId.trim();
  if (!clean) {
    return null;
  }
  if (clean.includes('<->')) {
    const [a, b] = clean.split('<->').map((part) => part.trim());
    return a && b ? { a, b } : null;
  }
  if (clean.includes('->')) {
    const [a, b] = clean.split('->').map((part) => part.trim());
    return a && b ? { a, b } : null;
  }
  if (clean.includes('|')) {
    const [a, b] = clean.split('|').map((part) => part.trim());
    return a && b ? { a, b } : null;
  }
  return null;
}

function normalizeIdentity(value) {
  if (value == null) {
    return '';
  }
  return String(value).trim().toLowerCase();
}

function normalizeIdentityLoose(value) {
  return normalizeIdentity(value).replace(/[^a-z0-9]/g, '');
}

function svgDataUri(svg) {
  return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
}

function buildNodeIcon(type) {
  if (type === 'leo') {
    return svgDataUri(`
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
        <defs>
          <linearGradient id="g1" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stop-color="#9eff4a"/>
            <stop offset="100%" stop-color="#4edfff"/>
          </linearGradient>
        </defs>
        <circle cx="32" cy="32" r="28" fill="#0f2133" stroke="#6ee7ff" stroke-width="2"/>
        <rect x="25" y="25" width="14" height="14" rx="2" fill="url(#g1)" stroke="#dff7ff" stroke-width="1.5"/>
        <rect x="7" y="28" width="16" height="8" rx="1.5" fill="#5c8dbf" stroke="#cbe7ff" stroke-width="1"/>
        <rect x="41" y="28" width="16" height="8" rx="1.5" fill="#5c8dbf" stroke="#cbe7ff" stroke-width="1"/>
        <line x1="32" y1="39" x2="32" y2="49" stroke="#fef08a" stroke-width="2"/>
        <circle cx="32" cy="50" r="2" fill="#fde047"/>
      </svg>
    `);
  }
  if (type === 'aircraft') {
    return svgDataUri(`
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
        <circle cx="32" cy="32" r="28" fill="#102a32" stroke="#59e1c1" stroke-width="2"/>
        <path d="M32 11 L37 25 L53 30 L53 34 L37 39 L32 53 L27 39 L11 34 L11 30 L27 25 Z"
              fill="#e7f7ff" stroke="#7ed7ff" stroke-width="1.5"/>
        <rect x="29" y="14" width="6" height="35" rx="2" fill="#8ec5ff" opacity="0.45"/>
      </svg>
    `);
  }
  return svgDataUri(`
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
      <circle cx="32" cy="32" r="28" fill="#0b2433" stroke="#60d9ff" stroke-width="2"/>
      <path d="M14 39 H50 L44 47 H20 Z" fill="#f1f5f9" stroke="#bde7ff" stroke-width="1.5"/>
      <rect x="24" y="28" width="16" height="10" rx="1.5" fill="#88b5e6" stroke="#d6efff" stroke-width="1"/>
      <rect x="29" y="23" width="6" height="5" rx="1" fill="#88b5e6"/>
      <line x1="14" y1="50" x2="50" y2="50" stroke="#4cc9f0" stroke-width="2" opacity="0.7"/>
    </svg>
  `);
}

const nodeIcon = {
  leo: buildNodeIcon('leo'),
  aircraft: buildNodeIcon('aircraft'),
  ship: buildNodeIcon('ship')
};

function toCartesian(node) {
  return Cartesian3.fromDegrees(node.lon, node.lat, node.alt_m);
}

function buildSatelliteOrbitPolyline(node) {
  if (node.type !== 'leo' || node.vx == null || node.vy == null || node.vz == null) {
    return null;
  }
  const r = new Cartesian3(node.x, node.y, node.z);
  const v = new Cartesian3(node.vx, node.vy, node.vz);
  const n = Cartesian3.cross(r, v, new Cartesian3());
  if (Cartesian3.magnitudeSquared(n) < 1e-6) {
    return null;
  }
  const u = Cartesian3.normalize(r, new Cartesian3());
  const w = Cartesian3.normalize(Cartesian3.cross(n, u, new Cartesian3()), new Cartesian3());
  const radius = Cartesian3.magnitude(r);
  const points = [];
  for (let i = 0; i <= SAT_ORBIT_SAMPLES; i += 1) {
    const theta = (2.0 * Math.PI * i) / SAT_ORBIT_SAMPLES;
    const pu = Cartesian3.multiplyByScalar(u, Math.cos(theta) * radius, new Cartesian3());
    const pw = Cartesian3.multiplyByScalar(w, Math.sin(theta) * radius, new Cartesian3());
    points.push(Cartesian3.add(pu, pw, new Cartesian3()));
  }
  return points;
}

function resolveLinkKind(a, b) {
  const aSat = a.type === 'leo';
  const bSat = b.type === 'leo';
  if (aSat && bSat) {
    return 'sat_sat';
  }
  if ((aSat && !bSat) || (!aSat && bSat)) {
    return 'sat_mobile';
  }
  return 'other';
}

function resolveLinkStyle(a, b) {
  return linkStyle[resolveLinkKind(a, b)];
}

function isNodeVisible(node, layerPrefs) {
  if (node.type === 'leo') {
    return layerPrefs.nodeLeo;
  }
  if (node.type === 'aircraft') {
    return layerPrefs.nodeAircraft;
  }
  return layerPrefs.nodeShip;
}

function isLinkVisible(linkKind, layerPrefs) {
  if (linkKind === 'sat_sat') {
    return layerPrefs.linkSatSat;
  }
  if (linkKind === 'sat_mobile') {
    return layerPrefs.linkSatMobile;
  }
  return layerPrefs.linkOther;
}

function edgeKey(a, b) {
  return a < b ? `${a}|${b}` : `${b}|${a}`;
}

function appendHistoryPoint(series, point) {
  if (point == null || Number.isNaN(point.v)) {
    return series;
  }
  const next = [...series];
  const last = next[next.length - 1];
  if (last && last.t === point.t) {
    next[next.length - 1] = point;
  } else {
    next.push(point);
  }
  const minTs = point.t - HISTORY_RETENTION_MS;
  while (next.length > 0 && next[0].t < minTs) {
    next.shift();
  }
  if (next.length > HISTORY_MAX_POINTS) {
    next.shift();
  }
  return next;
}

function buildSparklinePath(points, width = 156, height = 36) {
  if (!Array.isArray(points) || points.length < 2) {
    return '';
  }
  const values = points.map((p) => p.v);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(1e-6, max - min);
  const xStep = width / Math.max(1, points.length - 1);
  return points.map((p, idx) => {
    const x = idx * xStep;
    const y = height - ((p.v - min) / span) * height;
    return `${idx === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(' ');
}

function filterSeriesByWindow(series, windowSec) {
  if (!Array.isArray(series) || series.length === 0) {
    return [];
  }
  if (!windowSec || windowSec <= 0) {
    return series;
  }
  const lastTs = series[series.length - 1]?.t;
  if (!lastTs) {
    return series;
  }
  const startTs = lastTs - windowSec * 1000;
  return series.filter((p) => p.t >= startTs);
}

function getAnalysisResult(payload) {
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  if (payload.result && typeof payload.result === 'object') {
    return payload.result;
  }
  if (payload.data && typeof payload.data === 'object') {
    if (payload.data.result && typeof payload.data.result === 'object') {
      return payload.data.result;
    }
    return payload.data;
  }
  return null;
}

export function App() {
  const [frame, setFrame] = useState(null);
  const [connected, setConnected] = useState(false);
  const [hoverInfo, setHoverInfo] = useState(null);
  const [selected, setSelected] = useState(null);
  const [runtimeHealth, setRuntimeHealth] = useState({
    stalenessMs: 0,
    ingestFps: 0
  });
  const [playback, setPlayback] = useState({
    paused: false,
    speed: 1
  });
  const [queueDepth, setQueueDepth] = useState(0);
  const [faults, setFaults] = useState([]);
  const [controlStatus, setControlStatus] = useState('');
  const [monitorSnapshot, setMonitorSnapshot] = useState(() => createEmptyMonitorSnapshot());
  const [monitorMockTick, setMonitorMockTick] = useState(0);
  const [selectedMonitorAlarmId, setSelectedMonitorAlarmId] = useState(null);
  const [selectedFlowId, setSelectedFlowId] = useState(null);
  const [trendWindowSec, setTrendWindowSec] = useState(300);
  const [monitorSourceMode, setMonitorSourceMode] = useState(ENABLE_MONITOR_MOCK ? 'mock' : (MONITOR_API_URL ? 'snapshot_connecting' : 'idle'));
  const [monitorError, setMonitorError] = useState('');
  const [monitorActionStatus, setMonitorActionStatus] = useState('');
  const [monitorEpoch, setMonitorEpoch] = useState(1708848000);
  const [monitorAvailableEpochs, setMonitorAvailableEpochs] = useState([]);
  const [monitorConsecutiveFailures, setMonitorConsecutiveFailures] = useState(0);
  const [monitorLastSuccessAt, setMonitorLastSuccessAt] = useState(0);
  const [alarmSeverityFilter, setAlarmSeverityFilter] = useState('all');
  const [alarmScopeFilter, setAlarmScopeFilter] = useState('all');
  const [collectorHealth, setCollectorHealth] = useState(null);
  const [collectorMetrics, setCollectorMetrics] = useState(null);
  const [pathAnalysis, setPathAnalysis] = useState(null);
  const [faultSpread, setFaultSpread] = useState(null);
  const [taskImpact, setTaskImpact] = useState(null);
  const [analysisOverview, setAnalysisOverview] = useState(null);
  const [seriesSnapshot, setSeriesSnapshot] = useState(null);
  const [analysisLoading, setAnalysisLoading] = useState(false);
  const [analysisError, setAnalysisError] = useState('');
  const [analysisSupported, setAnalysisSupported] = useState(true);
  const [simulationLoading, setSimulationLoading] = useState(false);
  const [simulationResult, setSimulationResult] = useState(null);
  const [analysisSummary, setAnalysisSummary] = useState(null);
  const [replayMode, setReplayMode] = useState(false);
  const [showLayerPanel, setShowLayerPanel] = useState(false);
  const [showFaultPanel, setShowFaultPanel] = useState(false);
  const [showFlowPanel, setShowFlowPanel] = useState(false);
  const [toast, setToast] = useState(null);
  const [layerPrefs, setLayerPrefs] = useState(() => {
    try {
      const raw = window.localStorage.getItem(LAYER_PREFS_KEY);
      if (!raw) {
        return defaultLayerPrefs;
      }
      return { ...defaultLayerPrefs, ...JSON.parse(raw) };
    } catch {
      return defaultLayerPrefs;
    }
  });

  const containerRef = useRef(null);
  const viewerRef = useRef(null);
  const nodeEntitiesRef = useRef(new Map());
  const trailEntitiesRef = useRef(new Map());
  const trailPointsRef = useRef(new Map());
  const orbitEntitiesRef = useRef(new Map());
  const linkEntitiesRef = useRef(new Map());
  const faultLinkEntitiesRef = useRef(new Map());
  const linkVisualStateRef = useRef(new Map());
  const pickHandlerRef = useRef(null);
  const nodeStateRef = useRef(new Map());
  const linkStateRef = useRef(new Map());
  const nodeVisibilityRef = useRef(new Map());
  const lastFrameAtRef = useRef(0);
  const frameTimestampsRef = useRef([]);
  const frameQueueRef = useRef([]);
  const orbitCacheRef = useRef(new Map());
  const wsRef = useRef(null);
  const monitorClientRef = useRef(MONITOR_API_URL ? new MonitorApiClient({ baseUrl: MONITOR_API_URL, token: MONITOR_API_TOKEN }) : null);
  const metricHistoryRef = useRef({ nodes: new Map(), links: new Map() });
  const monitorFailureRef = useRef(0);
  const monitorLastSuccessRef = useRef(0);
  const monitorEtagRef = useRef('');
  const topoNodeAliasRef = useRef(new Map());
  const replayFileInputRef = useRef(null);
  const toastTimerRef = useRef(null);
  const faultInitRef = useRef(false);

  function pushToast(text, level = 'ok') {
    setToast({ text, level, id: Date.now() });
    if (toastTimerRef.current) {
      window.clearTimeout(toastTimerRef.current);
    }
    toastTimerRef.current = window.setTimeout(() => setToast(null), 2200);
  }

  useEffect(() => {
    window.localStorage.setItem(LAYER_PREFS_KEY, JSON.stringify(layerPrefs));
  }, [layerPrefs]);

  useEffect(() => () => {
    if (toastTimerRef.current) {
      window.clearTimeout(toastTimerRef.current);
    }
  }, []);

  useEffect(() => {
    if (!ENABLE_MONITOR_MOCK) {
      return undefined;
    }
    const timer = window.setInterval(() => {
      setMonitorMockTick((v) => v + 1);
    }, 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!ENABLE_MONITOR_MOCK) {
      return;
    }
    const events = generateMockMonitorEvents(monitorMockTick);
    setMonitorSnapshot((prev) => events.reduce((acc, event) => applyMonitorEvent(acc, event), prev));
  }, [monitorMockTick]);

  useEffect(() => {
    if (ENABLE_MONITOR_MOCK || replayMode || !monitorClientRef.current) {
      return undefined;
    }
    let disposed = false;
    let timer = null;

    async function pullSnapshot() {
      try {
        const rawResult = await monitorClientRef.current.getSnapshot({
          topologyEpoch: monitorEpoch,
          etag: monitorEtagRef.current
        });
        if (disposed) {
          return;
        }
        monitorEtagRef.current = rawResult.etag || monitorEtagRef.current;
        if (rawResult.notModified) {
          setMonitorSourceMode('snapshot');
          setMonitorError('');
          monitorFailureRef.current = 0;
          setMonitorConsecutiveFailures(0);
          monitorLastSuccessRef.current = Date.now();
          setMonitorLastSuccessAt(monitorLastSuccessRef.current);
          return;
        }

        const raw = rawResult.data || {};
        const monitor = raw?.monitor || {};
        setMonitorSnapshot((prev) => applyMonitorSnapshot(prev, raw));
        setMonitorSourceMode('snapshot');
        setMonitorError('');
        monitorFailureRef.current = 0;
        setMonitorConsecutiveFailures(0);
        monitorLastSuccessRef.current = Date.now();
        setMonitorLastSuccessAt(monitorLastSuccessRef.current);

        const availableEpochs = Array.isArray(monitor.available_epochs)
          ? monitor.available_epochs.map((v) => Number(v)).filter((v) => Number.isFinite(v))
          : [];
        if (availableEpochs.length > 0) {
          setMonitorAvailableEpochs(availableEpochs);
        }
        if (monitor.topology_epoch != null) {
          const serverEpoch = Number(monitor.topology_epoch);
          if (Number.isFinite(serverEpoch) && monitorEpoch == null) {
            setMonitorEpoch(serverEpoch);
          }
        }
      } catch (err) {
        if (disposed) {
          return;
        }
        const now = Date.now();
        setMonitorError(err?.message || 'monitor snapshot 拉取失败');
        monitorFailureRef.current += 1;
        setMonitorConsecutiveFailures(monitorFailureRef.current);
        if (monitorLastSuccessRef.current > 0 && now - monitorLastSuccessRef.current <= SNAPSHOT_STALE_MS) {
          setMonitorSourceMode('snapshot_stale');
        } else {
          setMonitorSourceMode('snapshot_error');
        }
      } finally {
        if (!disposed) {
          const nextDelay = monitorFailureRef.current >= 3 ? 5000 : 2000;
          timer = window.setTimeout(pullSnapshot, nextDelay);
        }
      }
    }

    pullSnapshot();
    return () => {
      disposed = true;
      if (timer) {
        window.clearTimeout(timer);
      }
    };
  }, [monitorEpoch, replayMode]);

  useEffect(() => {
    if (ENABLE_MONITOR_MOCK || replayMode || !monitorClientRef.current) {
      return undefined;
    }
    let disposed = false;
    async function pullStatus() {
      try {
        const [health, metrics] = await Promise.all([
          monitorClientRef.current.getHealth(),
          monitorClientRef.current.getMetrics()
        ]);
        if (disposed) {
          return;
        }
        setCollectorHealth(health);
        setCollectorMetrics(metrics);
      } catch {
        if (disposed) {
          return;
        }
        setCollectorHealth(null);
        setCollectorMetrics(null);
      }
    }
    pullStatus();
    const timer = window.setInterval(pullStatus, 5000);
    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, [replayMode]);

  useEffect(() => {
    const ts = Date.parse(monitorSnapshot.updatedAt || '');
    if (Number.isNaN(ts)) {
      return;
    }
    const history = metricHistoryRef.current;
    for (const nodeMetric of Object.values(monitorSnapshot.byNode)) {
      const nodeId = nodeMetric.nodeId;
      const prev = history.nodes.get(nodeId) || {
        cpu: [],
        mem: [],
        tx: [],
        rx: []
      };
      history.nodes.set(nodeId, {
        cpu: appendHistoryPoint(prev.cpu, { t: ts, v: nodeMetric.cpuRatio ?? NaN }),
        mem: appendHistoryPoint(prev.mem, { t: ts, v: nodeMetric.memRatio ?? NaN }),
        tx: appendHistoryPoint(prev.tx, { t: ts, v: nodeMetric.txBps ?? NaN }),
        rx: appendHistoryPoint(prev.rx, { t: ts, v: nodeMetric.rxBps ?? NaN })
      });
    }
    for (const linkMetric of Object.values(monitorSnapshot.byLink)) {
      const linkId = linkMetric.linkId;
      const prev = history.links.get(linkId) || {
        loss: [],
        rtt: [],
        jitter: []
      };
      history.links.set(linkId, {
        loss: appendHistoryPoint(prev.loss, { t: ts, v: linkMetric.lossRate ?? NaN }),
        rtt: appendHistoryPoint(prev.rtt, { t: ts, v: linkMetric.rttMs ?? NaN }),
        jitter: appendHistoryPoint(prev.jitter, { t: ts, v: linkMetric.jitterMs ?? NaN })
      });
    }
  }, [monitorSnapshot]);

  function toggleLayer(key) {
    setLayerPrefs((prev) => ({ ...prev, [key]: !prev[key] }));
  }

  function resetLayerPrefs() {
    setLayerPrefs(defaultLayerPrefs);
  }

  function shiftFrameFromQueue() {
    if (frameQueueRef.current.length === 0) {
      setQueueDepth(0);
      return false;
    }
    const nextFrame = frameQueueRef.current.shift();
    setQueueDepth(frameQueueRef.current.length);
    if (nextFrame) {
      setFrame(nextFrame);
      return true;
    }
    return false;
  }

  function stepOnce() {
    setPlayback((prev) => ({ ...prev, paused: true }));
    shiftFrameFromQueue();
  }

  function sendControl(action, extra = {}) {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setControlStatus('控制通道未连接');
      return;
    }
    const request_id = `req-${Date.now()}`;
    ws.send(JSON.stringify({ action, request_id, ...extra }));
  }

  function focusFaultTarget(fault) {
    const viewer = viewerRef.current;
    if (!viewer) {
      return;
    }
    if (fault.fault_type === 'DAMAGED') {
      const nodeId = fault.target?.node_id;
      if (!nodeId) {
        return;
      }
      const node = nodeStateRef.current.get(nodeId);
      if (!node) {
        return;
      }
      setSelected({ kind: 'node', id: nodeId });
      viewer.camera.flyTo({
        destination: Cartesian3.fromDegrees(node.lon, node.lat, node.alt_m + 1_200_000),
        duration: 0.8
      });
      return;
    }
    const a = fault.target?.a;
    const b = fault.target?.b;
    if (!a || !b) {
      return;
    }
    const aNode = nodeStateRef.current.get(a);
    const bNode = nodeStateRef.current.get(b);
    if (!aNode || !bNode) {
      return;
    }
    const linkIdAB = `${a}-${b}`;
    const linkIdBA = `${b}-${a}`;
    const linkIdFault = `fault-${edgeKey(a, b)}`;
    const linkId = linkStateRef.current.has(linkIdAB)
      ? linkIdAB
      : (linkStateRef.current.has(linkIdBA) ? linkIdBA : linkIdFault);
    if (linkStateRef.current.has(linkId)) {
      setSelected({ kind: 'link', id: linkId });
    }
    viewer.camera.flyTo({
      destination: Cartesian3.fromDegrees(
        (aNode.lon + bNode.lon) / 2.0,
        (aNode.lat + bNode.lat) / 2.0,
        Math.max(aNode.alt_m, bNode.alt_m) + 1_200_000
      ),
      duration: 0.8
    });
  }

  function focusLinkByNodes(a, b) {
    const viewer = viewerRef.current;
    if (!viewer || !a || !b) {
      return false;
    }
    const aNode = nodeStateRef.current.get(a);
    const bNode = nodeStateRef.current.get(b);
    if (!aNode || !bNode) {
      return false;
    }
    const linkIdAB = `${a}-${b}`;
    const linkIdBA = `${b}-${a}`;
    const linkIdFault = `fault-${edgeKey(a, b)}`;
    const linkId = linkStateRef.current.has(linkIdAB)
      ? linkIdAB
      : (linkStateRef.current.has(linkIdBA) ? linkIdBA : linkIdFault);
    if (linkStateRef.current.has(linkId)) {
      setSelected({ kind: 'link', id: linkId });
    }
    viewer.camera.flyTo({
      destination: Cartesian3.fromDegrees(
        (aNode.lon + bNode.lon) / 2.0,
        (aNode.lat + bNode.lat) / 2.0,
        Math.max(aNode.alt_m, bNode.alt_m) + 1_200_000
      ),
      duration: 0.8
    });
    return true;
  }

  function resolveTopoNodeId(identity) {
    if (!identity) {
      return '';
    }
    const raw = String(identity).trim();
    if (nodeStateRef.current.has(raw)) {
      return raw;
    }
    const key = normalizeIdentity(raw);
    const loose = normalizeIdentityLoose(raw);
    return topoNodeAliasRef.current.get(key) || topoNodeAliasRef.current.get(loose) || '';
  }

  function focusMonitorAlarm(alarm) {
    if (!alarm) {
      return;
    }
    setSelectedMonitorAlarmId(alarm.id);
    if (alarm.scopeType === 'node') {
      const lookup = alarm.scopeUid || alarm.scopeId;
      const topoNodeId = resolveTopoNodeId(lookup);
      const node = topoNodeId ? nodeStateRef.current.get(topoNodeId) : null;
      const viewer = viewerRef.current;
      if (!node || !viewer) {
        const msg = `定位失败：当前拓扑中找不到节点 ${lookup || '-'}`;
        setMonitorActionStatus(msg);
        pushToast(msg, 'warn');
        return;
      }
      setSelected({ kind: 'node', id: node.id });
      viewer.camera.flyTo({
        destination: Cartesian3.fromDegrees(node.lon, node.lat, node.alt_m + 1_200_000),
        duration: 0.8
      });
      setMonitorActionStatus(`已定位节点 ${node.id}`);
      pushToast(`已定位节点 ${node.id}`, 'ok');
      return;
    }
    const link = parseScopedLink(alarm.scopeUid || alarm.scopeId);
    if (!link) {
      const msg = `定位失败：无法解析 scope (${alarm.scopeUid || alarm.scopeId || '-'})`;
      setMonitorActionStatus(msg);
      pushToast(msg, 'warn');
      return;
    }
    const aTopo = resolveTopoNodeId(link.a);
    const bTopo = resolveTopoNodeId(link.b);
    const ok = focusLinkByNodes(aTopo || link.a, bTopo || link.b);
    const msg = ok
      ? `已定位链路 ${(aTopo || link.a)}<->${(bTopo || link.b)}`
      : `定位失败：当前拓扑中找不到链路 ${link.a}<->${link.b}`;
    setMonitorActionStatus(msg);
    pushToast(msg, ok ? 'ok' : 'warn');
  }
  async function runAdvancedAnalysis() {
    if (!monitorClientRef.current || !frame) {
      setAnalysisError('分析失败：monitor API 或拓扑帧不可用');
      return;
    }
    if (!analysisSupported) {
      setAnalysisError('当前后端未启用高级分析接口');
      pushToast('当前后端未启用高级分析接口', 'warn');
      return;
    }
    try {
      setAnalysisLoading(true);
      setAnalysisError('');
      let analysisMode = 'auto';
      let scopeType = 'network';
      let scopeId = 'all';
      let entityId = '';
      if (selected?.kind === 'link' && selectedLink) {
        analysisMode = 'focused';
        scopeType = 'link';
        scopeId = selectedLinkMetric?.linkUid || [selectedLink.a.id, selectedLink.b.id].sort().join('<->');
        entityId = scopeId;
      } else if (selected?.kind === 'node' && selected?.id) {
        analysisMode = 'focused';
        scopeType = 'node';
        scopeId = selected.id;
        entityId = selected.id;
      } else if (selectedFlow?.path?.length > 1) {
        entityId = `${selectedFlow.path[0]}->${selectedFlow.path[selectedFlow.path.length - 1]}`;
      }
      if (monitorEpoch == null || monitorEpoch === '') {
        const msg = '分析失败：topology_epoch 不能为空';
        setAnalysisError(msg);
        setMonitorActionStatus(msg);
        pushToast(msg, 'warn');
        return;
      }
      setAnalysisSummary({
        mode: analysisMode,
        scopeType,
        scopeId,
        topologyEpoch: String(monitorEpoch),
        entityId,
        linkCount: Object.keys(monitorSnapshot.byLink || {}).length
      });

      const runResp = await monitorClientRef.current.analyzeRun({
        mode: analysisMode,
        scope_type: scopeType,
        scope_id: scopeId,
        topology_epoch: String(monitorEpoch),
        include_debug: false,
        max_depth: analysisMode === 'focused' ? 2 : 4,
        spread_mode: analysisMode === 'focused' ? 'single_point' : 'cascade',
        cascade_threshold: 0.6
      });
      const overviewResult = (() => {
        if (runResp && typeof runResp === 'object' && runResp.contract_version) {
          return runResp;
        }
        return getAnalysisResult(runResp);
      })();
      const spreadResult = overviewResult?.topology_impact || null;
      const impactResult = overviewResult || null;
      const impactedCount = Array.isArray(spreadResult?.impacted_nodes) ? spreadResult.impacted_nodes.length : 0;
      const taskCount = Array.isArray(impactResult?.tasks) ? impactResult.tasks.length : 0;
      if (!spreadResult && !impactResult) {
        const msg = '高级分析返回空结果：后端未返回 result/data';
        setAnalysisError(msg);
        setMonitorActionStatus(msg);
        pushToast(msg, 'warn');
      } else if (impactedCount === 0 && taskCount === 0) {
        const msg = '高级分析已调用，但当前没有可见影响（impacted_nodes=0, tasks=0）';
        setAnalysisError(msg);
        setMonitorActionStatus(msg);
        pushToast(msg, 'warn');
      } else {
        setAnalysisError('');
      }
      setSeriesSnapshot(null);
      setPathAnalysis(null);
      setFaultSpread(spreadResult);
      setTaskImpact(impactResult);
      setAnalysisOverview(overviewResult);
      setMonitorActionStatus('已刷新高级分析结果');
      pushToast('高级分析已更新', 'ok');
      setAnalysisSupported(true);
    } catch (err) {
      const msg = err?.message || '高级分析失败';
      if ([404, 405, 501].includes(err?.status)) {
        setAnalysisSupported(false);
      }
      setAnalysisError(msg);
      setMonitorActionStatus(msg);
      pushToast(msg, 'warn');
    } finally {
      setAnalysisLoading(false);
    }
  }

  async function runSimulationFlow() {
    if (!monitorClientRef.current || !frame) {
      const msg = '推演失败：monitor API 或拓扑帧不可用';
      setMonitorActionStatus(msg);
      pushToast(msg, 'warn');
      return;
    }
    let scopeId = selectedLinkMetric?.linkUid || '';
    if (!scopeId && selectedLink) {
      scopeId = [selectedLink.a.id, selectedLink.b.id].sort().join('<->');
    }
    if (!scopeId) {
      const first = Object.values(monitorSnapshot.byLink || {})[0];
      scopeId = first?.linkUid || '';
    }
    if (!scopeId) {
      const msg = '推演失败：当前无可用链路 scope_id';
      setMonitorActionStatus(msg);
      pushToast(msg, 'warn');
      return;
    }
    try {
      setSimulationLoading(true);
      const scoped = parseScopedLink(scopeId);
      const createResp = await monitorClientRef.current.createSimulation({
        scenario_type: 'link_down',
        topology_epoch: String(monitorEpoch || 1708848000),
        steps_total: 5,
        params: {
          link_id: scopeId,
          src_node_id: selectedLink?.a?.id || scoped?.a || '',
          dst_node_id: selectedLink?.b?.id || scoped?.b || ''
        }
      });
      const simulationId = createResp?.simulation_id || createResp?.result?.simulation_id;
      if (!simulationId) {
        throw new Error('推演创建成功但未返回 simulation_id');
      }
      let status = createResp?.status || createResp?.result?.status || 'running';
      for (let i = 0; i < 8 && status !== 'completed'; i += 1) {
        const stepResp = await monitorClientRef.current.stepSimulation(simulationId, {});
        status = stepResp?.status || stepResp?.result?.status || status;
        if (status === 'completed') {
          break;
        }
      }
      const timelineResp = await monitorClientRef.current.getSimulationTimeline(simulationId);
      const timeline = timelineResp?.timeline || timelineResp?.result?.timeline || [];
      setSimulationResult({
        simulationId,
        status,
        timelineCount: Array.isArray(timeline) ? timeline.length : 0,
        latest: Array.isArray(timeline) && timeline.length > 0 ? timeline[timeline.length - 1] : null
      });
      const msg = `推演完成：${simulationId}，timeline=${Array.isArray(timeline) ? timeline.length : 0}`;
      setMonitorActionStatus(msg);
      pushToast(msg, 'ok');
    } catch (err) {
      const msg = err?.message || '推演失败';
      setMonitorActionStatus(msg);
      pushToast(msg, 'warn');
    } finally {
      setSimulationLoading(false);
    }
  }

  useEffect(() => {
    if (!containerRef.current || viewerRef.current) {
      return;
    }

    const viewer = new Viewer(containerRef.current, {
      imageryProvider: false,
      terrainProvider: new EllipsoidTerrainProvider(),
      animation: false,
      timeline: false,
      baseLayerPicker: false,
      geocoder: false,
      sceneModePicker: false,
      homeButton: false,
      navigationHelpButton: false,
      selectionIndicator: false,
      infoBox: false
    });

    viewer.scene.globe.baseColor = Color.fromCssColorString('#123b58');
    const localTextureUrl = `${(window.CESIUM_BASE_URL || '/cesium').replace(/\/$/, '')}/Assets/Textures/NaturalEarthII`;
    TileMapServiceImageryProvider.fromUrl(localTextureUrl)
      .then((provider) => {
        viewer.imageryLayers.addImageryProvider(provider);
      })
      .catch(() => {
        viewer.imageryLayers.addImageryProvider(
          new OpenStreetMapImageryProvider({ url: 'https://tile.openstreetmap.org/' })
        );
      });
    viewer.camera.flyTo({
      destination: Cartesian3.fromDegrees(110, 25, 22_000_000),
      duration: 0
    });
    const pickHandler = new ScreenSpaceEventHandler(viewer.scene.canvas);
    pickHandler.setInputAction((movement) => {
      const picked = viewer.scene.pick(movement.endPosition);
      if (!defined(picked) || !picked?.id?.id || typeof picked.id.id !== 'string') {
        setHoverInfo(null);
        return;
      }
      if (!picked.id.id.startsWith('node-')) {
        setHoverInfo(null);
        return;
      }
      const nodeId = picked.id.id.slice(5);
      if (!nodeVisibilityRef.current.get(nodeId)) {
        setHoverInfo(null);
        return;
      }
      const node = nodeStateRef.current.get(nodeId);
      if (!node) {
        setHoverInfo(null);
        return;
      }
      setHoverInfo({
        x: movement.endPosition.x,
        y: movement.endPosition.y,
        node
      });
    }, ScreenSpaceEventType.MOUSE_MOVE);
    pickHandler.setInputAction((movement) => {
      const picked = viewer.scene.pick(movement.position);
      if (!defined(picked) || !picked?.id?.id || typeof picked.id.id !== 'string') {
        setSelected(null);
        return;
      }
      const pickedId = picked.id.id;
      if (pickedId.startsWith('node-')) {
        const nodeId = pickedId.slice(5);
        if (!nodeVisibilityRef.current.get(nodeId)) {
          setSelected(null);
          return;
        }
        setSelected({ kind: 'node', id: nodeId });
        return;
      }
      if (pickedId.startsWith('link-')) {
        const linkId = pickedId.slice(5);
        setSelected({ kind: 'link', id: linkId });
        return;
      }
      setSelected(null);
    }, ScreenSpaceEventType.LEFT_CLICK);

    viewerRef.current = viewer;
    pickHandlerRef.current = pickHandler;

    return () => {
      pickHandler.destroy();
      viewer.destroy();
      pickHandlerRef.current = null;
      viewerRef.current = null;
    };
  }, []);

  useEffect(() => {
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;
    ws.onopen = () => {
      setConnected(true);
      lastFrameAtRef.current = Date.now();
      frameTimestampsRef.current = [];
      frameQueueRef.current = [];
      setQueueDepth(0);
      setControlStatus('');
      if (AUTO_CLEAR_FAULTS_ON_CONNECT && !faultInitRef.current) {
        faultInitRef.current = true;
        ws.send(JSON.stringify({ action: 'clear_all_faults', request_id: `req-${Date.now()}` }));
      }
      ws.send(JSON.stringify({ action: 'list_faults', request_id: `req-${Date.now()}` }));
    };
    ws.onclose = () => {
      setConnected(false);
      wsRef.current = null;
    };
    ws.onerror = () => {
      setConnected(false);
      wsRef.current = null;
    };
    ws.onmessage = (evt) => {
      const payload = JSON.parse(evt.data);
      if (payload && payload.type === 'control_ack') {
        if (Array.isArray(payload.faults)) {
          setFaults(payload.faults);
        }
        if (monitorClientRef.current) {
          monitorClientRef.current.ingestFaultControlAck({
            ...payload,
            topology_epoch: String(monitorEpoch || 1708848000)
          }).catch(() => {});
        }
        if (payload.ok) {
          setControlStatus(payload.deduplicated ? '已存在相同故障，已去重' : '控制操作成功');
        } else {
          setControlStatus(payload.error || '控制操作失败');
        }
        return;
      }
      const now = Date.now();
      lastFrameAtRef.current = now;
      frameTimestampsRef.current.push(now);
      frameQueueRef.current.push(payload);
      if (frameQueueRef.current.length > FRAME_QUEUE_MAX) {
        frameQueueRef.current.shift();
      }
      setQueueDepth(frameQueueRef.current.length);
    };
    return () => {
      wsRef.current = null;
      ws.close();
    };
  }, [monitorEpoch]);

  const damagedNodeIds = new Set(
    faults
      .filter((f) => f.fault_type === 'DAMAGED' && f.target?.node_id)
      .map((f) => f.target.node_id)
  );

  useEffect(() => {
    if (playback.paused) {
      return undefined;
    }
    const intervalMs = Math.max(80, Math.floor(1000 / playback.speed));
    const timer = window.setInterval(() => {
      shiftFrameFromQueue();
    }, intervalMs);
    return () => window.clearInterval(timer);
  }, [playback.paused, playback.speed]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      const now = Date.now();
      const stamps = frameTimestampsRef.current.filter((t) => now - t <= 10_000);
      frameTimestampsRef.current = stamps;
      let ingestFps = 0;
      if (stamps.length >= 2) {
        const spanMs = Math.max(1, stamps[stamps.length - 1] - stamps[0]);
        ingestFps = ((stamps.length - 1) * 1000) / spanMs;
      }
      const stalenessMs = connected ? Math.max(0, now - (lastFrameAtRef.current || now)) : 0;
      setRuntimeHealth({ stalenessMs, ingestFps });
    }, 500);
    return () => window.clearInterval(timer);
  }, [connected]);

  useEffect(() => {
    const viewer = viewerRef.current;
    if (!viewer || !frame) {
      return;
    }

    const entities = viewer.entities;
    const activeNodeIds = new Set();
    const nodePositionMap = new Map();
    const frameTick = Math.floor(frame.sim_time_s ?? 0);

    for (const node of frame.nodes) {
      activeNodeIds.add(node.id);
      const position = toCartesian(node);
      nodePositionMap.set(node.id, position);
      const color = typeColor[node.type] || Color.WHITE;
      const nodeVisible = isNodeVisible(node, layerPrefs);
      nodeVisibilityRef.current.set(node.id, nodeVisible);

      const selectedNode = selected?.kind === 'node' && selected.id === node.id;
      const isDamagedNode = damagedNodeIds.has(node.id);
      let nodeEntity = nodeEntitiesRef.current.get(node.id);
      const labelText = node.name || node.id;
      const labelScale = node.type === 'leo' ? 0.45 : 0.35;
      if (!nodeEntity) {
        nodeEntity = entities.add({
          id: `node-${node.id}`,
          name: labelText,
          position,
          billboard: {
            image: nodeIcon[node.type] || nodeIcon.leo,
            width: node.type === 'leo' ? 26 : 24,
            height: node.type === 'leo' ? 26 : 24,
            verticalOrigin: VerticalOrigin.CENTER,
            horizontalOrigin: HorizontalOrigin.CENTER,
            color,
            scale: 1.0
          },
          label: {
            text: labelText,
            show: true,
            scale: labelScale,
            fillColor: Color.WHITE,
            showBackground: true,
            backgroundColor: Color.BLACK.withAlpha(0.55),
            style: LabelStyle.FILL,
            verticalOrigin: VerticalOrigin.BOTTOM,
            pixelOffset: new Cartesian2(0, -12)
          }
        });
        nodeEntitiesRef.current.set(node.id, nodeEntity);
      } else {
        nodeEntity.position = position;
      }
      nodeEntity.show = nodeVisible;
      if (nodeEntity.billboard) {
        nodeEntity.billboard.scale = selectedNode ? 1.35 : 1.0;
        nodeEntity.billboard.color = isDamagedNode
          ? DAMAGED_NODE_COLOR
          : (selectedNode ? SELECTED_NODE_COLOR : color);
      }
      if (nodeEntity.label) {
        nodeEntity.label.show = nodeVisible && layerPrefs.showLabels;
      }

      const trail = trailPointsRef.current.get(node.id) || [];
      trail.push(position);
      const trailLen = TRAIL_LEN_BY_TYPE[node.type] || 180;
      if (trail.length > trailLen) {
        trail.shift();
      }
      trailPointsRef.current.set(node.id, trail);

      let trailEntity = trailEntitiesRef.current.get(node.id);
      if (!trailEntity) {
        trailEntity = entities.add({
          id: `trail-${node.id}`,
          polyline: {
            positions: trail,
            width: node.type === 'leo' ? 1.4 : 2.1,
            material: color.withAlpha(0.45)
          }
        });
        trailEntitiesRef.current.set(node.id, trailEntity);
      } else {
        trailEntity.polyline.positions = trail;
      }
      trailEntity.show = nodeVisible && layerPrefs.showTrails;

      if (node.type === 'leo') {
        let orbitPositions = null;
        const orbitCache = orbitCacheRef.current.get(node.id);
        const shouldRecompute =
          !orbitCache ||
          orbitCache.orbitClass !== node.orbit_class ||
          frameTick - orbitCache.tick >= ORBIT_UPDATE_INTERVAL_TICKS;
        if (shouldRecompute) {
          orbitPositions = buildSatelliteOrbitPolyline(node);
          if (orbitPositions) {
            orbitCacheRef.current.set(node.id, {
              orbitClass: node.orbit_class,
              tick: frameTick,
              positions: orbitPositions
            });
          }
        } else {
          orbitPositions = orbitCache.positions;
        }
        if (orbitPositions) {
          let orbitEntity = orbitEntitiesRef.current.get(node.id);
          if (!orbitEntity) {
            orbitEntity = entities.add({
              id: `orbit-${node.id}`,
              polyline: {
                positions: orbitPositions,
                width: 1,
                material: orbitColor[node.orbit_class] || Color.WHITE.withAlpha(0.15)
              }
            });
            orbitEntitiesRef.current.set(node.id, orbitEntity);
          } else {
            orbitEntity.polyline.positions = orbitPositions;
          }
          orbitEntity.show = nodeVisible && layerPrefs.showOrbits;
        }
      }
    }

    for (const [id, ent] of nodeEntitiesRef.current) {
      if (!activeNodeIds.has(id)) {
        entities.remove(ent);
        nodeEntitiesRef.current.delete(id);
      }
    }
    for (const [id, ent] of trailEntitiesRef.current) {
      if (!activeNodeIds.has(id)) {
        entities.remove(ent);
        trailEntitiesRef.current.delete(id);
        trailPointsRef.current.delete(id);
      }
    }
    for (const [id, ent] of orbitEntitiesRef.current) {
      if (!activeNodeIds.has(id)) {
        entities.remove(ent);
        orbitEntitiesRef.current.delete(id);
        orbitCacheRef.current.delete(id);
      }
    }

    const activeLinks = new Set();
    const linkState = new Map();
    const degreeCount = new Map();
    const nodeMap = new Map(frame.nodes.map((n) => [n.id, n]));
    const faultLinkKeys = new Set(
      faults
        .filter((f) => f.fault_type === 'INTERRUPTED' && f.target?.a && f.target?.b)
        .map((f) => edgeKey(f.target.a, f.target.b))
    );
    for (const edge of frame.links) {
      const a = nodeMap.get(edge.a);
      const b = nodeMap.get(edge.b);
      if (!a || !b) {
        continue;
      }
      degreeCount.set(edge.a, (degreeCount.get(edge.a) || 0) + 1);
      degreeCount.set(edge.b, (degreeCount.get(edge.b) || 0) + 1);
      const linkId = `${edge.a}-${edge.b}`;
      const linkFaulted = faultLinkKeys.has(edgeKey(edge.a, edge.b));
      activeLinks.add(linkId);
      const pa = nodePositionMap.get(a.id);
      const pb = nodePositionMap.get(b.id);
      if (!pa || !pb) {
        continue;
      }
      const positions = [pa, pb];
      const selectedLink = selected?.kind === 'link' && selected.id === linkId;
      const inSelectedFlow = flowEdgeKeys.has(edgeKey(edge.a, edge.b));

      let lineEntity = linkEntitiesRef.current.get(linkId);
      const style = resolveLinkStyle(a, b);
      const linkKind = resolveLinkKind(a, b);
      const visible = isLinkVisible(linkKind, layerPrefs) && isNodeVisible(a, layerPrefs) && isNodeVisible(b, layerPrefs);
      linkState.set(linkId, {
        id: linkId,
        kind: linkKind,
        a,
        b
      });
      if (!lineEntity) {
        lineEntity = entities.add({
          id: `link-${linkId}`,
          polyline: {
            positions,
            width: style.width,
            material: linkFaulted
              ? new PolylineDashMaterialProperty({
                  color: FAULT_LINK_COLOR,
                  dashLength: 12
                })
              : inSelectedFlow
                ? FLOW_HIGHLIGHT_COLOR
              : style.color
          }
        });
        linkEntitiesRef.current.set(linkId, lineEntity);
        linkVisualStateRef.current.set(linkId, {
          width: style.width,
          selected: selectedLink,
          kind: linkKind,
          fault: linkFaulted,
          flow: inSelectedFlow
        });
      } else {
        lineEntity.polyline.positions = positions;
      }
      const visual = linkVisualStateRef.current.get(linkId);
      const baseWidth = linkFaulted ? Math.max(style.width, 2.8) : style.width;
      const expectedWidth = selectedLink ? baseWidth + 1.6 : (inSelectedFlow ? baseWidth + 1.0 : baseWidth);
      const widthChanged = !visual || visual.width !== expectedWidth || visual.selected !== selectedLink;
      if (widthChanged) {
        lineEntity.polyline.width = expectedWidth;
      }
      const materialChanged = !visual || visual.selected !== selectedLink || visual.kind !== linkKind || visual.fault !== linkFaulted || visual.flow !== inSelectedFlow;
      if (materialChanged) {
        if (selectedLink) {
          lineEntity.polyline.material = SELECTED_LINK_COLOR;
        } else if (linkFaulted) {
          lineEntity.polyline.material = new PolylineDashMaterialProperty({
            color: FAULT_LINK_COLOR,
            dashLength: 12
          });
        } else if (inSelectedFlow) {
          lineEntity.polyline.material = FLOW_HIGHLIGHT_COLOR;
        } else {
          lineEntity.polyline.material = style.color;
        }
      }
      lineEntity.show = visible;
      linkVisualStateRef.current.set(linkId, {
        width: expectedWidth,
        selected: selectedLink,
        kind: linkKind,
        fault: linkFaulted,
        flow: inSelectedFlow
      });
    }

    for (const [id, ent] of linkEntitiesRef.current) {
      if (!activeLinks.has(id)) {
        entities.remove(ent);
        linkEntitiesRef.current.delete(id);
        linkVisualStateRef.current.delete(id);
      }
    }

    const activeFaultLinks = new Set();
    for (const fault of faults) {
      if (fault.fault_type !== 'INTERRUPTED') {
        continue;
      }
      const aId = fault.target?.a;
      const bId = fault.target?.b;
      if (!aId || !bId) {
        continue;
      }
      const a = nodeMap.get(aId);
      const b = nodeMap.get(bId);
      if (!a || !b) {
        continue;
      }
      const normalAB = `${aId}-${bId}`;
      const normalBA = `${bId}-${aId}`;
      if (activeLinks.has(normalAB) || activeLinks.has(normalBA)) {
        continue;
      }
      const pa = nodePositionMap.get(aId);
      const pb = nodePositionMap.get(bId);
      if (!pa || !pb) {
        continue;
      }
      const faultId = `fault-${edgeKey(aId, bId)}`;
      activeFaultLinks.add(faultId);
      const selectedFault = selected?.kind === 'link' && selected.id === faultId;
      const linkKind = resolveLinkKind(a, b);
      const visible = isLinkVisible(linkKind, layerPrefs) && isNodeVisible(a, layerPrefs) && isNodeVisible(b, layerPrefs);
      let faultEntity = faultLinkEntitiesRef.current.get(faultId);
      if (!faultEntity) {
        faultEntity = entities.add({
          id: `link-${faultId}`,
          polyline: {
            positions: [pa, pb],
            width: 2.8,
            material: new PolylineDashMaterialProperty({
              color: FAULT_LINK_COLOR,
              dashLength: 12
            })
          }
        });
        faultLinkEntitiesRef.current.set(faultId, faultEntity);
      } else {
        faultEntity.polyline.positions = [pa, pb];
      }
      faultEntity.polyline.width = selectedFault ? 4.2 : 2.8;
      faultEntity.polyline.material = selectedFault
        ? SELECTED_LINK_COLOR
        : new PolylineDashMaterialProperty({
            color: FAULT_LINK_COLOR,
            dashLength: 12
          });
      faultEntity.show = visible;
      linkState.set(faultId, {
        id: faultId,
        kind: linkKind,
        a,
        b
      });
    }
    for (const [id, ent] of faultLinkEntitiesRef.current) {
      if (!activeFaultLinks.has(id)) {
        entities.remove(ent);
        faultLinkEntitiesRef.current.delete(id);
      }
    }

    const nodeState = new Map();
    const topoAlias = new Map();
    for (const node of frame.nodes) {
      const degree = degreeCount.get(node.id) || 0;
      nodeState.set(node.id, {
        ...node,
        degree,
        has_link: degree > 0
      });
      topoAlias.set(normalizeIdentity(node.id), node.id);
      topoAlias.set(normalizeIdentityLoose(node.id), node.id);
      if (node.name) {
        topoAlias.set(normalizeIdentity(node.name), node.id);
        topoAlias.set(normalizeIdentityLoose(node.name), node.id);
      }
    }
    for (const metric of Object.values(monitorSnapshot.byNode)) {
      const topoNodeId = metric.topoNodeId || metric.nodeId || '';
      if (!topoNodeId || !nodeState.has(topoNodeId)) {
        continue;
      }
      const aliases = [
        metric.nodeUid,
        metric.nodeId,
        metric.topoNodeId,
        metric.dockerName
      ];
      for (const alias of aliases) {
        const key = normalizeIdentity(alias);
        const loose = normalizeIdentityLoose(alias);
        if (key) {
          topoAlias.set(key, topoNodeId);
        }
        if (loose) {
          topoAlias.set(loose, topoNodeId);
        }
      }
    }
    nodeStateRef.current = nodeState;
    topoNodeAliasRef.current = topoAlias;
    linkStateRef.current = linkState;

    if (selected?.kind === 'node') {
      const node = nodeState.get(selected.id);
      if (!node || !isNodeVisible(node, layerPrefs)) {
        setSelected(null);
      }
    }
    if (selected?.kind === 'link') {
      const link = linkState.get(selected.id);
      if (!link || !isNodeVisible(link.a, layerPrefs) || !isNodeVisible(link.b, layerPrefs) || !isLinkVisible(link.kind, layerPrefs)) {
        setSelected(null);
      }
    }
  }, [frame, layerPrefs, selected, faults, selectedFlowId, monitorSnapshot.byFlow, monitorSnapshot.byNode]);

  const selectedNode = selected?.kind === 'node' ? nodeStateRef.current.get(selected.id) : null;
  const selectedLink = selected?.kind === 'link' ? linkStateRef.current.get(selected.id) : null;
  const selectedFlow = selectedFlowId ? monitorSnapshot.byFlow[selectedFlowId] : null;
  const flowEdgeKeys = new Set();
  if (selectedFlow?.path?.length > 1) {
    for (let i = 0; i < selectedFlow.path.length - 1; i += 1) {
      flowEdgeKeys.add(edgeKey(selectedFlow.path[i], selectedFlow.path[i + 1]));
    }
  }
  const nodeMetricAliasMap = new Map();
  for (const item of Object.values(monitorSnapshot.byNode)) {
    const aliases = [item.nodeId, item.nodeUid, item.topoNodeId, item.dockerName];
    for (const alias of aliases) {
      const strictKey = normalizeIdentity(alias);
      const looseKey = normalizeIdentityLoose(alias);
      if (strictKey && !nodeMetricAliasMap.has(strictKey)) {
        nodeMetricAliasMap.set(strictKey, { metric: item, by: strictKey });
      }
      if (looseKey && !nodeMetricAliasMap.has(looseKey)) {
        nodeMetricAliasMap.set(looseKey, { metric: item, by: looseKey });
      }
    }
  }
  const selectedNodeMetricHit = selectedNode
    ? (() => {
      const queryKeys = [
        selectedNode.id,
        selectedNode.name
      ].flatMap((v) => [normalizeIdentity(v), normalizeIdentityLoose(v)]).filter(Boolean);
      for (const q of queryKeys) {
        const hit = nodeMetricAliasMap.get(q);
        if (hit) {
          return hit;
        }
      }
      return null;
    })()
    : null;
  const selectedNodeMetric = selectedNodeMetricHit?.metric || null;
  const hoverNodeMetric = hoverInfo?.node
    ? (() => {
      const queryKeys = [
        hoverInfo.node.id,
        hoverInfo.node.name
      ].flatMap((v) => [normalizeIdentity(v), normalizeIdentityLoose(v)]).filter(Boolean);
      for (const q of queryKeys) {
        const hit = nodeMetricAliasMap.get(q);
        if (hit?.metric) {
          return hit.metric;
        }
      }
      return null;
    })()
    : null;
  const selectedLinkMetric = selectedLink
    ? Object.values(monitorSnapshot.byLink).find((item) => {
      if (!item) {
        return false;
      }
      const srcCandidates = [
        item.srcNodeId,
        item.srcNodeUid
      ].map((v) => resolveTopoNodeId(v) || v).map(normalizeIdentity);
      const dstCandidates = [
        item.dstNodeId,
        item.dstNodeUid
      ].map((v) => resolveTopoNodeId(v) || v).map(normalizeIdentity);
      const aId = normalizeIdentity(selectedLink.a.id);
      const bId = normalizeIdentity(selectedLink.b.id);
      return (
        (srcCandidates.includes(aId) && dstCandidates.includes(bId)) ||
          (srcCandidates.includes(bId) && dstCandidates.includes(aId))
      );
    }) || null
    : null;
  const selectedNodeHistory = selectedNode ? metricHistoryRef.current.nodes.get(selectedNode.id) : null;
  const selectedLinkHistory = selectedLinkMetric ? metricHistoryRef.current.links.get(selectedLinkMetric.linkId) : null;
  const selectedNodeHistoryView = {
    cpu: filterSeriesByWindow(selectedNodeHistory?.cpu || [], trendWindowSec),
    mem: filterSeriesByWindow(selectedNodeHistory?.mem || [], trendWindowSec),
    tx: filterSeriesByWindow(selectedNodeHistory?.tx || [], trendWindowSec),
    rx: filterSeriesByWindow(selectedNodeHistory?.rx || [], trendWindowSec)
  };
  const selectedLinkHistoryView = {
    loss: filterSeriesByWindow(selectedLinkHistory?.loss || [], trendWindowSec),
    rtt: filterSeriesByWindow(selectedLinkHistory?.rtt || [], trendWindowSec),
    jitter: filterSeriesByWindow(selectedLinkHistory?.jitter || [], trendWindowSec)
  };
  const monitorHealthClass = monitorSnapshot.health === 'critical'
    ? 'error'
    : monitorSnapshot.health === 'warning'
      ? 'warn'
      : monitorSnapshot.health === 'unknown'
        ? 'warn'
        : 'ok';
  const alerts = [];
  if (!connected) {
    alerts.push({ level: 'error', text: 'WebSocket 已断开' });
  }
  if (connected && runtimeHealth.stalenessMs >= STALE_ERROR_MS) {
    alerts.push({ level: 'error', text: `数据延迟过高：${(runtimeHealth.stalenessMs / 1000).toFixed(1)}s` });
  } else if (connected && runtimeHealth.stalenessMs >= STALE_WARN_MS) {
    alerts.push({ level: 'warn', text: `数据延迟偏高：${(runtimeHealth.stalenessMs / 1000).toFixed(1)}s` });
  }
  if (connected && runtimeHealth.ingestFps > 0 && runtimeHealth.ingestFps < INGEST_FPS_WARN) {
    alerts.push({ level: 'warn', text: `帧率偏低：${runtimeHealth.ingestFps.toFixed(2)} fps` });
  }
  const timelineAlarms = [...monitorSnapshot.topAlarms].sort((a, b) => {
    const ta = Date.parse(a.timestamp || '');
    const tb = Date.parse(b.timestamp || '');
    return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0);
  });
  const filteredTimelineAlarms = timelineAlarms.filter((alarm) => {
    const bySeverity = alarmSeverityFilter === 'all' || alarm.severity === alarmSeverityFilter;
    const byScope = alarmScopeFilter === 'all' || alarm.scopeType === alarmScopeFilter;
    return bySeverity && byScope;
  });
  const scopeUidPresentCount = timelineAlarms.filter((alarm) => Boolean((alarm.scopeUid || '').trim())).length;
  const scopeUidMissingCount = Math.max(0, timelineAlarms.length - scopeUidPresentCount);
  const scopeUidCoverage = timelineAlarms.length > 0 ? (scopeUidPresentCount / timelineAlarms.length) * 100 : 0;
  const scopeUidCoverageWarn = scopeUidCoverage < 95;
  const nodeMetricList = Object.values(monitorSnapshot.byNode);
  const nodeCpuPresentCount = nodeMetricList.filter((item) => item.cpuRatio != null).length;
  const nodeMemPresentCount = nodeMetricList.filter((item) => item.memRatio != null).length;
  const nodeCpuCoverage = nodeMetricList.length > 0 ? (nodeCpuPresentCount / nodeMetricList.length) * 100 : 0;
  const nodeMemCoverage = nodeMetricList.length > 0 ? (nodeMemPresentCount / nodeMetricList.length) * 100 : 0;
  const nodeMetricCoverageWarn = nodeMetricList.length > 0 && (nodeCpuCoverage < 80 || nodeMemCoverage < 80);

  function exportMonitorSnapshotJson() {
    const payload = {
      exported_at: new Date().toISOString(),
      monitor_epoch: monitorEpoch,
      monitor_source_mode: monitorSourceMode,
      monitor_last_success_at: monitorLastSuccessAt || null,
      monitor_failures: monitorConsecutiveFailures,
      collector_health: collectorHealth || null,
      collector_metrics: collectorMetrics || null,
      alarm_filters: {
        severity: alarmSeverityFilter,
        scope: alarmScopeFilter
      },
      scope_uid_stats: {
        total_alarms: timelineAlarms.length,
        present: scopeUidPresentCount,
        missing: scopeUidMissingCount,
        coverage_percent: Number(scopeUidCoverage.toFixed(2))
      },
      monitor_snapshot: monitorSnapshot
    };
    const text = JSON.stringify(payload, null, 2);
    const blob = new Blob([text], { type: 'application/json;charset=utf-8' });
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = `monitor_snapshot_${Date.now()}.json`;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.URL.revokeObjectURL(url);
    setMonitorActionStatus('已导出 monitor 快照 JSON');
    pushToast('已导出 monitor 快照 JSON', 'ok');
  }

  function importMonitorSnapshotJsonFile(file) {
    if (!file) {
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const parsed = JSON.parse(String(reader.result || '{}'));
        const snapshot = parsed.monitor_snapshot || parsed.monitor || null;
        if (!snapshot) {
          const msg = '导入失败：文件中缺少 monitor_snapshot';
          setMonitorActionStatus(msg);
          pushToast(msg, 'warn');
          return;
        }
        setMonitorSnapshot((prev) => applyMonitorSnapshot(prev, snapshot));
        if (parsed.monitor_epoch != null && Number.isFinite(Number(parsed.monitor_epoch))) {
          setMonitorEpoch(Number(parsed.monitor_epoch));
        }
        setMonitorSourceMode('replay');
        setMonitorError('');
        setReplayMode(true);
        setMonitorActionStatus(`已导入回放文件：${file.name}`);
        pushToast(`已导入回放文件：${file.name}`, 'ok');
      } catch {
        const msg = '导入失败：JSON 解析错误';
        setMonitorActionStatus(msg);
        pushToast(msg, 'warn');
      }
    };
    reader.onerror = () => {
      const msg = '导入失败：文件读取错误';
      setMonitorActionStatus(msg);
      pushToast(msg, 'warn');
    };
    reader.readAsText(file, 'utf-8');
  }

  return (
    <div className="app-shell">
      <div className="hud">
        <h1>Dynamic Topology - Deploy Check 2026-02-20</h1>
        <p>Status: {connected ? 'connected' : 'disconnected'}</p>
        <div className="status-badges">
          <span className={`badge ${connected ? 'ok' : 'error'}`}>{connected ? '连接正常' : '连接中断'}</span>
          <span className={`badge ${runtimeHealth.stalenessMs >= STALE_ERROR_MS ? 'error' : runtimeHealth.stalenessMs >= STALE_WARN_MS ? 'warn' : 'ok'}`}>
            延迟 {runtimeHealth.stalenessMs}ms
          </span>
          <span className={`badge ${runtimeHealth.ingestFps > 0 && runtimeHealth.ingestFps < INGEST_FPS_WARN ? 'warn' : 'ok'}`}>
            帧率 {runtimeHealth.ingestFps.toFixed(2)}fps
          </span>
        </div>
        <div className="time-controls">
          <button type="button" onClick={() => setPlayback((p) => ({ ...p, paused: !p.paused }))}>
            {playback.paused ? '继续' : '暂停'}
          </button>
          <button type="button" onClick={stepOnce}>单步</button>
          {SPEED_OPTIONS.map((sp) => (
            <button
              type="button"
              key={`speed-${sp}`}
              className={playback.speed === sp ? 'active' : ''}
              onClick={() => setPlayback((p) => ({ ...p, speed: sp }))}
            >
              {sp}x
            </button>
          ))}
          <span className="queue-chip">缓冲 {queueDepth}</span>
        </div>
        <p>WS: {WS_URL}</p>
        <p>t: {frame ? frame.sim_time_s.toFixed(1) : '-'} s</p>
        <p>nodes: {frame ? frame.nodes.length : 0}</p>
        <p>links: {frame ? frame.metrics.edge_count : 0}</p>
        <p>avg degree: {frame ? frame.metrics.avg_degree.toFixed(2) : '-'}</p>
        <p>mobile connected: {frame ? `${frame.metrics.mobile_connected_count ?? 0}/${(frame.nodes.filter((n) => n.type !== 'leo').length || 1)}` : '-'}</p>
        <p>mobile ratio: {frame ? `${((frame.metrics.mobile_connected_ratio ?? 0) * 100).toFixed(1)}%` : '-'}</p>
        <p>I(QoE-Imbalance): {frame ? (frame.metrics.qoe_imbalance ?? 0).toFixed(4) : '-'}</p>
        <p>fault nodes: {frame ? frame.metrics.fault_node_count ?? 0 : 0}</p>
        <p>fault links: {frame ? frame.metrics.fault_link_count ?? 0 : 0}</p>
        <p>tick: {frame ? frame.elapsed_ms.toFixed(2) : '-'} ms</p>
        <p>control: {controlStatus || '-'}</p>
        <div className="alert-box">
          {alerts.length === 0 ? (
            <div className="alert-row ok">当前无告警</div>
          ) : (
            alerts.map((a, idx) => (
              <div key={`${a.level}-${idx}`} className={`alert-row ${a.level}`}>
                {a.text}
              </div>
            ))
          )}
        </div>
        <div className="legend">
          <div className="legend-item"><span className="swatch orbit" />satellite orbit</div>
          <div className="legend-item"><span className="swatch sat-sat" />satellite-satellite link</div>
          <div className="legend-item"><span className="swatch sat-mobile" />satellite-air/ship link</div>
        </div>
        <div className="layer-panel">
          <div className="layer-header">
            <span>图层控制</span>
            <div className="monitor-header-actions">
              <button type="button" onClick={() => setShowLayerPanel((v) => !v)}>{showLayerPanel ? '收起' : '展开'}</button>
              <button type="button" onClick={resetLayerPrefs}>重置</button>
            </div>
          </div>
          {showLayerPanel ? <div className="layer-grid">
            <label><input type="checkbox" checked={layerPrefs.nodeLeo} onChange={() => toggleLayer('nodeLeo')} /> 卫星</label>
            <label><input type="checkbox" checked={layerPrefs.nodeAircraft} onChange={() => toggleLayer('nodeAircraft')} /> 飞机</label>
            <label><input type="checkbox" checked={layerPrefs.nodeShip} onChange={() => toggleLayer('nodeShip')} /> 舰船</label>
            <label><input type="checkbox" checked={layerPrefs.linkSatSat} onChange={() => toggleLayer('linkSatSat')} /> 星间链路</label>
            <label><input type="checkbox" checked={layerPrefs.linkSatMobile} onChange={() => toggleLayer('linkSatMobile')} /> 星地/空链路</label>
            <label><input type="checkbox" checked={layerPrefs.linkOther} onChange={() => toggleLayer('linkOther')} /> 非卫星链路</label>
            <label><input type="checkbox" checked={layerPrefs.showTrails} onChange={() => toggleLayer('showTrails')} /> 轨迹</label>
            <label><input type="checkbox" checked={layerPrefs.showOrbits} onChange={() => toggleLayer('showOrbits')} /> 轨道环</label>
            <label><input type="checkbox" checked={layerPrefs.showLabels} onChange={() => toggleLayer('showLabels')} /> 标签</label>
          </div> : <div className="fault-empty">已收起</div>}
        </div>
        <div className="fault-panel">
          <div className="layer-header">
            <span>故障面板</span>
            <div className="monitor-header-actions">
              <button type="button" onClick={() => setShowFaultPanel((v) => !v)}>{showFaultPanel ? '收起' : '展开'}</button>
              <button type="button" onClick={() => sendControl('list_faults')}>刷新</button>
            </div>
          </div>
          {showFaultPanel ? <div className="fault-list">
            {faults.length === 0 ? (
              <div className="fault-empty">当前无故障注入</div>
            ) : (
              faults.map((fault) => (
                <div key={fault.fault_id} className="fault-row">
                  <div className="fault-row-title">{fault.fault_type}</div>
                  <div className="fault-row-target">
                    {fault.fault_type === 'DAMAGED'
                      ? `node=${fault.target?.node_id || '-'}`
                      : `a=${fault.target?.a || '-'}, b=${fault.target?.b || '-'}`}
                  </div>
                  <div className="fault-row-actions">
                    <button type="button" onClick={() => focusFaultTarget(fault)}>定位</button>
                    <button type="button" onClick={() => sendControl('clear_fault', { fault_id: fault.fault_id })}>解除</button>
                  </div>
                </div>
              ))
            )}
          </div> : <div className="fault-empty">已收起</div>}
          {showFaultPanel ? <div className="fault-row-actions fault-footer-actions">
            <button type="button" onClick={() => sendControl('clear_all_faults')}>解除全部故障</button>
          </div> : null}
        </div>
      </div>
      {hoverInfo ? (
        <div
          className="node-tooltip"
          style={{
            left: `${Math.min(hoverInfo.x + 14, window.innerWidth - 260)}px`,
            top: `${Math.min(hoverInfo.y + 14, window.innerHeight - 180)}px`
          }}
        >
          <div className="title">{hoverInfo.node.name}</div>
          <div>id: {hoverInfo.node.id}</div>
          <div>type: {hoverInfo.node.category}</div>
          <div>orbit: {hoverInfo.node.orbit_class || '-'}</div>
          <div>links: {hoverInfo.node.has_link ? `yes (degree ${hoverInfo.node.degree})` : 'no'}</div>
          <div>alt: {hoverInfo.node.alt_m.toFixed(0)} m</div>
          <div>cpu: {hoverNodeMetric?.cpuRatio != null ? `${(hoverNodeMetric.cpuRatio * 100).toFixed(1)}%` : '--'}</div>
          <div>mem: {hoverNodeMetric?.memRatio != null ? `${(hoverNodeMetric.memRatio * 100).toFixed(1)}%` : '--'}</div>
        </div>
      ) : null}
      <aside className={`detail-panel ${(selected || selectedFlow) ? 'show' : ''}`}>
        <div className="detail-header">
          <strong>详情侧栏</strong>
          <button type="button" onClick={() => { setSelected(null); setSelectedFlowId(null); }}>关闭</button>
        </div>
        <div className="trend-window-switch">
          {TREND_WINDOW_OPTIONS.map((sec) => (
            <button
              key={`trend-window-${sec}`}
              type="button"
              className={trendWindowSec === sec ? 'active' : ''}
              onClick={() => setTrendWindowSec(sec)}
            >
              {Math.round(sec / 60)}m
            </button>
          ))}
        </div>
        {!selectedNode && !selectedLink && !selectedFlow ? (
          <div className="detail-empty">点击节点或链路查看详情</div>
        ) : null}
        {selectedNode ? (
          <div className="detail-block">
            <div className="detail-title">节点 {selectedNode.name}</div>
            <div>id: {selectedNode.id}</div>
            <div>类别: {selectedNode.category}</div>
            <div>轨道: {selectedNode.orbit_class || '-'}</div>
            <div>纬度: {selectedNode.lat.toFixed(3)}</div>
            <div>经度: {selectedNode.lon.toFixed(3)}</div>
            <div>高度: {selectedNode.alt_m.toFixed(0)} m</div>
            <div>连通: {selectedNode.has_link ? `是（度 ${selectedNode.degree}）` : '否'}</div>
            <div>docker: {selectedNodeMetric?.dockerName || '-'}</div>
            <div>docker ip: {selectedNodeMetric?.dockerIp || '-'}</div>
            <div>monitor match: {selectedNodeMetric ? (selectedNodeMetric.nodeId || selectedNodeMetric.nodeUid || '-') : 'unmatched'}</div>
            <div>monitor health: {selectedNodeMetric?.health || '-'}</div>
            <div>cpu: {selectedNodeMetric?.cpuRatio != null ? `${(selectedNodeMetric.cpuRatio * 100).toFixed(1)}%` : '--'}</div>
            <div>mem: {selectedNodeMetric?.memRatio != null ? `${(selectedNodeMetric.memRatio * 100).toFixed(1)}%` : '--'}</div>
            <div>tx/rx: {selectedNodeMetric?.txBps != null ? selectedNodeMetric.txBps.toFixed(1) : '--'} / {selectedNodeMetric?.rxBps != null ? selectedNodeMetric.rxBps.toFixed(1) : '--'} bps</div>
            <div className="trend-group">
              <div className="trend-title">趋势窗口（{Math.round(trendWindowSec / 60)} 分钟）</div>
              <div className="trend-row">
                <span>cpu</span>
                <svg viewBox="0 0 156 36" className="sparkline">
                  <path d={buildSparklinePath(selectedNodeHistoryView.cpu)} />
                </svg>
              </div>
              <div className="trend-row">
                <span>mem</span>
                <svg viewBox="0 0 156 36" className="sparkline">
                  <path d={buildSparklinePath(selectedNodeHistoryView.mem)} />
                </svg>
              </div>
              <div className="trend-row">
                <span>tx</span>
                <svg viewBox="0 0 156 36" className="sparkline">
                  <path d={buildSparklinePath(selectedNodeHistoryView.tx)} />
                </svg>
              </div>
              <div className="trend-row">
                <span>rx</span>
                <svg viewBox="0 0 156 36" className="sparkline">
                  <path d={buildSparklinePath(selectedNodeHistoryView.rx)} />
                </svg>
              </div>
            </div>
            <div className="detail-actions">
              <button type="button" onClick={() => sendControl('inject_node_fault', { node_id: selectedNode.id })}>
                注入节点故障
              </button>
            </div>
          </div>
        ) : null}
        {selectedLink ? (
          <div className="detail-block">
            <div className="detail-title">链路 {selectedLink.id}</div>
            <div>类型: {selectedLink.kind}</div>
            <div>A: {selectedLink.a.name} ({selectedLink.a.id})</div>
            <div>B: {selectedLink.b.name} ({selectedLink.b.id})</div>
            <div>A 类别: {selectedLink.a.category}</div>
            <div>B 类别: {selectedLink.b.category}</div>
            <div>A 高度: {selectedLink.a.alt_m.toFixed(0)} m</div>
            <div>B 高度: {selectedLink.b.alt_m.toFixed(0)} m</div>
            <div>link uid: {selectedLinkMetric?.linkUid || '-'}</div>
            <div>monitor health: {selectedLinkMetric?.health || '-'}</div>
            <div>loss: {selectedLinkMetric?.lossRate != null ? `${(selectedLinkMetric.lossRate * 100).toFixed(2)}%` : '--'}</div>
            <div>rtt/jitter: {selectedLinkMetric?.rttMs != null ? `${selectedLinkMetric.rttMs.toFixed(1)}ms` : '--'} / {selectedLinkMetric?.jitterMs != null ? `${selectedLinkMetric.jitterMs.toFixed(1)}ms` : '--'}</div>
            <div className="trend-group">
              <div className="trend-title">趋势窗口（{Math.round(trendWindowSec / 60)} 分钟）</div>
              <div className="trend-row">
                <span>loss</span>
                <svg viewBox="0 0 156 36" className="sparkline">
                  <path d={buildSparklinePath(selectedLinkHistoryView.loss)} />
                </svg>
              </div>
              <div className="trend-row">
                <span>rtt</span>
                <svg viewBox="0 0 156 36" className="sparkline">
                  <path d={buildSparklinePath(selectedLinkHistoryView.rtt)} />
                </svg>
              </div>
              <div className="trend-row">
                <span>jitter</span>
                <svg viewBox="0 0 156 36" className="sparkline">
                  <path d={buildSparklinePath(selectedLinkHistoryView.jitter)} />
                </svg>
              </div>
            </div>
            <div className="detail-actions">
              <button
                type="button"
                onClick={() => sendControl('inject_link_fault', { a: selectedLink.a.id, b: selectedLink.b.id })}
              >
                注入链路故障
              </button>
            </div>
          </div>
        ) : null}
        {selectedFlow ? (
          <div className="detail-block">
            <div className="detail-title">流 {selectedFlow.flowId}</div>
            <div>src: {selectedFlow.srcNodeId || '-'}</div>
            <div>dst: {selectedFlow.dstNodeId || '-'}</div>
            <div>bps: {selectedFlow.bps != null ? selectedFlow.bps.toFixed(1) : '--'}</div>
            <div>priority: {selectedFlow.priority || '-'}</div>
            <div>path: {(selectedFlow.path || []).join(' -> ') || '-'}</div>
          </div>
        ) : null}
      </aside>
      <div className="monitor-dock">
        <div className="monitor-panel">
          <div className="layer-header">
            <span>Monitor 摘要</span>
            <div className="monitor-header-actions">
              <span className={`badge ${monitorHealthClass}`}>{monitorSnapshot.health}</span>
              <button type="button" onClick={exportMonitorSnapshotJson}>导出JSON</button>
              <button type="button" onClick={() => replayFileInputRef.current?.click()}>导入回放</button>
              {replayMode ? (
                <button
                  type="button"
                  onClick={() => {
                    setReplayMode(false);
                    setMonitorSourceMode('snapshot_connecting');
                    setMonitorActionStatus('已退出回放模式，恢复实时拉取');
                    pushToast('已退出回放模式，恢复实时拉取', 'ok');
                  }}
                >
                  退出回放
                </button>
              ) : null}
            </div>
          </div>
          <input
            ref={replayFileInputRef}
            type="file"
            accept="application/json,.json"
            className="monitor-file-input"
            onChange={(e) => {
              const file = e.target.files && e.target.files[0];
              importMonitorSnapshotJsonFile(file || null);
              e.target.value = '';
            }}
          />
          <p>mode: {monitorSourceMode}</p>
          <p>failures: {monitorConsecutiveFailures}</p>
          <p>last success: {monitorLastSuccessAt ? new Date(monitorLastSuccessAt).toLocaleTimeString() : '-'}</p>
          <p>collector nats: {collectorHealth?.nats_connected == null ? '-' : (collectorHealth.nats_connected ? 'up' : 'down')}</p>
          <p>collector total: {collectorMetrics?.total ?? collectorMetrics?.requests_total ?? collectorMetrics?.request_total ?? '-'}</p>
          <p>collector success_rate: {collectorMetrics?.success_rate != null ? Number(collectorMetrics.success_rate).toFixed(2) : (collectorMetrics?.availability != null ? Number(collectorMetrics.availability).toFixed(2) : '-')}</p>
          <div className="monitor-epoch-row">
            <span>epoch</span>
            <select
              value={monitorEpoch ?? ''}
              onChange={(e) => setMonitorEpoch(e.target.value ? Number(e.target.value) : null)}
            >
              {monitorAvailableEpochs.length === 0 ? (
                <option value={monitorEpoch ?? ''}>{monitorEpoch ?? '-'}</option>
              ) : (
                monitorAvailableEpochs.map((epoch) => (
                  <option key={`epoch-${epoch}`} value={epoch}>{epoch}</option>
                ))
              )}
            </select>
          </div>
          <p>updated: {monitorSnapshot.updatedAt || '-'}</p>
          <p>nodes: {monitorSnapshot.nodeCount}</p>
          <p>links: {monitorSnapshot.linkCount}</p>
          <p>flows: {monitorSnapshot.flowCount}</p>
          <p>alarms: {monitorSnapshot.alarmCount}</p>
          <p>critical alarms: {monitorSnapshot.criticalAlarmCount}</p>
          <p>warning alarms: {monitorSnapshot.warningAlarmCount}</p>
          <div className="coverage-wrap">
            <div className="coverage-head">
              <span>scope_uid 覆盖</span>
              <span>{scopeUidCoverage.toFixed(1)}% / 缺失 {scopeUidMissingCount}</span>
            </div>
            <div className={`coverage-bar ${scopeUidCoverageWarn ? 'warn' : ''}`}>
              <div style={{ width: `${Math.max(0, Math.min(100, scopeUidCoverage))}%` }} />
            </div>
          </div>
          <div className="coverage-wrap">
            <div className="coverage-head">
              <span>node 指标覆盖</span>
              <span>cpu {nodeCpuCoverage.toFixed(1)}% / mem {nodeMemCoverage.toFixed(1)}%</span>
            </div>
            <div className={`coverage-bar ${nodeMetricCoverageWarn ? 'warn' : ''}`}>
              <div style={{ width: `${Math.max(0, Math.min(100, Math.min(nodeCpuCoverage, nodeMemCoverage)))}%` }} />
            </div>
          </div>
          {monitorError ? <p>error: {monitorError}</p> : null}
          {monitorActionStatus ? <p className="monitor-action-note">最近操作: {monitorActionStatus}</p> : null}
          <div className="monitor-filter-row">
            <select value={alarmSeverityFilter} onChange={(e) => setAlarmSeverityFilter(e.target.value)}>
              <option value="all">severity: all</option>
              <option value="critical">critical</option>
              <option value="warning">warning</option>
              <option value="info">info</option>
            </select>
            <select value={alarmScopeFilter} onChange={(e) => setAlarmScopeFilter(e.target.value)}>
              <option value="all">scope: all</option>
              <option value="node">node</option>
              <option value="link">link</option>
              <option value="path">path</option>
            </select>
          </div>
          <div className="monitor-alarm-list">
            {filteredTimelineAlarms.length === 0 ? (
              <div className="fault-empty">暂无 monitor 告警</div>
            ) : (
              filteredTimelineAlarms.slice(0, 6).map((alarm) => (
                <div
                  key={alarm.id}
                  className={`monitor-alarm-row severity-${alarm.severity || 'info'} ${selectedMonitorAlarmId === alarm.id ? 'active' : ''}`}
                >
                  <div className="monitor-alarm-main">
                    <strong>{alarm.title}</strong>
                  </div>
                  <div className="monitor-alarm-sub">{alarm.timestamp || '-'} | {alarm.scopeType}/{alarm.scopeId}</div>
                  <button type="button" onClick={() => focusMonitorAlarm(alarm)}>定位</button>
                </div>
              ))
            )}
          </div>

          <div className="monitor-flow-list">
            <div className="layer-header">
              <span>flow 路径</span>
              <div className="monitor-header-actions">
                <button type="button" onClick={() => setShowFlowPanel((v) => !v)}>{showFlowPanel ? '收起' : '展开'}</button>
                <button type="button" onClick={() => setSelectedFlowId(null)}>清除高亮</button>
              </div>
            </div>
            {!showFlowPanel ? (
              <div className="fault-empty">已收起</div>
            ) : Object.values(monitorSnapshot.byFlow).length === 0 ? (
              <div className="fault-empty">暂无 flow 数据</div>
            ) : (
              Object.values(monitorSnapshot.byFlow).slice(0, 3).map((flow) => (
                <div key={flow.flowId} className={`monitor-flow-row ${selectedFlowId === flow.flowId ? 'active' : ''}`}>
                  <div><strong>{flow.flowId}</strong> {flow.bps != null ? `${flow.bps.toFixed(1)} bps` : ''}</div>
                  <div className="monitor-alarm-sub">{(flow.path || []).join(' -> ') || '-'}</div>
                  <button type="button" onClick={() => setSelectedFlowId(flow.flowId)}>高亮路径</button>
                </div>
              ))
            )}
          </div>
          <div className="monitor-analysis">
            <div className="layer-header">
              <span>高级分析</span>
              <div className="monitor-header-actions">
                <button type="button" onClick={runAdvancedAnalysis} disabled={analysisLoading || !analysisSupported}>{analysisLoading ? '分析中...' : (analysisSupported ? '运行分析' : '接口不可用')}</button>
                <button type="button" onClick={runSimulationFlow} disabled={simulationLoading}>{simulationLoading ? '推演中...' : '运行推演'}</button>
              </div>
            </div>
            {analysisError ? <div className="analysis-error">{analysisError}</div> : null}
            {analysisSummary ? (
              <div className="analysis-block">
                <div><strong>Request</strong>: mode={analysisSummary.mode || '-'}</div>
                <div>scope: {analysisSummary.scopeType || '-'} / {analysisSummary.scopeId || '-'}</div>
                <div>epoch: {analysisSummary.topologyEpoch || '-'}</div>
                <div>entity: {analysisSummary.entityId || '-'}</div>
                <div>links: {analysisSummary.linkCount ?? '-'}</div>
              </div>
            ) : null}
            {seriesSnapshot ? (
              <div className="analysis-block">
                <div><strong>Series</strong>: metric=rtt_ms</div>
                <div>points: {seriesSnapshot.points?.length ?? seriesSnapshot.items?.length ?? seriesSnapshot.series?.length ?? '-'}</div>
              </div>
            ) : null}
            {pathAnalysis ? (
              <div className="analysis-block">
                <div><strong>Path</strong>: {pathAnalysis.src || '-'} -&gt; {pathAnalysis.dst || '-'}</div>
                <div>TopN: {pathAnalysis.top_n ?? '-'}, candidates: {pathAnalysis.total_candidates ?? '-'}</div>
                <div>paths: {pathAnalysis.paths?.length ?? 0}, best score: {pathAnalysis.paths?.[0]?.score ?? '-'}</div>
              </div>
            ) : null}
            {faultSpread ? (
              <div className="analysis-block">
                <div><strong>Topology Impact</strong></div>
                <div>seeds: {faultSpread.seed_nodes?.length ?? 0}, impacted_nodes: {faultSpread.impacted_nodes?.length ?? 0}</div>
                <div>impacted_links: {faultSpread.impacted_links?.length ?? 0}, boundary: {faultSpread.boundary_nodes?.length ?? 0}</div>
              </div>
            ) : null}
            {taskImpact ? (
              <div className="analysis-block">
                <div><strong>Overview</strong>: {taskImpact.contract_version || 'analysis.v1'}</div>
                <div>risk: {taskImpact.summary?.risk_level || '-'}, tasks: {taskImpact.summary?.task_total ?? taskImpact.tasks?.length ?? '-'}</div>
                <div>high_priority: {taskImpact.summary?.high_priority_tasks ?? '-'}, avg_score: {taskImpact.summary?.average_priority_score ?? '-'}</div>
                <div>alerts: {taskImpact.alerts?.length ?? 0}, top_task: {taskImpact.tasks?.[0]?.task_id || '-'}</div>
              </div>
            ) : null}
            {Array.isArray(taskImpact?.clusters) && taskImpact.clusters.length > 0 ? (
              <div className="analysis-block">
                <div><strong>故障簇</strong>: {taskImpact.clusters.length}</div>
                {taskImpact.clusters.slice(0, 3).map((c) => (
                  <div key={c.cluster_id}>
                    {c.cluster_id}: seeds(node/link)={c.seed_nodes?.length || 0}/{c.seed_links?.length || 0},
                    impacted(node/link)={c.impacted_nodes_count || 0}/{c.impacted_links_count || 0},
                    contribution={(Number(c.contribution_ratio || 0) * 100).toFixed(1)}%
                  </div>
                ))}
              </div>
            ) : null}
            {taskImpact?.narrative ? (
              <div className="analysis-block">
                <div><strong>人类可读结论</strong>: {taskImpact.narrative.verdict || '-'}</div>
                <div>{taskImpact.narrative.summary_sentence || '-'}</div>
                <div>建议动作: {taskImpact.narrative.next_action || '-'}</div>
                <div>优先任务: {taskImpact.narrative.top_task_id || '-'}</div>
              </div>
            ) : null}
            {simulationResult ? (
              <div className="analysis-block">
                <div><strong>Simulation</strong>: {simulationResult.simulationId}</div>
                <div>status: {simulationResult.status || '-'}</div>
                <div>timeline: {simulationResult.timelineCount}</div>
                <div>latest_risk: {simulationResult.latest?.risk_level || simulationResult.latest?.risk || '-'}</div>
              </div>
            ) : null}
          </div>
        </div>
      </div>
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      {toast ? <div className={`toast ${toast.level}`}>{toast.text}</div> : null}
    </div>
  );
}
