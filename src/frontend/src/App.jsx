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
const IMPACTED_NODE_COLOR = Color.fromCssColorString('#ffd166');
const SIM_IMPACTED_NODE_COLOR = Color.fromCssColorString('#7bdff2');
const IMPACTED_LINK_COLOR = Color.fromCssColorString('#8ac926').withAlpha(0.96);
const SIM_IMPACTED_LINK_COLOR = Color.fromCssColorString('#3a86ff').withAlpha(0.96);
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

function normalizeLinkPair(a, b) {
  const x = String(a || '').trim();
  const y = String(b || '').trim();
  if (!x || !y) {
    return '';
  }
  return x < y ? `${x}<->${y}` : `${y}<->${x}`;
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

function sampleByGranularity(points, granularity) {
  if (!Array.isArray(points) || points.length === 0) {
    return [];
  }
  const step = granularity === 'week' ? 8 : granularity === 'day' ? 4 : granularity === 'hour' ? 2 : 1;
  return points.filter((_, idx) => idx % step === 0 || idx === points.length - 1);
}

function buildTrendPaths(historyPoints, forecastPoints, width = 620, height = 220, padding = { left: 54, right: 14, top: 14, bottom: 34 }) {
  const hist = Array.isArray(historyPoints) ? historyPoints : [];
  const pred = Array.isArray(forecastPoints) ? forecastPoints : [];
  if (hist.length === 0 && pred.length === 0) {
    return {
      historyPath: '',
      forecastPath: '',
      min: 0,
      max: 1,
      toX: () => 0,
      toY: () => 0,
      plot: { x0: padding.left, y0: padding.top, w: width - padding.left - padding.right, h: height - padding.top - padding.bottom },
      totalCount: 0
    };
  }
  const all = [...hist, ...pred];
  const values = all.map((p) => Number(p.v)).filter((v) => Number.isFinite(v));
  if (values.length === 0) {
    return {
      historyPath: '',
      forecastPath: '',
      min: 0,
      max: 1,
      toX: () => 0,
      toY: () => 0,
      plot: { x0: padding.left, y0: padding.top, w: width - padding.left - padding.right, h: height - padding.top - padding.bottom },
      totalCount: 0
    };
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(1e-6, max - min);
  const totalCount = Math.max(2, all.length);
  const plot = {
    x0: padding.left,
    y0: padding.top,
    w: Math.max(10, width - padding.left - padding.right),
    h: Math.max(10, height - padding.top - padding.bottom)
  };
  const xStep = plot.w / (totalCount - 1);
  const toX = (index) => plot.x0 + index * xStep;
  const toY = (value) => {
    const y = plot.y0 + plot.h - ((Number(value) - min) / span) * plot.h;
    return y;
  };
  const toXY = (value, index) => {
    const x = toX(index);
    const y = toY(value);
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  };

  let historyPath = '';
  hist.forEach((p, idx) => {
    historyPath += `${idx === 0 ? 'M' : 'L'}${toXY(p.v, idx)} `;
  });
  historyPath = historyPath.trim();

  let forecastPath = '';
  if (pred.length > 0) {
    const offset = hist.length;
    pred.forEach((p, idx) => {
      const absoluteIdx = offset + idx;
      forecastPath += `${idx === 0 ? 'M' : 'L'}${toXY(p.v, absoluteIdx)} `;
    });
    forecastPath = forecastPath.trim();
  }
  return { historyPath, forecastPath, min, max, toX, toY, plot, totalCount };
}

function formatTrendTimeLabel(t, granularity, fallback = '-') {
  const num = Number(t);
  if (!Number.isFinite(num) || num <= 0) {
    return fallback;
  }
  const d = new Date(num);
  if (Number.isNaN(d.getTime())) {
    return fallback;
  }
  if (granularity === 'week') {
    return `${d.getMonth() + 1}/${d.getDate()}`;
  }
  if (granularity === 'day') {
    return `${d.getDate()}日`;
  }
  if (granularity === 'hour') {
    return `${String(d.getHours()).padStart(2, '0')}:00`;
  }
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
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
  const [analysisDirectReason, setAnalysisDirectReason] = useState('');
  const [analysisAiReport, setAnalysisAiReport] = useState('');
  const [analysisAiMeta, setAnalysisAiMeta] = useState(null);
  const [analysisAiLoading, setAnalysisAiLoading] = useState(false);
  const [analysisAiError, setAnalysisAiError] = useState('');
  const [replayMode, setReplayMode] = useState(false);
  const [showLayerPanel, setShowLayerPanel] = useState(false);
  const [showFaultPanel, setShowFaultPanel] = useState(false);
  const [showFlowPanel, setShowFlowPanel] = useState(false);
  const [showMonitorDiag, setShowMonitorDiag] = useState(false);
  const [showCandidatePanel, setShowCandidatePanel] = useState(true);
  const [showAnalysisPanel, setShowAnalysisPanel] = useState(true);
  const [activeCandidateKey, setActiveCandidateKey] = useState('');
  const [showReportDrawer, setShowReportDrawer] = useState(false);
  const [reportViewMode, setReportViewMode] = useState('wide');
  const [reportGranularity, setReportGranularity] = useState('min');
  const [showRuntimeDrawer, setShowRuntimeDrawer] = useState(false);
  const [bottomTab, setBottomTab] = useState('fault_analysis');
  const [showLayerDrawer, setShowLayerDrawer] = useState(false);
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
  const aiExplainSeqRef = useRef(0);

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
  function focusFaultCandidate(candidate) {
    if (!candidate) {
      return;
    }
    setActiveCandidateKey(candidate.key || '');
    if (candidate.scopeType === 'node') {
      setSelected({ kind: 'node', id: candidate.scopeId });
      const viewer = viewerRef.current;
      const node = nodeStateRef.current.get(candidate.scopeId);
      if (viewer && node) {
        viewer.camera.flyTo({
          destination: Cartesian3.fromDegrees(node.lon, node.lat, node.alt_m + 1_200_000),
          duration: 0.8
        });
      }
      return;
    }
    const p = parseScopedLink(candidate.scopeId);
    if (p) {
      focusLinkByNodes(p.a, p.b);
    }
  }

  function candidateFromSelected(candidates) {
    if (!Array.isArray(candidates) || candidates.length === 0) {
      return null;
    }
    if (selected?.kind === 'node' && selected?.id) {
      return candidates.find((x) => x.scopeType === 'node' && x.scopeId === selected.id) || null;
    }
    if (selected?.kind === 'link' && selectedLink) {
      const uid = normalizeLinkPair(selectedLink.a?.id, selectedLink.b?.id);
      if (!uid) {
        return null;
      }
      return candidates.find((x) => x.scopeType === 'link' && normalizeLinkPair(...(x.scopeId || '').split('<->')) === uid) || null;
    }
    return null;
  }

  async function runAdvancedAnalysis(forcedScope = null) {
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
      setAnalysisSummary(null);
      setAnalysisDirectReason('');
      setAnalysisAiReport('');
      setAnalysisAiMeta(null);
      setAnalysisAiError('');
      setFaultSpread(null);
      setTaskImpact(null);
      let analysisMode = 'auto';
      let scopeType = 'network';
      let scopeId = 'all';
      let entityId = '';
      if (forcedScope?.scopeType && forcedScope?.scopeId) {
        analysisMode = 'focused';
        scopeType = forcedScope.scopeType;
        scopeId = forcedScope.scopeId;
        entityId = forcedScope.scopeId;
      } else {
        const activeCandidate = faultCandidates.find((x) => x.key === activeCandidateKey) || null;
        const selectedCandidate = candidateFromSelected(faultCandidates);
        const fallbackCandidate = faultCandidates[0] || null;
        const preferred = activeCandidate || selectedCandidate || fallbackCandidate;
        if (preferred) {
          analysisMode = 'focused';
          scopeType = preferred.scopeType;
          scopeId = preferred.scopeId;
          entityId = preferred.scopeId;
          setActiveCandidateKey(preferred.key);
          focusFaultCandidate(preferred);
        } else if (selectedFlow?.path?.length > 1) {
          entityId = `${selectedFlow.path[0]}->${selectedFlow.path[selectedFlow.path.length - 1]}`;
        }
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
      setReportViewMode('wide');
      setShowReportDrawer(true);

      const directReasonFromMetrics = (() => {
        if (scopeType === 'link' && scopeId) {
          const parsed = parseScopedLink(scopeId);
          let target = null;
          for (const item of Object.values(monitorSnapshot.byLink || {})) {
            if (!item || typeof item !== 'object') {
              continue;
            }
            if (item.linkUid === scopeId || item.linkId === scopeId) {
              target = item;
              break;
            }
            if (parsed) {
              const p1 = normalizeLinkPair(item.srcNodeId || item.srcNodeUid, item.dstNodeId || item.dstNodeUid);
              if (p1 && p1 === normalizeLinkPair(parsed.a, parsed.b)) {
                target = item;
                break;
              }
            }
          }
          if (target) {
            const loss = Number(target.lossRate);
            const rtt = Number(target.rttMs);
            const jitter = Number(target.jitterMs);
            const state = String(target.state || '').toUpperCase();
            const causes = [];
            if (Number.isFinite(loss) && loss >= 0.06) causes.push(`loss=${loss.toFixed(3)}>=0.06`);
            else if (Number.isFinite(loss) && loss >= 0.03) causes.push(`loss=${loss.toFixed(3)}>=0.03`);
            if (Number.isFinite(rtt) && rtt >= 280) causes.push(`rtt=${rtt.toFixed(1)}>=280ms`);
            else if (Number.isFinite(rtt) && rtt >= 180) causes.push(`rtt=${rtt.toFixed(1)}>=180ms`);
            if (Number.isFinite(jitter) && jitter >= 35) causes.push(`jitter=${jitter.toFixed(1)}>=35ms`);
            if (state === 'DOWN' || state === 'DEGRADED') causes.push(`state=${state}`);
            if (causes.length > 0) {
              return `链路 ${scopeId} 触发异常：${causes.join('，')}。`;
            }
          }
        }
        if (scopeType === 'node' && scopeId) {
          let target = null;
          for (const item of Object.values(monitorSnapshot.byNode || {})) {
            if (!item || typeof item !== 'object') {
              continue;
            }
            const aliases = [item.nodeId, item.nodeUid, item.topoNodeId, item.dockerName].map((x) => String(x || '').trim());
            if (aliases.includes(scopeId)) {
              target = item;
              break;
            }
          }
          if (target) {
            const cpu = Number(target.cpuRatio);
            const mem = Number(target.memRatio);
            const status = String(target.status || '').toUpperCase();
            const causes = [];
            if (Number.isFinite(cpu) && cpu >= 0.92) causes.push(`cpu=${(cpu * 100).toFixed(1)}%>=92%`);
            else if (Number.isFinite(cpu) && cpu >= 0.82) causes.push(`cpu=${(cpu * 100).toFixed(1)}%>=82%`);
            if (Number.isFinite(mem) && mem >= 0.92) causes.push(`mem=${(mem * 100).toFixed(1)}%>=92%`);
            else if (Number.isFinite(mem) && mem >= 0.82) causes.push(`mem=${(mem * 100).toFixed(1)}%>=82%`);
            if (status && status !== 'UP') causes.push(`status=${status}`);
            if (causes.length > 0) {
              return `节点 ${scopeId} 触发异常：${causes.join('，')}。`;
            }
          }
        }
        return '';
      })();
      const scopeObservation = (() => {
        if (scopeType === 'node' && scopeId) {
          for (const item of Object.values(monitorSnapshot.byNode || {})) {
            if (!item || typeof item !== 'object') {
              continue;
            }
            const aliases = [item.nodeId, item.nodeUid, item.topoNodeId, item.dockerName].map((x) => String(x || '').trim());
            if (!aliases.includes(scopeId)) {
              continue;
            }
            return {
              cpu_ratio: Number(item.cpuRatio),
              mem_ratio: Number(item.memRatio),
              status: String(item.status || '')
            };
          }
          return null;
        }
        if (scopeType === 'link' && scopeId) {
          const parsed = parseScopedLink(scopeId);
          for (const item of Object.values(monitorSnapshot.byLink || {})) {
            if (!item || typeof item !== 'object') {
              continue;
            }
            if (item.linkUid === scopeId || item.linkId === scopeId) {
              return {
                loss_rate: Number(item.lossRate),
                rtt_ms: Number(item.rttMs),
                jitter_ms: Number(item.jitterMs),
                state: String(item.state || '')
              };
            }
            if (parsed) {
              const p1 = normalizeLinkPair(item.srcNodeId || item.srcNodeUid, item.dstNodeId || item.dstNodeUid);
              if (p1 && p1 === normalizeLinkPair(parsed.a, parsed.b)) {
                return {
                  loss_rate: Number(item.lossRate),
                  rtt_ms: Number(item.rttMs),
                  jitter_ms: Number(item.jitterMs),
                  state: String(item.state || '')
                };
              }
            }
          }
        }
        return null;
      })();
      if (directReasonFromMetrics) {
        setAnalysisDirectReason(directReasonFromMetrics);
      }

      const runResp = await monitorClientRef.current.analyzeRun({
        mode: analysisMode,
        scope_type: scopeType,
        scope_id: scopeId,
        topology_epoch: String(monitorEpoch),
        include_debug: false,
        max_depth: analysisMode === 'focused' ? 2 : 4,
        spread_mode: analysisMode === 'focused' ? 'single_point' : 'cascade',
        cascade_threshold: 0.6,
        scope_observation: scopeObservation || undefined
      });
      const overviewResult = (() => {
        if (runResp && typeof runResp === 'object' && runResp.contract_version) {
          return runResp;
        }
        return getAnalysisResult(runResp);
      })();
      const spreadResult = overviewResult?.topology_impact || null;
      const impactResult = overviewResult || null;
      if (!directReasonFromMetrics) {
        const topFinding = Array.isArray(impactResult?.reasoning?.top_findings)
          ? impactResult.reasoning.top_findings.find((x) => x && typeof x === 'object')
          : null;
        if (topFinding) {
          const st = String(topFinding.scope_type || 'unknown');
          const sid = String(topFinding.scope_id || '-');
          const sev = String(topFinding.severity || 'warning');
          const ev = Array.isArray(topFinding.evidence) ? topFinding.evidence.filter(Boolean) : [];
          const evText = ev.length ? `，触发条件：${ev.join('、')}` : '';
          setAnalysisDirectReason(`${st}/${sid} 出现 ${sev} 级异常${evText}。`);
        } else {
          setAnalysisDirectReason('未命中明确阈值证据，请查看判定依据与实时指标。');
        }
      }
      const impactedCount = Array.isArray(spreadResult?.impacted_nodes) ? spreadResult.impacted_nodes.length : 0;
      const taskCount = Array.isArray(impactResult?.tasks) ? impactResult.tasks.length : 0;
      const riskLevel = String(impactResult?.summary?.risk_level || '').toLowerCase();
      const hasTopFinding = Array.isArray(impactResult?.reasoning?.top_findings) && impactResult.reasoning.top_findings.length > 0;
      if (!spreadResult && !impactResult) {
        const msg = '高级分析返回空结果：后端未返回 result/data';
        setAnalysisError(msg);
        setMonitorActionStatus(msg);
        pushToast(msg, 'warn');
      } else if (analysisMode === 'focused' && riskLevel === 'normal' && !hasTopFinding) {
        const msg = '当前对象无活跃异常（未命中告警证据），建议作为观察对象而非故障对象';
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
      let forecastContext = {};
      if (scopeType === 'node' || scopeType === 'link') {
        try {
          const eventType = scopeType === 'link' ? 'link_metric' : 'node_metric';
          const metric = scopeType === 'link' ? 'rtt_ms' : 'cpu_ratio';
          const forecastResp = await monitorClientRef.current.getForecastLstm({
            eventType,
            metric,
            entityId: scopeId,
            strategy: 'fallback',
            horizon: 12,
            window: 24
          });
          const pointsRaw = (
            forecastResp?.points
            || forecastResp?.forecast
            || forecastResp?.result?.points
            || forecastResp?.result?.forecast
            || forecastResp?.data?.points
            || forecastResp?.data?.forecast
            || []
          );
          const points = Array.isArray(pointsRaw)
            ? pointsRaw
              .map((p, idx) => ({
                t: p?.t || p?.ts || p?.timestamp || String(idx),
                v: Number(p?.v ?? p?.value ?? p?.y ?? p?.yhat),
                lower: Number(p?.lower),
                upper: Number(p?.upper)
              }))
              .filter((p) => Number.isFinite(p.v))
            : [];
          forecastContext = {
            model_type: forecastResp?.model_type || '',
            model_version: forecastResp?.model_version || '',
            metrics: forecastResp?.metrics || {},
            confidence: forecastResp?.confidence || {},
            horizon: forecastResp?.horizon,
            window: forecastResp?.window,
            points: points.slice(0, 24)
          };
          setSeriesSnapshot({
            eventType,
            metric,
            entityId: scopeId,
            points,
            modelType: forecastResp?.model_type || '',
            modelVersion: forecastResp?.model_version || '',
            metrics: forecastResp?.metrics || {},
            confidence: forecastResp?.confidence || {}
          });
        } catch {
          setSeriesSnapshot(null);
          forecastContext = {};
        }
      }
      const aiSeq = aiExplainSeqRef.current + 1;
      aiExplainSeqRef.current = aiSeq;
      setAnalysisAiLoading(true);
      monitorClientRef.current.analyzeExplain({
        analysis: overviewResult,
        scope_type: scopeType,
        scope_id: scopeId,
        extra_context: {
          direct_reason: directReasonFromMetrics || '',
          scope_observation: scopeObservation || {},
          forecast_context: forecastContext,
          security_correlation: overviewResult?.security_correlation || {},
          impacted_nodes: spreadResult?.impacted_nodes || [],
          impacted_links: spreadResult?.impacted_links || [],
          tasks_top: Array.isArray(impactResult?.tasks) ? impactResult.tasks.slice(0, 8) : []
        }
      }).then((explainResp) => {
        if (aiExplainSeqRef.current !== aiSeq) {
          return;
        }
        setAnalysisAiReport(String(explainResp?.report || '').trim());
        setAnalysisAiMeta({
          source: explainResp?.source || 'unknown',
          model: explainResp?.model || '',
          fallbackReason: explainResp?.fallback_reason || ''
        });
      }).catch((explainErr) => {
        if (aiExplainSeqRef.current !== aiSeq) {
          return;
        }
        setAnalysisAiError(explainErr?.message || 'AI报告生成失败');
        setAnalysisAiMeta(null);
        setAnalysisAiReport('');
      }).finally(() => {
        if (aiExplainSeqRef.current !== aiSeq) {
          return;
        }
        setAnalysisAiLoading(false);
      });
      const impactedNodeCount = Array.isArray(spreadResult?.impacted_nodes) ? spreadResult.impacted_nodes.length : 0;
      const impactedLinkCount = Array.isArray(spreadResult?.impacted_links) ? spreadResult.impacted_links.length : 0;
      setMonitorActionStatus(`已刷新高级分析结果（高亮节点 ${impactedNodeCount}，链路 ${impactedLinkCount}）`);
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
    try {
      setSimulationLoading(true);
      const activeCandidate = faultCandidates.find((x) => x.key === activeCandidateKey) || null;
      let scenarioType = 'link_down';
      let scopeId = '';
      let focusScopeType = 'link';
      if (activeCandidate?.scopeType === 'node' && activeCandidate?.scopeId) {
        scenarioType = 'node_hotspot';
        scopeId = String(activeCandidate.scopeId);
        focusScopeType = 'node';
      } else if (activeCandidate?.scopeType === 'link' && activeCandidate?.scopeId) {
        scenarioType = 'link_down';
        scopeId = String(activeCandidate.scopeId);
        focusScopeType = 'link';
      } else if (selected?.kind === 'node' && selected?.id) {
        scenarioType = 'node_hotspot';
        scopeId = String(selected.id);
        focusScopeType = 'node';
      } else {
        scopeId = selectedLinkMetric?.linkUid || '';
        if (!scopeId && selectedLink) {
          scopeId = [selectedLink.a.id, selectedLink.b.id].sort().join('<->');
        }
        if (!scopeId) {
          const first = Object.values(monitorSnapshot.byLink || {})[0];
          scopeId = first?.linkUid || '';
        }
        scenarioType = 'link_down';
        focusScopeType = 'link';
      }
      if (!scopeId) {
        const msg = '推演失败：当前无可用故障对象';
        setMonitorActionStatus(msg);
        pushToast(msg, 'warn');
        return;
      }
      const scoped = parseScopedLink(scopeId);
      const createResp = await monitorClientRef.current.createSimulation({
        scenario_type: scenarioType,
        topology_epoch: String(monitorEpoch || 1708848000),
        steps_total: 5,
        params: scenarioType === 'node_hotspot'
          ? {
              node_id: scopeId,
              scope_id: scopeId
            }
          : {
              link_id: scopeId,
              scope_id: scopeId,
              src_node_id: selectedLink?.a?.id || scoped?.a || '',
              dst_node_id: selectedLink?.b?.id || scoped?.b || ''
            }
      });
      const simulationId = createResp?.simulation_id || createResp?.result?.simulation_id;
      if (!simulationId) {
        throw new Error('推演创建成功但未返回 simulation_id');
      }
      let status = 'created';
      let lastSimulation = null;
      for (let i = 0; i < 8 && status !== 'completed'; i += 1) {
        const stepResp = await monitorClientRef.current.stepSimulation(simulationId, {});
        const simObj = stepResp?.simulation || stepResp?.result?.simulation || null;
        lastSimulation = simObj || lastSimulation;
        status = simObj?.status || status;
        if (status === 'completed') {
          break;
        }
      }
      const timelineResp = await monitorClientRef.current.getSimulationTimeline(simulationId);
      const timeline = timelineResp?.timeline || timelineResp?.result?.timeline || [];
      const latest = Array.isArray(timeline) && timeline.length > 0
        ? timeline[timeline.length - 1]
        : (lastSimulation?.latest || null);
      const first = Array.isArray(timeline) && timeline.length > 0 ? timeline[0] : null;
      setSimulationResult({
        simulationId,
        scenarioType,
        focusScopeType,
        focusScopeId: scopeId,
        status,
        timelineCount: Array.isArray(timeline) ? timeline.length : 0,
        latest,
        first,
        timeline: Array.isArray(timeline) ? timeline.slice(-5) : []
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

    const impactedNodeSet = new Set(
      Array.isArray(faultSpread?.impacted_nodes) ? faultSpread.impacted_nodes.map((x) => String(x || '').trim()).filter(Boolean) : []
    );
    const impactedLinkKeySet = new Set();
    if (Array.isArray(faultSpread?.impacted_links)) {
      for (const uid of faultSpread.impacted_links) {
        const p = parseScopedLink(uid);
        if (p?.a && p?.b) {
          impactedLinkKeySet.add(edgeKey(p.a, p.b));
        }
      }
    }
    const simImpactedNodeSet = new Set(
      Array.isArray(simulationResult?.latest?.impacted_nodes)
        ? simulationResult.latest.impacted_nodes.map((x) => String(x || '').trim()).filter(Boolean)
        : []
    );
    const simImpactedLinkKeySet = new Set();
    if (Array.isArray(simulationResult?.latest?.impacted_links)) {
      for (const uid of simulationResult.latest.impacted_links) {
        const p = parseScopedLink(uid);
        if (p?.a && p?.b) {
          simImpactedLinkKeySet.add(edgeKey(p.a, p.b));
        }
      }
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
      const isImpactedNode = impactedNodeSet.has(node.id);
      const isSimImpactedNode = simImpactedNodeSet.has(node.id);
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
          : (
              selectedNode
                ? SELECTED_NODE_COLOR
                : (isSimImpactedNode ? SIM_IMPACTED_NODE_COLOR : (isImpactedNode ? IMPACTED_NODE_COLOR : color))
            );
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
      const inAnalysisImpact = impactedLinkKeySet.has(edgeKey(edge.a, edge.b));
      const inSimulationImpact = simImpactedLinkKeySet.has(edgeKey(edge.a, edge.b));

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
              : inSimulationImpact
                ? SIM_IMPACTED_LINK_COLOR
              : inAnalysisImpact
                ? IMPACTED_LINK_COLOR
              : style.color
          }
        });
        linkEntitiesRef.current.set(linkId, lineEntity);
        linkVisualStateRef.current.set(linkId, {
          width: style.width,
          selected: selectedLink,
          kind: linkKind,
          fault: linkFaulted,
          flow: inSelectedFlow,
          impacted: inAnalysisImpact,
          simImpact: inSimulationImpact
        });
      } else {
        lineEntity.polyline.positions = positions;
      }
      const visual = linkVisualStateRef.current.get(linkId);
      const baseWidth = linkFaulted ? Math.max(style.width, 2.8) : style.width;
      const expectedWidth = selectedLink
        ? baseWidth + 1.6
        : (inSelectedFlow ? baseWidth + 1.0 : (inSimulationImpact ? baseWidth + 0.9 : (inAnalysisImpact ? baseWidth + 0.7 : baseWidth)));
      const widthChanged = !visual || visual.width !== expectedWidth || visual.selected !== selectedLink;
      if (widthChanged) {
        lineEntity.polyline.width = expectedWidth;
      }
      const materialChanged = !visual || visual.selected !== selectedLink || visual.kind !== linkKind || visual.fault !== linkFaulted || visual.flow !== inSelectedFlow || visual.impacted !== inAnalysisImpact || visual.simImpact !== inSimulationImpact;
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
        } else if (inSimulationImpact) {
          lineEntity.polyline.material = new PolylineDashMaterialProperty({
            color: SIM_IMPACTED_LINK_COLOR,
            dashLength: 10
          });
        } else if (inAnalysisImpact) {
          lineEntity.polyline.material = IMPACTED_LINK_COLOR;
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
        flow: inSelectedFlow,
        impacted: inAnalysisImpact,
        simImpact: inSimulationImpact
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
  }, [frame, layerPrefs, selected, faults, selectedFlowId, monitorSnapshot.byFlow, monitorSnapshot.byNode, faultSpread, simulationResult]);

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
  const buildCanonicalLinkKey = (a, b) => {
    const x = resolveTopoNodeId(a) || String(a || '').trim();
    const y = resolveTopoNodeId(b) || String(b || '').trim();
    if (!x || !y) {
      return '';
    }
    return x < y ? `${x}<->${y}` : `${y}<->${x}`;
  };
  const monitorLinkMetricIndex = new Map();
  const addMetricKey = (item, a, b) => {
    const key = buildCanonicalLinkKey(a, b);
    if (key && !monitorLinkMetricIndex.has(key)) {
      monitorLinkMetricIndex.set(key, item);
    }
  };
  for (const item of Object.values(monitorSnapshot.byLink)) {
    if (!item) {
      continue;
    }
    addMetricKey(item, item.srcNodeId, item.dstNodeId);
    addMetricKey(item, item.srcNodeUid, item.dstNodeUid);
    const pairFromUid = parseScopedLink(item.linkUid || '');
    if (pairFromUid) {
      addMetricKey(item, pairFromUid.a, pairFromUid.b);
    }
    const pairFromId = parseScopedLink(item.linkId || '');
    if (pairFromId) {
      addMetricKey(item, pairFromId.a, pairFromId.b);
    }
  }
  const selectedLinkMetricKey = selectedLink
    ? (
      buildCanonicalLinkKey(selectedLink.a?.id, selectedLink.b?.id) ||
      buildCanonicalLinkKey(selectedLink.a?.name, selectedLink.b?.name)
    )
    : '';
  const selectedLinkMetric = selectedLinkMetricKey
    ? (monitorLinkMetricIndex.get(selectedLinkMetricKey) || null)
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
  const focusedScopeIds = (() => {
    const ids = new Set();
    if (selected?.kind === 'node' && selected?.id) {
      ids.add(String(selected.id));
    }
    if (selected?.kind === 'link' && selectedLink) {
      ids.add(String(selectedLink.a?.id || ''));
      ids.add(String(selectedLink.b?.id || ''));
      ids.add(normalizeLinkPair(selectedLink.a?.id, selectedLink.b?.id));
    }
    return ids;
  })();
  const warningBroadcastItems = timelineAlarms
    .filter((alarm) => alarm && ['critical', 'warning'].includes(String(alarm.severity || '').toLowerCase()))
    .sort((a, b) => {
      const aHit = focusedScopeIds.has(String(a.scopeId || '')) || focusedScopeIds.has(String(a.scopeUid || ''));
      const bHit = focusedScopeIds.has(String(b.scopeId || '')) || focusedScopeIds.has(String(b.scopeUid || ''));
      if (aHit !== bHit) {
        return aHit ? -1 : 1;
      }
      return 0;
    })
    .slice(0, 8)
    .map((alarm) => {
      const ts = Date.parse(alarm.timestamp || '');
      const hhmmss = Number.isFinite(ts) ? new Date(ts).toLocaleTimeString() : '--:--:--';
      const severity = String(alarm.severity || 'info').toUpperCase();
      const focusMark = focusedScopeIds.has(String(alarm.scopeId || '')) || focusedScopeIds.has(String(alarm.scopeUid || '')) ? ' [关注]' : '';
      return `[${hhmmss}] ${severity}${focusMark} ${alarm.scopeType}/${alarm.scopeId} ${alarm.title || '未命名告警'}`;
    });
  const warningBroadcastText = warningBroadcastItems.length > 0
    ? warningBroadcastItems.join(' ｜ ')
    : '当前未发现新的疑似告警，系统处于持续监视状态';
  const warningBannerLevel = warningBroadcastItems.some((item) => item.includes('CRITICAL'))
    ? 'critical'
    : warningBroadcastItems.length > 0
      ? 'warning'
      : 'normal';
  const filteredTimelineAlarms = timelineAlarms.filter((alarm) => {
    const bySeverity = alarmSeverityFilter === 'all' || alarm.severity === alarmSeverityFilter;
    const byScope = alarmScopeFilter === 'all' || alarm.scopeType === alarmScopeFilter;
    return bySeverity && byScope;
  });
  const autoAlarmLooksActive = (alarm) => {
    if (!alarm || typeof alarm !== 'object') {
      return false;
    }
    const scopeType = String(alarm.scopeType || '').trim();
    const scopeId = String(alarm.scopeId || '').trim();
    const severity = String(alarm.severity || '').toLowerCase();
    if (!scopeType || !scopeId || !['warning', 'critical'].includes(severity)) {
      return false;
    }
    if (scopeType === 'node') {
      let metric = null;
      const keys = [normalizeIdentity(scopeId), normalizeIdentityLoose(scopeId)].filter(Boolean);
      for (const k of keys) {
        const hit = nodeMetricAliasMap.get(k);
        if (hit?.metric) {
          metric = hit.metric;
          break;
        }
      }
      if (!metric) {
        return false;
      }
      const cpu = Number(metric.cpuRatio);
      const mem = Number(metric.memRatio);
      const st = String(metric.status || '').toUpperCase();
      if (severity === 'critical') {
        return st === 'DOWN' || (Number.isFinite(cpu) && cpu >= 0.92) || (Number.isFinite(mem) && mem >= 0.92);
      }
      return (st && st !== 'UP') || (Number.isFinite(cpu) && cpu >= 0.82) || (Number.isFinite(mem) && mem >= 0.82);
    }
    if (scopeType === 'link') {
      const p = parseScopedLink(scopeId);
      const key = p ? buildCanonicalLinkKey(p.a, p.b) : '';
      const metric = (key && monitorLinkMetricIndex.get(key)) || null;
      if (!metric) {
        return false;
      }
      const loss = Number(metric.lossRate);
      const rtt = Number(metric.rttMs);
      const jitter = Number(metric.jitterMs);
      const st = String(metric.state || '').toUpperCase();
      if (severity === 'critical') {
        return st === 'DOWN' || st === 'DISCONNECTED' || (Number.isFinite(loss) && loss >= 0.06) || (Number.isFinite(rtt) && rtt >= 280);
      }
      return st === 'DEGRADED' || (Number.isFinite(loss) && loss >= 0.03) || (Number.isFinite(rtt) && rtt >= 180) || (Number.isFinite(jitter) && jitter >= 35);
    }
    return false;
  };
  const faultCandidates = (() => {
    const out = [];
    const seen = new Set();
    for (const fault of faults) {
      if (!fault || !fault.fault_type) {
        continue;
      }
      if (fault.fault_type === 'DAMAGED') {
        const nodeId = String(fault.target?.node_id || '').trim();
        if (!nodeId) {
          continue;
        }
        const key = `manual:node:${nodeId}:${fault.fault_id || 'na'}`;
        if (seen.has(key)) {
          continue;
        }
        seen.add(key);
        out.push({
          key,
          scopeType: 'node',
          scopeId: nodeId,
          severity: 'critical',
          source: 'manual',
          faultId: fault.fault_id || '',
          title: `手动注入节点故障 ${nodeId}`
        });
      } else if (fault.fault_type === 'INTERRUPTED') {
        const a = String(fault.target?.a || '').trim();
        const b = String(fault.target?.b || '').trim();
        const uid = normalizeLinkPair(a, b);
        if (!uid) {
          continue;
        }
        const key = `manual:link:${uid}:${fault.fault_id || 'na'}`;
        if (seen.has(key)) {
          continue;
        }
        seen.add(key);
        out.push({
          key,
          scopeType: 'link',
          scopeId: uid,
          severity: 'critical',
          source: 'manual',
          faultId: fault.fault_id || '',
          title: `手动注入链路故障 ${uid}`
        });
      }
    }
    for (const alarm of filteredTimelineAlarms) {
      if (!alarm || !['critical', 'warning'].includes(String(alarm.severity || '').toLowerCase())) {
        continue;
      }
      if (!autoAlarmLooksActive(alarm)) {
        continue;
      }
      const scopeType = String(alarm.scopeType || '').trim();
      const scopeId = String(alarm.scopeId || '').trim();
      if (!scopeType || !scopeId) {
        continue;
      }
      const key = `auto:${scopeType}:${scopeId}`;
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      out.push({ key, scopeType, scopeId, severity: String(alarm.severity || 'warning'), source: 'auto', title: alarm.title || `${scopeType} ${scopeId}` });
    }
    return out.slice(0, 12);
  })();
  const manualNodeCandidates = faultCandidates.filter((x) => x.source === 'manual' && x.scopeType === 'node');
  const manualLinkCandidates = faultCandidates.filter((x) => x.source === 'manual' && x.scopeType === 'link');
  const autoNodeCandidates = faultCandidates.filter((x) => x.source === 'auto' && x.scopeType === 'node');
  const autoLinkCandidates = faultCandidates.filter((x) => x.source === 'auto' && x.scopeType === 'link');
  const directReasonText = analysisDirectReason;
  const reportHistoryPoints = (() => {
    if (!analysisSummary?.scopeType || !analysisSummary?.scopeId) {
      return [];
    }
    if (analysisSummary.scopeType === 'node') {
      const scopeId = String(analysisSummary.scopeId || '').trim();
      const keys = [normalizeIdentity(scopeId), normalizeIdentityLoose(scopeId)].filter(Boolean);
      let nodeId = scopeId;
      for (const k of keys) {
        const hit = nodeMetricAliasMap.get(k);
        if (hit?.metric?.topoNodeId || hit?.metric?.nodeId) {
          nodeId = String(hit.metric.topoNodeId || hit.metric.nodeId);
          break;
        }
      }
      const raw = metricHistoryRef.current.nodes.get(nodeId)?.cpu || [];
      return raw.map((p) => ({ t: p.t, v: p.v })).filter((p) => Number.isFinite(p.v));
    }
    if (analysisSummary.scopeType === 'link') {
      const scope = parseScopedLink(String(analysisSummary.scopeId || ''));
      const key = scope ? buildCanonicalLinkKey(scope.a, scope.b) : '';
      const metric = (key && monitorLinkMetricIndex.get(key)) || null;
      if (!metric?.linkId) {
        return [];
      }
      const raw = metricHistoryRef.current.links.get(metric.linkId)?.rtt || [];
      return raw.map((p) => ({ t: p.t, v: p.v })).filter((p) => Number.isFinite(p.v));
    }
    return [];
  })();
  const reportForecastPointsRaw = (seriesSnapshot?.points || []).map((p) => ({ t: p.t, v: p.v })).filter((p) => Number.isFinite(p.v));
  const reportForecastPoints = (() => {
    if (reportForecastPointsRaw.length > 0) {
      return reportForecastPointsRaw;
    }
    if (reportHistoryPoints.length < 2) {
      return [];
    }
    const last = reportHistoryPoints[reportHistoryPoints.length - 1];
    const prev = reportHistoryPoints[reportHistoryPoints.length - 2];
    const delta = Number(last.v) - Number(prev.v);
    const out = [];
    for (let i = 1; i <= 12; i += 1) {
      out.push({ t: `f-${i}`, v: Number(last.v) + delta * i });
    }
    return out;
  })();
  const reportHistoryView = sampleByGranularity(reportHistoryPoints, reportGranularity);
  const reportForecastView = sampleByGranularity(reportForecastPoints, reportGranularity);
  const reportTrendPaths = buildTrendPaths(reportHistoryView, reportForecastView, 620, 220);
  const reportDividerX = (() => {
    const histCount = reportHistoryView.length;
    const total = histCount + reportForecastView.length;
    if (histCount <= 0 || total <= 1) {
      return null;
    }
    const x = reportTrendPaths.toX(histCount - 1);
    return Number.isFinite(x) ? x : null;
  })();
  const yTicks = [
    reportTrendPaths.max,
    (reportTrendPaths.max + reportTrendPaths.min) / 2,
    reportTrendPaths.min
  ];
  const xTicks = (() => {
    const total = reportHistoryView.length + reportForecastView.length;
    if (total <= 1) {
      return [];
    }
    const idxList = [0, Math.floor((total - 1) * 0.25), Math.floor((total - 1) * 0.5), Math.floor((total - 1) * 0.75), total - 1];
    const uniqueIdx = [...new Set(idxList)];
    return uniqueIdx.map((idx) => {
      let label = '-';
      if (idx < reportHistoryView.length) {
        label = formatTrendTimeLabel(reportHistoryView[idx]?.t, reportGranularity, `H${idx + 1}`);
      } else {
        label = `+${idx - reportHistoryView.length + 1}`;
      }
      return { idx, x: reportTrendPaths.toX(idx), label };
    });
  })();

  useEffect(() => {
    if (!activeCandidateKey) {
      return;
    }
    const stillExists = faultCandidates.some((x) => x.key === activeCandidateKey);
    if (!stillExists) {
      setActiveCandidateKey(faultCandidates[0]?.key || '');
    }
  }, [faultCandidates, activeCandidateKey]);
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
  const stalenessMsText = `${Math.max(0, runtimeHealth.stalenessMs).toFixed(0)}ms`;
  const ingestFpsText = runtimeHealth.ingestFps.toFixed(2);
  const simTimeText = frame ? `${frame.sim_time_s.toFixed(1)}s` : '-';
  const tickText = frame ? `${frame.elapsed_ms.toFixed(2)}ms` : '-';
  const avgDegreeText = frame ? frame.metrics.avg_degree.toFixed(2) : '-';
  const qoeImbalanceText = frame ? (frame.metrics.qoe_imbalance ?? 0).toFixed(4) : '-';
  const mobileConnectedText = frame
    ? `${frame.metrics.mobile_connected_count ?? 0}/${frame.nodes.filter((n) => n.type !== 'leo').length || 0}`
    : '-';
  const playbackText = playback.paused ? 'paused' : 'running';
  const speedText = `${playback.speed}x`;
  const manualFaultNodeCount = faults.filter((f) => f.fault_type === 'DAMAGED').length;
  const manualFaultLinkCount = faults.filter((f) => f.fault_type === 'INTERRUPTED').length;
  const monitorUpdatedText = monitorSnapshot.updatedAt
    ? new Date(monitorSnapshot.updatedAt).toLocaleTimeString()
    : '-';
  const monitorLastSuccessText = monitorLastSuccessAt
    ? new Date(monitorLastSuccessAt).toLocaleTimeString()
    : '-';

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

  function renderFaultRows(candidates, allowClear = false) {
    if (!Array.isArray(candidates) || candidates.length === 0) {
      return <div className="fault-empty">暂无</div>;
    }
    return candidates.map((x) => (
      <div
        key={x.key}
        className={`candidate-row ${activeCandidateKey === x.key ? 'active' : ''}`}
        onClick={() => focusFaultCandidate(x)}
      >
        <div className="candidate-main">{x.scopeType}/{x.scopeId}</div>
        <div className="fault-row-actions">
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              focusFaultCandidate(x);
            }}
          >
            定位
          </button>
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              focusFaultCandidate(x);
              runAdvancedAnalysis({ scopeType: x.scopeType, scopeId: x.scopeId });
            }}
          >
            分析
          </button>
          {allowClear && x.faultId ? (
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                sendControl('clear_fault', { fault_id: x.faultId });
              }}
            >
              清零
            </button>
          ) : null}
        </div>
      </div>
    ));
  }

  return (
    <div className="app-shell">
      <div className={`warning-banner ${warningBannerLevel}`}>
        <div className="warning-banner-track">
          <span>{warningBroadcastText}</span>
          <span>{warningBroadcastText}</span>
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
            <div>match key: {selectedLinkMetricKey || '-'}</div>
            <div>monitor health: {selectedLinkMetric?.health || '-'}</div>
            <div>loss: {selectedLinkMetric?.lossRate != null ? `${(selectedLinkMetric.lossRate * 100).toFixed(2)}%` : '--'}</div>
            <div>rtt/jitter: {selectedLinkMetric?.rttMs != null ? `${selectedLinkMetric.rttMs.toFixed(1)}ms` : '--'} / {selectedLinkMetric?.jitterMs != null ? `${selectedLinkMetric.jitterMs.toFixed(1)}ms` : '--'}</div>
            <div>tx/rx: {selectedLinkMetric?.txBps != null ? selectedLinkMetric.txBps.toFixed(1) : '--'} / {selectedLinkMetric?.rxBps != null ? selectedLinkMetric.rxBps.toFixed(1) : '--'} bps</div>
            {!selectedLinkMetric ? <div>monitor 匹配: 未命中链路指标（请检查链路命名映射）</div> : null}
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
      <div className={`fault-dock ${showReportDrawer && reportViewMode === 'wide' ? 'wide-mode' : ''} ${showReportDrawer && reportViewMode === 'fullscreen' ? 'fullscreen-mode' : ''}`}>
        <div className="fault-panel">
          <div className="bottom-tab-row">
            <button type="button" className={bottomTab === 'fault_analysis' ? 'active' : ''} onClick={() => setBottomTab('fault_analysis')}>故障与分析</button>
            <button type="button" className={bottomTab === 'simulation' ? 'active' : ''} onClick={() => setBottomTab('simulation')}>推演</button>
          </div>
          {bottomTab === 'fault_analysis' ? (
            <div className="bottom-tab-content">
              <div className="layer-header">
                <span>故障与分析</span>
                <div className="monitor-header-actions">
                  <button type="button" onClick={runAdvancedAnalysis} disabled={analysisLoading || !analysisSupported}>{analysisLoading ? '分析中...' : (analysisSupported ? '运行分析' : '接口不可用')}</button>
                  <button type="button" onClick={() => sendControl('list_faults')}>刷新故障</button>
                </div>
              </div>
              <div className={`fault-center-layout ${showReportDrawer ? '' : 'no-report'} ${showReportDrawer && reportViewMode === 'wide' ? 'wide' : ''} ${showReportDrawer && reportViewMode === 'fullscreen' ? 'fullscreen' : ''}`}>
                <div className="fault-center-list">
                  <div className="layer-header">
                    <span>自动发现</span>
                  </div>
                  <div className="analysis-block">
                    <div><strong>节点故障</strong> ({autoNodeCandidates.length})</div>
                    {renderFaultRows(autoNodeCandidates, false)}
                  </div>
                  <div className="analysis-block">
                    <div><strong>链路故障</strong> ({autoLinkCandidates.length})</div>
                    {renderFaultRows(autoLinkCandidates, false)}
                  </div>
                  <div className="layer-header">
                    <span>手动注入</span>
                    <button type="button" onClick={() => sendControl('clear_all_faults')} disabled={faults.length === 0}>全部清零</button>
                  </div>
                  <div className="analysis-block">
                    <div><strong>节点故障</strong> ({manualNodeCandidates.length})</div>
                    {renderFaultRows(manualNodeCandidates, true)}
                  </div>
                  <div className="analysis-block">
                    <div><strong>链路故障</strong> ({manualLinkCandidates.length})</div>
                    {renderFaultRows(manualLinkCandidates, true)}
                  </div>
                </div>
                {showReportDrawer ? (
                  <aside className={`analysis-report-drawer ${reportViewMode}`}>
                    <div className="layer-header">
                      <span>分析报告</span>
                      <div className="monitor-header-actions">
                        <button type="button" onClick={() => setReportViewMode((v) => (v === 'fullscreen' ? 'wide' : 'fullscreen'))}>{reportViewMode === 'fullscreen' ? '退出全屏' : '全屏'}</button>
                        <button type="button" onClick={() => setShowReportDrawer(false)}>关闭</button>
                      </div>
                    </div>
                    {analysisError ? <div className="analysis-error">{analysisError}</div> : null}
                    {!analysisSummary && !faultSpread && !taskImpact ? <div className="fault-empty">请选择故障对象并点击分析</div> : null}
                    {analysisSummary ? (
                      <div className="analysis-block">
                        <div><strong>Request</strong>: mode={analysisSummary.mode || '-'}</div>
                        <div>scope: {analysisSummary.scopeType || '-'} / {analysisSummary.scopeId || '-'}</div>
                        <div>entity: {analysisSummary.entityId || '-'}</div>
                      </div>
                    ) : null}
                    {seriesSnapshot ? (
                      <div className="analysis-block">
                        <div><strong>LSTM预测</strong>: {seriesSnapshot.eventType}/{seriesSnapshot.metric}</div>
                        <div>entity: {seriesSnapshot.entityId || '-'}</div>
                        <div>points: {seriesSnapshot.points?.length || 0}</div>
                        <div>model: {seriesSnapshot.modelType || '-'}@{seriesSnapshot.modelVersion || '-'}</div>
                        <div>mape/rmse: {Number.isFinite(Number(seriesSnapshot?.metrics?.mape)) ? Number(seriesSnapshot.metrics.mape).toFixed(4) : '-'} / {Number.isFinite(Number(seriesSnapshot?.metrics?.rmse)) ? Number(seriesSnapshot.metrics.rmse).toFixed(4) : '-'}</div>
                        <div>confidence: {seriesSnapshot?.confidence?.level || '-'}</div>
                        <div>
                          trend: {(() => {
                            const pts = seriesSnapshot.points || [];
                            if (pts.length < 2) return '-';
                            const d = pts[pts.length - 1].v - pts[0].v;
                            if (d > 0.0001) return '上升';
                            if (d < -0.0001) return '下降';
                            return '平稳';
                          })()}
                        </div>
                      </div>
                    ) : null}
                    <div className="analysis-block">
                      <div className="layer-header">
                        <span>趋势图（历史 + 预测）</span>
                        <div className="monitor-header-actions">
                          {['min', 'hour', 'day', 'week'].map((g) => (
                            <button key={`g-${g}`} type="button" className={reportGranularity === g ? 'active' : ''} onClick={() => setReportGranularity(g)}>{g === 'min' ? '分' : g === 'hour' ? '时' : g === 'day' ? '日' : '周'}</button>
                          ))}
                        </div>
                      </div>
                      <svg viewBox="0 0 620 220" className="report-trend-chart">
                        <line x1={reportTrendPaths.plot.x0} y1={reportTrendPaths.plot.y0} x2={reportTrendPaths.plot.x0} y2={reportTrendPaths.plot.y0 + reportTrendPaths.plot.h} className="report-axis" />
                        <line x1={reportTrendPaths.plot.x0} y1={reportTrendPaths.plot.y0 + reportTrendPaths.plot.h} x2={reportTrendPaths.plot.x0 + reportTrendPaths.plot.w} y2={reportTrendPaths.plot.y0 + reportTrendPaths.plot.h} className="report-axis" />
                        {yTicks.map((v, idx) => {
                          const y = reportTrendPaths.toY(v);
                          return (
                            <g key={`yt-${idx}`}>
                              <line x1={reportTrendPaths.plot.x0} y1={y} x2={reportTrendPaths.plot.x0 + reportTrendPaths.plot.w} y2={y} className="report-grid" />
                              <text x={reportTrendPaths.plot.x0 - 8} y={y + 4} className="report-axis-text">{Number(v).toFixed(2)}</text>
                            </g>
                          );
                        })}
                        {xTicks.map((tick) => (
                          <g key={`xt-${tick.idx}`}>
                            <line x1={tick.x} y1={reportTrendPaths.plot.y0 + reportTrendPaths.plot.h} x2={tick.x} y2={reportTrendPaths.plot.y0 + reportTrendPaths.plot.h + 5} className="report-axis" />
                            <text x={tick.x} y={reportTrendPaths.plot.y0 + reportTrendPaths.plot.h + 18} textAnchor="middle" className="report-axis-text">{tick.label}</text>
                          </g>
                        ))}
                        {reportDividerX != null ? <line x1={reportDividerX} y1={reportTrendPaths.plot.y0} x2={reportDividerX} y2={reportTrendPaths.plot.y0 + reportTrendPaths.plot.h} className="report-trend-divider" /> : null}
                        <path className="report-trend-history" d={reportTrendPaths.historyPath} />
                        <path className="report-trend-forecast" d={reportTrendPaths.forecastPath} />
                      </svg>
                      <div className="report-trend-legend">
                        <span className="legend-swatch history"></span>历史
                        <span className="legend-swatch forecast"></span>LSTM预测
                      </div>
                    </div>
                    {faultSpread ? (
                      <div className="analysis-block">
                        <div><strong>Topology Impact</strong></div>
                        <div>seeds: {faultSpread.seed_nodes?.length ?? 0}, impacted_nodes: {faultSpread.impacted_nodes?.length ?? 0}</div>
                        <div>impacted_links: {faultSpread.impacted_links?.length ?? 0}, boundary: {faultSpread.boundary_nodes?.length ?? 0}</div>
                      </div>
                    ) : null}
                    {directReasonText ? (
                      <div className="analysis-block">
                        <div><strong>直接原因</strong></div>
                        <div>{directReasonText}</div>
                      </div>
                    ) : null}
                    {taskImpact?.reasoning ? (
                      <div className="analysis-block">
                        <div><strong>判定依据</strong>: {taskImpact.reasoning.fault_domain || '-'}</div>
                        <div>{taskImpact.reasoning.note || '-'}</div>
                      </div>
                    ) : null}
                    {taskImpact?.security_correlation ? (
                      <div className="analysis-block">
                        <div><strong>安全联动</strong>: {taskImpact.security_correlation.level || 'none'} (score={Number(taskImpact.security_correlation.score || 0).toFixed(2)})</div>
                        <div>security_events: {taskImpact.security_correlation.matched_security_events ?? 0}, window: {taskImpact.security_correlation.window_sec ?? '-'}s</div>
                        <div>evidence: {Array.isArray(taskImpact.security_correlation.evidence) ? taskImpact.security_correlation.evidence.map((e) => `${e.type}:${e.detail}`).slice(0, 2).join(' | ') : '-'}</div>
                      </div>
                    ) : null}
                    {taskImpact?.narrative ? (
                      <div className="analysis-block">
                        <div><strong>人类可读结论</strong>: {taskImpact.narrative.verdict || '-'}</div>
                        <div>{taskImpact.narrative.summary_sentence || '-'}</div>
                      </div>
                    ) : null}
                    <div className="analysis-block">
                      <div><strong>AI详细报告</strong>{analysisAiMeta?.model ? ` (${analysisAiMeta.model})` : ''}</div>
                      <div>固定信息: risk={taskImpact?.summary?.risk_level || taskImpact?.risk_level || '-'}, impacted_nodes={faultSpread?.impacted_nodes?.length ?? 0}, impacted_links={faultSpread?.impacted_links?.length ?? 0}</div>
                      {directReasonText ? <div>固定原因: {directReasonText}</div> : null}
                      {analysisAiLoading ? <div>生成中...</div> : null}
                      {analysisAiError ? <div className="analysis-error">{analysisAiError}</div> : null}
                      {analysisAiMeta?.source === 'fallback' ? <div>当前使用规则兜底报告：{analysisAiMeta?.fallbackReason || 'ollama unavailable'}</div> : null}
                      {analysisAiReport ? <div className="analysis-ai-report">{analysisAiReport}</div> : (!analysisAiLoading ? <div className="fault-empty">暂无AI报告</div> : null)}
                    </div>
                  </aside>
                ) : null}
              </div>
            </div>
          ) : null}
          {bottomTab === 'simulation' ? (
            <div className="monitor-analysis bottom-tab-content">
              <div className="layer-header">
                <span>推演</span>
                <div className="monitor-header-actions">
                  <button type="button" onClick={runSimulationFlow} disabled={simulationLoading}>{simulationLoading ? '推演中...' : '运行推演'}</button>
                </div>
              </div>
              {simulationResult ? (
                <div className="analysis-block">
                  <div><strong>Simulation</strong>: {simulationResult.simulationId}</div>
                  <div>scenario: {simulationResult.scenarioType || '-'}</div>
                  <div>focus: {simulationResult.focusScopeType || '-'} / {simulationResult.focusScopeId || '-'}</div>
                  <div>status: {simulationResult.status || '-'}</div>
                  <div>timeline: {simulationResult.timelineCount}</div>
                  <div>latest_risk: {simulationResult.latest?.risk_level || simulationResult.latest?.risk || '-'}</div>
                  <div>impacted_nodes: {simulationResult.latest?.impacted_nodes ?? '-'} ({simulationResult.latest?.delta?.impacted_nodes != null ? `${simulationResult.latest.delta.impacted_nodes >= 0 ? '+' : ''}${simulationResult.latest.delta.impacted_nodes}` : '-'})</div>
                  <div>impacted_links: {simulationResult.latest?.impacted_links ?? '-'} ({simulationResult.latest?.delta?.impacted_links != null ? `${simulationResult.latest.delta.impacted_links >= 0 ? '+' : ''}${simulationResult.latest.delta.impacted_links}` : '-'})</div>
                  <div>direct_reason: {simulationResult.latest?.direct_reason || '-'}</div>
                  <div>next_action: {simulationResult.latest?.next_action || '-'}</div>
                  {Array.isArray(simulationResult.latest?.primary_faults) && simulationResult.latest.primary_faults.length > 0 ? (
                    <div>
                      primary_faults: {simulationResult.latest.primary_faults.map((x) => `${x.scope_type}/${x.scope_id}(${x.severity || '-'})`).join(' | ')}
                    </div>
                  ) : null}
                </div>
              ) : <div className="fault-empty">尚未运行推演</div>}
            </div>
          ) : null}
        </div>
      </div>
      <button
        type="button"
        className="runtime-fab"
        onClick={() => {
          setShowRuntimeDrawer((v) => !v);
          setShowLayerDrawer(false);
        }}
      >
        运行信息
      </button>
      <button
        type="button"
        className="layer-fab"
        onClick={() => {
          setShowLayerDrawer((v) => !v);
          setShowRuntimeDrawer(false);
        }}
      >
        图层开关
      </button>
      {showRuntimeDrawer ? (
        <div className="layer-modal-backdrop" onClick={() => setShowRuntimeDrawer(false)}>
          <div className="layer-modal" onClick={(e) => e.stopPropagation()}>
            <div className="layer-header">
              <span>运行信息</span>
              <button type="button" onClick={() => setShowRuntimeDrawer(false)}>关闭</button>
            </div>
            <div className="analysis-block">
              <div>WS: {WS_URL} / t: {frame ? frame.sim_time_s.toFixed(1) : '-'} s / tick: {frame ? frame.elapsed_ms.toFixed(2) : '-'} ms</div>
              <div>nodes: {frame ? frame.nodes.length : 0}, links: {frame ? frame.metrics.edge_count : 0}, avg degree: {frame ? frame.metrics.avg_degree.toFixed(2) : '-'}</div>
              <div>mobile connected: {frame ? `${frame.metrics.mobile_connected_count ?? 0}/${(frame.nodes.filter((n) => n.type !== 'leo').length || 1)}` : '-'} / ratio: {frame ? `${((frame.metrics.mobile_connected_ratio ?? 0) * 100).toFixed(1)}%` : '-'}</div>
              <div>I(QoE-Imbalance): {frame ? (frame.metrics.qoe_imbalance ?? 0).toFixed(4) : '-'} / queue: {queueDepth}</div>
              <div className="time-controls">
                <button type="button" onClick={() => setPlayback((p) => ({ ...p, paused: !p.paused }))}>{playback.paused ? '继续' : '暂停'}</button>
                <button type="button" onClick={stepOnce}>单步</button>
                {SPEED_OPTIONS.map((sp) => (
                  <button type="button" key={`speed-runtime-${sp}`} className={playback.speed === sp ? 'active' : ''} onClick={() => setPlayback((p) => ({ ...p, speed: sp }))}>{sp}x</button>
                ))}
              </div>
            </div>
            <div className="layer-header">
              <span>监控诊断</span>
              <button type="button" onClick={() => setShowMonitorDiag((v) => !v)}>{showMonitorDiag ? '收起' : '展开'}</button>
            </div>
            {showMonitorDiag ? (
              <div className="analysis-block">
                <div>source: {monitorSourceMode} / epoch: {monitorEpoch ?? '-'}</div>
                <div>collector nats: {collectorHealth?.nats_connected == null ? '-' : (collectorHealth.nats_connected ? 'up' : 'down')} / alarms: {monitorSnapshot.alarmCount}</div>
                <div>snapshot: {monitorUpdatedText} / last_ok: {monitorLastSuccessText} / failures: {monitorConsecutiveFailures}</div>
                <div>scope_uid: {scopeUidCoverage.toFixed(1)}% / cpu/mem_cov: {nodeCpuCoverage.toFixed(1)}% / {nodeMemCoverage.toFixed(1)}%</div>
                {monitorAvailableEpochs.length > 0 ? (
                  <div className="monitor-epoch-row">
                    <span>epoch</span>
                    <select value={String(monitorEpoch ?? '')} onChange={(e) => setMonitorEpoch(Number(e.target.value))}>
                      {monitorAvailableEpochs.map((ep) => (
                        <option key={`ep-runtime-${ep}`} value={String(ep)}>{ep}</option>
                      ))}
                    </select>
                  </div>
                ) : null}
                <div className="monitor-filter-row">
                  <span>告警过滤</span>
                  <select value={alarmSeverityFilter} onChange={(e) => setAlarmSeverityFilter(e.target.value)}>
                    <option value="all">severity: all</option>
                    <option value="critical">severity: critical</option>
                    <option value="warning">severity: warning</option>
                    <option value="info">severity: info</option>
                  </select>
                  <select value={alarmScopeFilter} onChange={(e) => setAlarmScopeFilter(e.target.value)}>
                    <option value="all">scope: all</option>
                    <option value="node">scope: node</option>
                    <option value="link">scope: link</option>
                    <option value="flow">scope: flow</option>
                  </select>
                </div>
                {monitorError ? <div className="analysis-error">{monitorError}</div> : null}
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
      {showLayerDrawer ? (
        <div className="layer-modal-backdrop" onClick={() => setShowLayerDrawer(false)}>
          <div className="layer-modal" onClick={(e) => e.stopPropagation()}>
            <div className="layer-header">
              <span>图层选择</span>
              <button type="button" onClick={() => setShowLayerDrawer(false)}>关闭</button>
            </div>
            <div className="layer-grid">
              <label><input type="checkbox" checked={layerPrefs.nodeLeo} onChange={() => toggleLayer('nodeLeo')} /> 卫星</label>
              <label><input type="checkbox" checked={layerPrefs.nodeAircraft} onChange={() => toggleLayer('nodeAircraft')} /> 飞机</label>
              <label><input type="checkbox" checked={layerPrefs.nodeShip} onChange={() => toggleLayer('nodeShip')} /> 舰船</label>
              <label><input type="checkbox" checked={layerPrefs.linkSatSat} onChange={() => toggleLayer('linkSatSat')} /> 星间链路</label>
              <label><input type="checkbox" checked={layerPrefs.linkSatMobile} onChange={() => toggleLayer('linkSatMobile')} /> 星地/空链路</label>
              <label><input type="checkbox" checked={layerPrefs.linkOther} onChange={() => toggleLayer('linkOther')} /> 非卫星链路</label>
              <label><input type="checkbox" checked={layerPrefs.showTrails} onChange={() => toggleLayer('showTrails')} /> 轨迹</label>
              <label><input type="checkbox" checked={layerPrefs.showOrbits} onChange={() => toggleLayer('showOrbits')} /> 轨道环</label>
              <label><input type="checkbox" checked={layerPrefs.showLabels} onChange={() => toggleLayer('showLabels')} /> 标签</label>
            </div>
            <div className="fault-row-actions"><button type="button" onClick={resetLayerPrefs}>重置图层</button></div>
          </div>
        </div>
      ) : null}
      <div className="bottom-statusbar">
        <div className="bottom-statusbar-track">
          <span className={`badge ${connected ? 'ok' : 'error'}`}>{connected ? 'WS 正常' : 'WS 断开'}</span>
          <span className={`badge ${runtimeHealth.stalenessMs >= STALE_ERROR_MS ? 'error' : runtimeHealth.stalenessMs >= STALE_WARN_MS ? 'warn' : 'ok'}`}>延迟 <span className="status-value value-ms">{stalenessMsText}</span></span>
          <span className="status-chip">sim <span className="status-value value-time">{simTimeText}</span></span>
          <span className="status-chip">tick <span className="status-value value-ms">{tickText}</span></span>
          <span className="status-chip">fps <span className="status-value value-fps">{ingestFpsText}</span></span>
          <span className="status-chip">play <span className="status-value value-domain">{playbackText}</span></span>
          <span className="status-chip">speed <span className="status-value value-fps">{speedText}</span></span>
          <span className="status-chip">queue <span className="status-value value-int">{queueDepth}</span></span>
          <span className="status-chip">nodes <span className="status-value value-int">{frame ? frame.nodes.length : 0}</span></span>
          <span className="status-chip">links <span className="status-value value-int">{frame ? frame.metrics.edge_count : 0}</span></span>
          <span className="status-chip">avg_degree <span className="status-value value-fps">{avgDegreeText}</span></span>
          <span className="status-chip">mobile <span className="status-value value-domain">{mobileConnectedText}</span></span>
          <span className="status-chip">QoE-I <span className="status-value value-pct">{qoeImbalanceText}</span></span>
          <span className="status-chip">manual_fault(node/link) <span className="status-value value-domain">{manualFaultNodeCount}/{manualFaultLinkCount}</span></span>
          <span className="status-chip">monitor_alarms <span className="status-value value-int">{monitorSnapshot.alarmCount}</span></span>
          <span className={`status-chip ${scopeUidCoverageWarn ? 'status-chip-warn' : ''}`}>scope_uid <span className="status-value value-pct">{scopeUidCoverage.toFixed(1)}%</span></span>
          <span className={`status-chip ${nodeMetricCoverageWarn ? 'status-chip-warn' : ''}`}>cpu/mem_cov <span className="status-value value-domain">{nodeCpuCoverage.toFixed(1)}%/{nodeMemCoverage.toFixed(1)}%</span></span>
          <span className="status-chip">monitor_last <span className="status-value value-time">{monitorUpdatedText}</span></span>
          <span className="status-chip">snapshot_ok <span className="status-value value-time">{monitorLastSuccessText}</span></span>
          <span className="status-chip">snapshot_fail <span className="status-value value-int">{monitorConsecutiveFailures}</span></span>
          <button type="button" className="status-btn" onClick={exportMonitorSnapshotJson}>导出JSON</button>
          <button type="button" className="status-btn" onClick={() => replayFileInputRef.current?.click()}>导入回放</button>
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
          {monitorActionStatus ? <span className="status-note">最近操作: {monitorActionStatus}</span> : null}
        </div>
      </div>
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      {toast ? <div className={`toast ${toast.level}`}>{toast.text}</div> : null}
    </div>
  );
}
