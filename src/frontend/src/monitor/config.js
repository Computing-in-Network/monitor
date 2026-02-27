const DEFAULT_MONITOR_THRESHOLDS = Object.freeze({
  cpu_ratio: { warning: 0.7, critical: 0.9 },
  mem_ratio: { warning: 0.75, critical: 0.9 },
  loss_rate: { warning: 0.01, critical: 0.05 },
  rtt_ms: { warning: 80, critical: 150 },
  jitter_ms: { warning: 20, critical: 50 }
});

function readEnv(key) {
  try {
    if (typeof import.meta !== 'undefined' && import.meta.env && key in import.meta.env) {
      return import.meta.env[key];
    }
  } catch {
    // ignore
  }
  return undefined;
}

function toNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function cloneThresholds(input) {
  return {
    cpu_ratio: { ...input.cpu_ratio },
    mem_ratio: { ...input.mem_ratio },
    loss_rate: { ...input.loss_rate },
    rtt_ms: { ...input.rtt_ms },
    jitter_ms: { ...input.jitter_ms }
  };
}

function mergeThresholds(base, patch) {
  if (!patch || typeof patch !== 'object') {
    return base;
  }
  const next = cloneThresholds(base);
  for (const metric of Object.keys(next)) {
    const segment = patch[metric];
    if (!segment || typeof segment !== 'object') {
      continue;
    }
    next[metric].warning = toNumber(segment.warning, next[metric].warning);
    next[metric].critical = toNumber(segment.critical, next[metric].critical);
  }
  return next;
}

export function getMonitorThresholds() {
  let thresholds = cloneThresholds(DEFAULT_MONITOR_THRESHOLDS);

  const jsonRaw = readEnv('VITE_MONITOR_THRESHOLDS');
  if (jsonRaw) {
    try {
      thresholds = mergeThresholds(thresholds, JSON.parse(jsonRaw));
    } catch {
      // ignore invalid JSON and keep defaults
    }
  }

  thresholds.cpu_ratio.warning = toNumber(readEnv('VITE_MONITOR_CPU_WARNING'), thresholds.cpu_ratio.warning);
  thresholds.cpu_ratio.critical = toNumber(readEnv('VITE_MONITOR_CPU_CRITICAL'), thresholds.cpu_ratio.critical);
  thresholds.mem_ratio.warning = toNumber(readEnv('VITE_MONITOR_MEM_WARNING'), thresholds.mem_ratio.warning);
  thresholds.mem_ratio.critical = toNumber(readEnv('VITE_MONITOR_MEM_CRITICAL'), thresholds.mem_ratio.critical);
  thresholds.loss_rate.warning = toNumber(readEnv('VITE_MONITOR_LOSS_WARNING'), thresholds.loss_rate.warning);
  thresholds.loss_rate.critical = toNumber(readEnv('VITE_MONITOR_LOSS_CRITICAL'), thresholds.loss_rate.critical);
  thresholds.rtt_ms.warning = toNumber(readEnv('VITE_MONITOR_RTT_WARNING'), thresholds.rtt_ms.warning);
  thresholds.rtt_ms.critical = toNumber(readEnv('VITE_MONITOR_RTT_CRITICAL'), thresholds.rtt_ms.critical);
  thresholds.jitter_ms.warning = toNumber(readEnv('VITE_MONITOR_JITTER_WARNING'), thresholds.jitter_ms.warning);
  thresholds.jitter_ms.critical = toNumber(readEnv('VITE_MONITOR_JITTER_CRITICAL'), thresholds.jitter_ms.critical);
  return thresholds;
}

export { DEFAULT_MONITOR_THRESHOLDS };

