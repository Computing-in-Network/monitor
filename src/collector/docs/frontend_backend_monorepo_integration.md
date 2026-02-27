# Monitor 前后端并仓与实时告警说明（v1）

## 1. 目录调整

前端已并入 `monitor` 仓库：

- 前端源码：`src/frontend`
- 后端 collector：`src/collector`

说明：旧的独立前端项目后续可继续单独演进，但联调基线以本仓库 `src/frontend` 为准。

## 2. 实时告警能力（新增）

后端新增“实时告警引擎”，在接收监控指标时自动判断并维护告警状态：

- 输入：`POST /api/v1/ingest/node_metric`、`POST /api/v1/ingest/link_metric`
- 行为：
  - 指标越阈值时自动 `upsert` 告警（`lifecycle_state=active`）
  - 指标恢复时自动 `recover` 告警（`lifecycle_state=recovered`）
- 告警会同步进入：
  - 快照：`GET /api/v1/monitor/snapshot`（`monitor.alarms`）
  - 时序库：`monitor_ts.alarms`（若 TSDB 可用）

### 2.1 节点阈值

- `status=DOWN` -> critical
- `status=DEGRADED` -> warning
- `cpu_ratio >= 0.93` -> critical
- `cpu_ratio >= 0.85` -> warning
- `mem_ratio >= 0.93` -> critical
- `mem_ratio >= 0.85` -> warning

### 2.2 链路阈值

- `state=DOWN` -> critical
- `state=DEGRADED` -> warning
- `loss_rate >= 0.06` -> critical
- `loss_rate >= 0.03` -> warning
- `rtt_ms >= 280` -> critical
- `rtt_ms >= 180` -> warning
- `jitter_ms >= 35` -> warning

## 3. 前端联调约定

- 统一从 `GET /api/v1/bff/snapshot` 拉取节点、链路、告警。
- 告警唯一标识由后端生成：
  - 节点：`AUTO-RT-NODE-{node_uid}`
  - 链路：`AUTO-RT-LINK-{link_uid}`
- 前端故障注入后，若回执映射成异常指标，告警会自动出现；清除故障后若指标恢复，告警会自动消失。

## 4. 本地运行

### 4.1 前端

```bash
cd src/frontend
npm install
npm run dev
```

默认 Vite 地址：`http://127.0.0.1:5173`。

### 4.2 后端

```bash
cd src/collector
uv sync
uv run uvicorn app.main:create_app --factory --host 0.0.0.0 --port 9010
```

## 5. 闭环验收建议

1. 基线：无注入故障时，`monitor.alarms` 应为 0 或仅历史残留。
2. 注入节点/链路故障：告警总数上升，并可在前端定位到对应对象。
3. 清除故障：对应告警自动恢复并从活跃告警列表移除。
4. 高级分析：基于当前活跃告警自动选种子，返回人类可读结论。
