# Collector 服务（V1）

## 启动方式
1. 根据需要修改 `config.example.yaml`（或通过环境变量覆盖）
2. 设置 `COLLECTOR_API_TOKEN` 与 `NATS_URL`（可选）
3. 通过 Docker Compose 启动：
   - `docker compose -f ../../docs/deploy/docker-compose.base.yml up -d --build`

## 接口说明
- `GET /health`：健康检查
- `GET /metrics`：采集结果统计（`OK/DUPLICATE/INVALID_*` 等）
- `GET /api/v1/monitor/snapshot`：前端聚合快照（`monitor.nodes/links/alarms/snapshot_version`）
  - 支持查询参数：`topology_epoch`（可选，按拓扑批次拉取）
  - 响应头：`ETag`、`Last-Modified`
  - 支持条件请求：`If-None-Match` 命中后返回 `304`
- `GET /api/v1/ops/failed-events`：查看失败事件审计与状态
- `POST /api/v1/ops/failed-events/replay`：手动重放失败事件
- `GET /api/v1/analysis/forecast/models`：查询已注册预测模型列表
- `GET /api/v1/analysis/forecast/lstm`：预测查询（支持注册模型优先，fallback 兜底）
- `POST /api/v1/ingest/{kind}`：上报入口
  - kind 取值：`node_metric`、`node-metric`、`link_metric`、`link-metric`、`flow`、`alarm`
- Header：
  - `x-api-token`：上报鉴权

## 必填字段
- 所有事件必须包含：
  - `schema_version=monitor.v1`
  - `message_id`
- `timestamp` 缺省可由服务端补齐

## 错误码
- `INVALID_PAYLOAD`：字段校验失败
- `INVALID_KIND`：不支持事件类型
- `UNAUTHORIZED`：鉴权失败
- `NATS_UNAVAILABLE`：事件总线不可用

## 幂等策略
- 按 `message_id` 做去重

## 返回结构
- success: `{"status":"ok","event_type":"node_metric","message_id":"...","trace_id":"..."}`
- duplicate: `{"status":"duplicate","event_type":"node_metric","message_id":"...","trace_id":"..."}`
- error: `{"status":"error","error_code":"INVALID_PAYLOAD","error_message":"...","trace_id":"..."}`

## 本地联调
- 示例上报：`./scripts/send_example.sh`
- smoke 验证（401/422/200）：`./scripts/smoke_api.sh`

## 发布重试配置
- `PUBLISH_RETRIES`：NATS 发布失败重试次数（默认 `2`）
- `PUBLISH_RETRY_BACKOFF_MS`：重试间隔毫秒（默认 `200`）
- `FAILED_EVENTS_MAX_ITEMS`：内存保留失败事件上限（默认 `2000`）
- `FAILED_EVENTS_AUDIT_FILE`：失败事件审计落盘路径（默认 `/tmp/collector_failed_events.jsonl`）
- `TSDB_ENABLED`：是否启用 Timescale 写入（默认 `true`）
- `TSDB_DSN`：Timescale 连接串
- `TSDB_SCHEMA`：写入 schema（默认 `monitor_ts`）
- `FORECAST_MODEL_DIR`：预测模型目录（默认 `/tmp/forecast_models/lstm`）

## Timescale 写入
- 入站事件在发布 NATS 成功后会尝试写入 Timescale（四类表）
- 写库失败不会阻断主请求，会记录 `DB_WRITE_FAILED` 并进入 failed-events 审计

## 预测服务编排（I-034）
- `GET /api/v1/analysis/forecast/models`
  - 返回模型注册列表（`model_id/version/backend/validation_mape`）
- `GET /api/v1/analysis/forecast/lstm`
  - 参数：
    - 必填：`event_type/metric/entity_id`
    - 可选：`model_id/model_version/strategy(auto|registered|fallback)`、`horizon/window/history_limit/topology_epoch`
  - 返回：
    - `model_type/model_id/model_version/strategy`
    - `validation_mape`（有训练模型时）
    - `points` 预测点

## 故障追踪服务（I-035）
- `POST /api/v1/fault/spread`
- `POST /api/v1/fault/spread/analyze`（兼容路径）
  - 必填：`alarm_nodes(list)`、`links(list)`
  - 可选：`mode(single_point|cascade)`、`max_depth`、`cascade_threshold`
  - 返回：`impacted_nodes/impacted_links/subgraph/paths`
- `POST /api/v1/fault/task-impact`
- `POST /api/v1/fault/task-impact/evaluate`（兼容路径）
  - 必填：`tasks(list)`、`link_metrics(dict)`
  - 可选：`fault_spread`、`rtt_warn_ms`、`loss_warn_rate`
  - 返回：`tasks/work_orders/impacted_link_count`

## 故障 API 自测
- 先安装依赖（Python 3.10）：
  - `uv venv --python /usr/bin/python3.10 .venv310 && source .venv310/bin/activate && uv pip install -r requirements.txt httpx`
- 运行：
  - `python scripts/test_fault_api.py`
