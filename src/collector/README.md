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
  - 参数：`target=original|dlq`（默认 `original`）
- `POST /api/v1/ingest/{kind}`：上报入口
- `POST /api/v2/ingest/{kind}`：v2 上报入口（与 v1 并行）
  - kind 取值：`node_metric`、`node-metric`、`link_metric`、`link-metric`、`flow`、`alarm`
- Header：
  - `x-api-token`：上报鉴权

## 必填字段
- 所有事件必须包含：
  - `schema_version=monitor.v1|monitor.v2`
  - `message_id`
- `timestamp` 缺省可由服务端补齐

## 错误码
- `INVALID_PAYLOAD`：字段校验失败
- `INVALID_KIND`：不支持事件类型
- `AUTH_TOKEN_MISSING`：缺少 token
- `AUTH_TOKEN_INVALID`：token 不匹配
- `RATE_LIMITED`：同一 producer 触发限流
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
- `PRODUCER_RATE_LIMIT_RPM`：按 producer 每分钟限流阈值（`0` 表示关闭）

## Topic 规范（I-031）
- 事件主题格式：`{environment}.{domain}.{entity}.{version}`
- 示例：`dev.raw.node_metric.v1`、`dev.raw.link_metric.v2`
- DLQ 主题格式：`{environment}.dlq.{domain}.{entity}.{version}`
- 示例：`dev.dlq.raw.node_metric.v1`

## DLQ 与回放
- NATS 发布失败：失败事件进入 failed-events 审计（状态 `pending`）
- DB 写入失败：失败事件进入审计，并尝试发布到对应 DLQ 主题
- 手动回放：
  - `POST /api/v1/ops/failed-events/replay?target=original`
  - `POST /api/v1/ops/failed-events/replay?target=dlq`

## Timescale 写入
- 入站事件在发布 NATS 成功后会尝试写入 Timescale（四类表）
- 写库失败不会阻断主请求，会记录 `DB_WRITE_FAILED` 并进入 failed-events 审计
