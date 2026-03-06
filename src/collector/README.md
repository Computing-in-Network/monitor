# Collector 服务（V1）

## 启动方式
1. 根据需要修改 `config.example.yaml`（或通过环境变量覆盖）
2. 设置 `COLLECTOR_API_TOKEN` 与 `NATS_URL`（可选）
3. 通过 Docker Compose 启动：
   - 仓库根目录执行：`docker compose -f deploy/docker-compose.base.yml up -d --build`
   - 或在当前目录执行：`docker compose -f ../../deploy/docker-compose.base.yml up -d --build`

4. 可选：启动持续上报器（从拓扑 WS 持续生成 node/link 指标）
   - `docker compose -f deploy/docker-compose.base.yml --profile reporter up -d --build`
   - 默认读取 `ws://host.docker.internal:8765` 并上报到 `http://monitor-collector:9010`
   - 默认 `METRIC_SOURCE=docker`（通过 `/var/run/docker.sock` 采集容器 CPU/内存/网络；失败自动回退 synthetic）

## 接口说明
- `GET /health`：健康检查
- `GET /metrics`：采集结果统计（`OK/DUPLICATE/INVALID_*` 等）
- `GET /api/v1/monitor/snapshot`：前端聚合快照（`monitor.nodes/links/alarms/snapshot_version`）
  - 支持查询参数：`topology_epoch`（可选，按拓扑批次拉取）
  - 响应头：`ETag`、`Last-Modified`
  - 支持条件请求：`If-None-Match` 命中后返回 `304`
- `GET /api/v1/ops/failed-events`：查看失败事件审计与状态
- `POST /api/v1/ops/failed-events/replay`：手动重放失败事件
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

## Docker 实采模式（reporter）
- reporter 支持 `--metric-source docker|synthetic`
- `docker` 模式会额外上报：
  - `docker_name`
  - `cpu_usage_cores`、`cpu_limit_cores`
  - `mem_usage_bytes`、`mem_limit_bytes`
- 容器映射优先级：
  - `--node-mapping-csv`（`node_id,container_name,container_id`）
  - 其次尝试 erv300 命名推断（如 `SAT-POLAR-001 -> erv300_r_1`）
  - 最后回退 `container_name=node_id`

## 发布重试配置
- `PUBLISH_RETRIES`：NATS 发布失败重试次数（默认 `2`）
- `PUBLISH_RETRY_BACKOFF_MS`：重试间隔毫秒（默认 `200`）
- `FAILED_EVENTS_MAX_ITEMS`：内存保留失败事件上限（默认 `2000`）
- `FAILED_EVENTS_AUDIT_FILE`：失败事件审计落盘路径（默认 `/tmp/collector_failed_events.jsonl`）
- `TSDB_ENABLED`：是否启用 Timescale 写入（默认 `true`）
- `TSDB_DSN`：Timescale 连接串
- `TSDB_SCHEMA`：写入 schema（默认 `monitor_ts`）
- `CORS_ALLOW_ORIGINS`：允许跨域来源（默认 `*`，多个用逗号分隔）
- `TOPO_WS_URL`：持续上报器连接的拓扑 WS 地址（默认 `ws://host.docker.internal:8765`）
- `COLLECTOR_URL`：持续上报器上报目标地址（默认 `http://monitor-collector:9010`）
- `TOPOLOGY_EPOCH`：持续上报写入的 topology epoch（默认 `default`）
- `REPORT_INTERVAL_S`：持续上报间隔秒数（默认 `2`）
- `REPORT_TIMEOUT_S`：持续上报单请求超时秒数（默认 `5`）
- `REPORT_MAX_CONCURRENCY`：持续上报并发（默认 `64`）
- `REPORT_SEED`：持续上报随机种子（默认 `42`）
- `METRIC_SOURCE`：上报指标来源（`docker` 或 `synthetic`，默认 `docker`）
- `NODE_MAPPING_CSV`：可选，节点到容器映射 CSV 路径
- `DOCKER_TIMEOUT_S`：docker 采样超时（默认 `5`）
- `DOCKER_WORKERS`：docker 采样并发 worker（默认 `16`）

## Timescale 写入
- 入站事件在发布 NATS 成功后会尝试写入 Timescale（四类表）
- 写库失败不会阻断主请求，会记录 `DB_WRITE_FAILED` 并进入 failed-events 审计
