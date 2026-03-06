# ISSUE-0041 Reporter 对接 Docker 实采指标

## 背景

当前 `topology_metric_reporter.py` 仅按拓扑帧生成模拟指标（`cpu_ratio/mem_ratio/tx_bps/rx_bps`），未对接真实 Docker 容器运行数据，导致监控面板中的节点资源指标与容器真实状态脱节。

## 目标

1. Reporter 支持从 Docker 实时采集节点容器指标。
2. 保留原有 synthetic 模式，作为故障或无 Docker 环境下的回退。
3. 在不破坏现有接口契约的前提下，补充 Docker 相关扩展字段。
4. 通过 compose 默认启用 Docker 模式，降低联调门槛。

## 范围

- `src/collector/scripts/topology_metric_reporter.py`
  - 新增 `metric_source` 选项（`docker|synthetic`）
  - 新增 Docker 采集状态机（CPU、内存、网络）
  - 支持节点到容器映射 CSV（`node_id -> container_name/container_id`）
  - 支持 erv300 节点名到容器名推断
  - 采集失败自动按节点回退 synthetic，并标记状态
- `src/collector/requirements.txt`
  - 增加 Docker SDK 依赖
- `deploy/docker-compose.base.yml`
  - reporter 默认启用 `METRIC_SOURCE=docker`
  - 透传 Docker 采样相关参数
  - 挂载 `/var/run/docker.sock`
- `src/collector/README.md`
  - 补充 Docker 实采模式说明、字段说明、配置参数

## 输出字段

在 `node_metric` 上报中新增/补充：

- `docker_name`
- `metric_source`
- `cpu_usage_cores`
- `cpu_limit_cores`
- `mem_usage_bytes`
- `mem_limit_bytes`

## 验收标准

1. reporter 启动日志可见 `metric_source=docker`。
2. `monitor/snapshot` 返回中节点指标包含 `metric_source=docker` 与 Docker 扩展字段。
3. Docker 不可用或节点无法匹配容器时，reporter 不崩溃，自动回退 synthetic。
4. 现有 `link_metric` 上报逻辑保持兼容。

## 风险与说明

- 若未显式配置容器资源限制，`cpu_limit_cores/mem_limit_bytes` 可能取宿主机可见上限。
- 若部署环境开启 HTTP 代理，本地自检访问 `127.0.0.1` 需使用 `--noproxy '*'`。

## 对应分支

- `issue/41-reporter-docker-metrics`
