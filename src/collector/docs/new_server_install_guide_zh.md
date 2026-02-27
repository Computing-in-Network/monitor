# Monitor + Dynamic-Topo 新服务器安装说明（中文）

更新时间：2026-02-27  
适用范围：`monitor` 后端 + `dynamic-topo` 前端联调环境

## 1. 目标

在一台新服务器上完成以下能力：
- collector 启动并可接收数据
- TimescaleDB + NATS 可用
- dynamic-topo 前端通过 `/monitor-api` 反向代理访问 collector
- 支持 300 节点压力数据、故障注入、高级分析、推演

## 2. 环境要求

- 操作系统：Ubuntu 22.04+（推荐）
- 软件：
  - Docker / Docker Compose
  - Git
  - Node.js 20+（构建前端）
- 端口：
  - `8080`：前端
  - `8765`：dynamic-topo WS（可选）
  - `9010`：collector
  - `4222/8222`：NATS
  - `55432`：TimescaleDB

## 3. 拉取代码

```bash
mkdir -p ~/work && cd ~/work
git clone https://github.com/Computing-in-Network/monitor.git
git clone https://github.com/Computing-in-Network/dynamic-topo.git
```

## 4. 启动 monitor 基础服务

```bash
cd ~/work/monitor
docker compose -f docs/deploy/docker-compose.base.yml up -d --build
```

说明：
- 这一步会启动 `net_analysis_collector`、`net_analysis_nats`
- 如已单独启动 TimescaleDB，请确保 collector 的 `TSDB_DSN` 可连通

## 5. 启动 300 节点压力数据

```bash
cd ~/work/monitor
bash docs/scripts/frontend/start_stress_feeder.sh
```

停止：

```bash
bash docs/scripts/frontend/stop_stress_feeder.sh
```

## 6. 构建并启动前端（dynamic-topo）

```bash
cd ~/work/dynamic-topo/frontend
npm ci
npm run build
```

使用 nginx 容器挂载 `dist` 与 `nginx.conf`：

```bash
docker rm -f topo-frontend-bff >/dev/null 2>&1 || true
docker run -d \
  --name topo-frontend-bff \
  --network monitor-net \
  -p 8080:80 \
  -v ~/work/dynamic-topo/frontend/dist:/usr/share/nginx/html:ro \
  -v ~/work/dynamic-topo/frontend/nginx.conf:/etc/nginx/conf.d/default.conf:ro \
  nginx:1.27-alpine
```

关键点：
- `nginx.conf` 中 `/monitor-api/` 的上游应为 `http://net_analysis_collector:9010`

## 7. 健康检查

后端联通检查：

```bash
cd ~/work/monitor
bash docs/scripts/frontend/check_collector_bff.sh
```

前端容器检查（容器网络）：

```bash
docker run --rm --network monitor-net curlimages/curl:8.12.1 \
  sh -lc "curl -sS -o /dev/null -w '%{http_code}\n' http://topo-frontend-bff/ && \
          curl -sS -o /dev/null -w '%{http_code}\n' http://topo-frontend-bff/monitor-api/health"
```

预期均为 `200`。

## 8. 联调操作建议

1. 打开前端：`http://<server-ip>:8080`
2. 先确认节点详情中可看到 `cpu/mem`
3. 故障面板执行注入/清除，观察告警数量变化
4. 执行“高级分析”和“运行推演”，检查结果卡片是否刷新

## 9. 常见问题

1. 前端启动即报 `host.docker.internal` 解析失败  
处理：将 `frontend/nginx.conf` 上游改成 `net_analysis_collector`

2. snapshot 有 300 节点但前端节点无指标  
处理：确认压力 feeder 使用 dynamic-topo 同名节点（`SAT-POLAR/SAT-INCL/AIR/SHIP`）

3. 高级分析返回 422  
处理：
- 检查请求是否走 `/api/v1/bff/analysis/run`
- 检查 `topology_epoch` 是否一致（建议 `1708848000`）
- 确认 snapshot 中已有 link metrics

4. 推演 timeline 为空  
处理：
- 确保 `simulation/create` 返回有效 `simulation_id`
- 连续调用 `/step` 直到 `completed`
- 再查 `/timeline`

## 10. 相关文档

- [analysis_contract_v1.md](/home/zyren/monitor/src/collector/docs/analysis_contract_v1.md)
- [fault_injection_bridge_v1.md](/home/zyren/monitor/src/collector/docs/fault_injection_bridge_v1.md)
- [frontend_joint_test_spec_v1.md](/home/zyren/monitor/src/collector/docs/frontend_joint_test_spec_v1.md)
