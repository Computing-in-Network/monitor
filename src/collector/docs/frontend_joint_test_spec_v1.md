# 前后端联测规范（v1.1）

更新时间：2026-02-26  
适用分支：`develop`

## 1. 联测目标

验证前端在统一入口下可稳定完成：
1. 监控展示（snapshot/series/forecast）
2. 高级分析（analysis/run）
3. 推演回放（simulation）

## 2. 环境约束

1. 前端与 collector 必须在同一可达网络。  
2. 优先使用容器内地址：`http://net_analysis_collector:9010`。  
3. `topology_epoch` 建议固定：`1708848000`。  
4. 联测期间保持压力数据进程运行（300 节点级别）。

## 3. 接口清单（必测）

1. `GET /api/v1/bff/snapshot?topology_epoch=1708848000`
2. `GET /api/v1/bff/series?event_type=link_metric&metric=rtt_ms&entity_id=SAT-INCL-001<->SAT-INCL-011&limit=120`
3. `GET /api/v1/bff/forecast/lstm?event_type=link_metric&metric=rtt_ms&entity_id=SAT-INCL-001<->SAT-INCL-011&strategy=fallback&horizon=12&window=12`
4. `POST /api/v1/bff/analysis/run`
5. `POST /api/v1/bff/simulation/create`
6. `POST /api/v1/bff/simulation/{simulation_id}/step`
7. `GET /api/v1/bff/simulation/{simulation_id}/timeline`
8. `GET /api/v1/ops/slo`

## 4. 高级分析调用规范（冻结）

请求体仅传轻量参数：
```json
{
  "mode": "focused",
  "scope_type": "link",
  "scope_id": "SAT-INCL-001<->SAT-INCL-011",
  "topology_epoch": "1708848000"
}
```

`mode` 取值：
- `focused`：针对当前选中节点/链路
- `global`：全网分析
- `auto`：后端自动判定

错误返回固定三类：
- `INVALID_SCOPE`
- `INSUFFICIENT_DATA`
- `INTERNAL_ERROR`

## 5. 推演最小流程

1. 创建：
```json
{
  "scenario_type": "link_down",
  "topology_epoch": "1708848000",
  "steps_total": 5,
  "params": { "link_id": "SAT-INCL-001<->SAT-INCL-011" }
}
```
2. 循环调用 `/step` 直到 `status=completed`
3. 查询 `/timeline` 并渲染风险变化

## 6. 验收标准

1. 三类分析入口全为 200：
   - `focused(node)`
   - `focused(link)`
   - `global(network)`
2. `analysis/run` 响应必须包含：
   - `summary`
   - `topology_impact`
   - `tasks`
   - `alerts`
3. 推演流程必须可完成且 `timeline` 非空。
4. 60 秒连续轮询错误数为 0。

## 7. 问题上报模板（前端）

- 页面操作：
- 请求 URL：
- 请求体：
- 响应状态码：
- 响应体：
- 复现概率：
- 发生时间（UTC+8）：
