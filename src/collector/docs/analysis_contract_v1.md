# 高级分析契约冻结（analysis.v1）

更新时间：2026-02-26  
状态：`frozen-for-frontend`

## 1. 统一入口

- `POST /api/v1/bff/analysis/run`

前端只传轻量参数：
- `mode`: `auto|focused|global`
- `scope_type`: `node|link|network`
- `scope_id`: `NODE_ID | A<->B | all`
- `topology_epoch`

## 2. 响应结构（前端可依赖）

- `status`
- `contract_version`（固定 `analysis.v1`）
- `input`（前端传入）
- `resolved`（后端实际执行模式）
- `summary`
- `topology_impact`
- `tasks`
- `alerts`
- `meta`

## 3. 错误模型（固定三类）

后端在 4xx/5xx 时返回：
```json
{
  "detail": {
    "error_code": "INVALID_SCOPE|INSUFFICIENT_DATA|INTERNAL_ERROR",
    "error_message": "..."
  }
}
```

语义：
- `INVALID_SCOPE`：前端传参语义错误
- `INSUFFICIENT_DATA`：后端数据不足无法分析
- `INTERNAL_ERROR`：后端内部异常

## 4. 前端验收清单

1. `focused + node` 返回 200  
2. `focused + link` 返回 200  
3. `global + network` 返回 200  
4. 非法 scope 触发 `INVALID_SCOPE`  
5. 页面只依赖 `summary/topology_impact/tasks/alerts` 渲染
