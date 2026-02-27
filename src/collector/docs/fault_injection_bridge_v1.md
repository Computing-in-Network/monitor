# Monitor Fault Injection Bridge（v1）

更新时间：2026-02-26

## 1. 目标

把 dynamic-topo 前端的故障注入回执 `control_ack` 映射为 monitor 告警，形成闭环：
- 注入 -> 告警出现
- 分析/推演 -> 风险变化
- 清除 -> 告警恢复

## 2. 接口

- `POST /api/v1/ops/fault-injection/control-ack`

请求体要求：
- `type=control_ack`
- `ok`、`action`、`request_id`
- 可选：`topology_epoch`
- 注入动作需带 `fault`

支持动作：
- `inject_node_fault`
- `inject_link_fault`
- `clear_fault`
- `clear_all_faults`
- `list_faults`

## 3. 映射规则

1. `fault_type=DAMAGED`
- `scope_type=node`
- `scope_id=target.node_id`

2. `fault_type=INTERRUPTED`
- `scope_type=link`
- `scope_id=normalize(target.a,target.b)`，格式 `A<->B`

3. 去重
- `deduplicated=true` 时不重复生成新告警，仅刷新观察时间

4. 恢复
- `clear_fault/clear_all_faults/list_faults` 会触发恢复事件
- 恢复事件将从 snapshot 当前活动告警中移除

## 4. 前端联调建议

1. 前端收到 dynamic-topo WS 的 `control_ack` 后，原样转发到本接口。
2. 同步带上 `topology_epoch`（建议 `1708848000`）。
3. 每次注入/清除后拉取：
- `GET /api/v1/bff/snapshot`
- `POST /api/v1/bff/analysis/run`
- `POST /api/v1/bff/simulation/create` + `/step`

## 5. 自测

运行：

```bash
python src/collector/scripts/test_fault_injection_loop.py
```

通过标志：

```text
fault_injection_loop_test_ok
```
