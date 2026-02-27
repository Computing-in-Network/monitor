# Monitor Frontend (Merged)

该前端已并入 `monitor` 仓库，作为联调基线版本。

## 开发启动

```bash
cd src/frontend
npm install
npm run dev
```

默认地址：`http://127.0.0.1:5173`

## 打包

```bash
cd src/frontend
npm run build
```

## 对接后端

前端通过 BFF 接口读取数据，核心接口：

- `GET /api/v1/bff/snapshot`
- `POST /api/v1/bff/analysis/run`
- `POST /api/v1/bff/fault/spread`
- `POST /api/v1/bff/fault/task-impact`

后端默认地址示例：`http://127.0.0.1:9010`
