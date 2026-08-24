# MiniMax 视频售卖工作台

本地启动一个 MiniMax 多 Key 视频平台：管理员在网页中管理数据库内的上游 Key，并通过自增 ID 管理访问令牌及其独立额度；令牌按视频价格扣费，可查询状态、查看历史并下载结果。上游 Key 原文不会通过接口返回浏览器或令牌使用者。

## 启动

```powershell
python -m pip install -r requirements.txt
$env:DATABASE_URL = "postgresql+psycopg://postgres@127.0.0.1:5432/minimax_platform"
$env:MINIMAX_ADMIN_TOKEN = "替换为随机管理令牌"
python server.py
```

访问 <http://127.0.0.1:8000/>。

首次启动且数据库中还没有上游 Key 时，会从 `.env` 导入旧配置；以后在管理员网页的“上游 Key 管理”中新增、改名、更换、停用或删除：

```dotenv
MINIMAX_API_KEYS=key-1,key-2
```

- `MINIMAX_API_KEYS`：仅用于首次导入 Hailuo 2.3 Key 池，默认按每 Key 每日 3 条轮询。
- `MINIMAX_POLL_SECONDS`：后台任务查询间隔，默认 10 秒。

## 上游 Key 管理 API

管理接口仅允许本机访问，并复用 `MINIMAX_ADMIN_TOKEN`：

- `POST /api/admin/keys`：保存新的 MiniMax Key。
- `GET /api/admin/keys`：返回 Key 掩码、启用状态和当日配额，不返回原文。
- `PATCH /api/admin/keys/{id}`：改名、启停或更换 Key，立即生效。
- `DELETE /api/admin/keys/{id}`：删除未绑定历史任务的 Key；已绑定时应停用。

## 令牌管理 API

管理接口仅允许本机访问，并支持令牌 CRUD：

- `POST /api/admin/tokens`：新增令牌，原文只返回一次。
- `GET /api/admin/tokens`、`GET /api/admin/tokens/{id}`：列出或读取令牌安全字段和额度。
- `PATCH /api/admin/tokens/{id}`：修改名称或启用状态。
- `DELETE /api/admin/tokens/{id}`：禁用并软删除令牌，保留历史和账本；有冻结额度时拒绝删除。
- `POST /api/admin/tokens/{id}/recharge`：按人民币元 1:1 充值。

令牌 ID 由数据库自动递增。所有视频接口使用平台签发的 `mmx_live_...` 访问令牌，不是 MiniMax 原始 Key。

```http
Authorization: Bearer mmx_live_...
```

- `GET /v1/models`：模型、模式、时长、分辨率目录。
- `GET /v1/account`：余额、冻结额、可用额。
- `POST /v1/videos`：创建任务，必须提供唯一的 `Idempotency-Key`。
- `GET /v1/videos/{id}`：查询任务并触发结算。
- `GET /v1/videos`：当前令牌的历史记录。
- `GET /v1/videos/{id}/download`：鉴权后代理下载视频。

创建示例：

```json
{
  "model": "MiniMax-Hailuo-2.3",
  "mode": "text",
  "prompt": "一只橘猫坐在窗边，午后阳光缓慢移动。",
  "duration": 6,
  "resolution": "768P"
}
```

当前只开放 `MiniMax-Hailuo-2.3`（文生视频、图生视频）和 `MiniMax-Hailuo-2.3-Fast`（仅图生视频）。两者支持 768P（6/10 秒）和 1080P（6 秒）；售价按官方公开价格约 5 折计算，网页会同时展示售价、官方价和折扣。网页支持图片文件转 Data URL 或填写图片公网 URL。

## 账务规则

- 令牌充值和余额按人民币元显示，充值 1 元，余额增加 1 元。
- 账本内部仍使用整数分，避免浮点金额误差。
- 创建前按模型、时长和分辨率的固定 SKU 价格预扣；成功后按创建时报价结算；明确失败释放预扣。
- 网络超时、5xx 或无法确认是否创建时不换 Key、不重复提交，任务停在 `submitting_unknown` 并保留冻结额供人工处理。
- 访问令牌只返回一次，数据库只保存 SHA-256；上游 Key 因调用需要可恢复地保存在管理数据库中，但网页、接口、日志和调试导出只显示掩码。请限制数据库访问并配置 `MINIMAX_ADMIN_TOKEN`。

## 验证

```powershell
python -m unittest -v
```

PostgreSQL 集成测试：

```powershell
$env:MINIMAX_TEST_DATABASE_URL = "postgresql+psycopg://postgres@127.0.0.1:5432/minimax_test"
python -m unittest -v test_platform.PostgreSQLPlatformTests
```
