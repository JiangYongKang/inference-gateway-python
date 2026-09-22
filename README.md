# Inference Gateway（本地确定性模拟）

一个**不依赖任何真实模型**的模型推理网关。所有"推理后端"都是本地确定性模拟器：
输出由 `(backend, version, input)` 直接决定，故障、超时、延迟、灰度结果都可以脚本化
和复现，便于在**版本切换、并发压力、故障回退**下得到可预期的结论。

- Python ≥ 3.14，FastAPI 提供 HTTP 层，`asyncio` 实现并发控制与取消传播。
- 无网络、无随机数：灰度分桶使用 FNV-1a 哈希，时间可通过时钟注入。

---

## 1. 模型别名、版本解析与灰度

配置见 [`config/sample.json`](config/sample.json)。

对一次请求 `(caller, alias, group?)`，版本选择顺序固定为：

1. **调用方分组优先**：若请求带 `group` 且该分组在配置中固定到某版本，则直接选该版本；
2. **否则按灰度权重**：对 `(caller, alias)` 做确定性 FNV-1a 哈希，得到 `[0,1000)` 的桶号，
   按各版本权重（总和必须为 **1000**）的累计区间命中版本。
   - 同一 `caller + alias` 永远命中同一版本，与进程启动次数、并发顺序无关；
   - 不使用 Python 内置 `hash()`（它按进程加盐，不可复现）。

以下情况在**进入配额/后端之前**即被拒绝，且原因互不相同：

| 场景 | error.code | HTTP |
|---|---|---|
| 别名 / 分组 / 固定版本未注册 | `version_not_found` | 404 |
| 灰度权重非正整数或之和 ≠ 1000、分组指向未注册版本 | `invalid_canary` | 422 |
| 解析到的版本没有任何**可用**后端 | `no_backend_available` | 503 |

加载期（`load_config`）也会对权重之和、空后端列表、稳定版本是否存在、配额是否为正等做 fail-fast 校验。

### 版本切换一致性

`request_id` 由调用方提供，幂等账本保证：

- 进行中的同 `request_id` 请求只有**一个**真正执行，其余并发调用共享同一结论；
- 已完成的 `request_id` 再次请求（即使期间灰度/分组配置已改变、原后端已下线）一律
  **重放首次结论**（响应 `source="replay"`），同一调用标识不会得到互相矛盾的结果；
- 已失败的 `request_id` 会被记住，不会"重试成成功"而造成双重计费或矛盾结论。

## 2. 并发隔离与合并

- **全局并发上限 + 有界 FIFO 队列**（`limits.max_concurrency` / `limits.max_queue`）：
  - 有空闲槽：立即进入；
  - 槽满但队列未满：排队等待；
  - 队列也满：**立即快速拒绝** `queue_limit`（429），不无限阻塞、不静默丢弃。
- **请求合并（singleflight）**：相同 `(alias, version, input)` 的并发请求只产生
  **一次**后端调用和一次后端计费；跟随者响应 `source="coalesced", charged=false`。
  某个跟随者被取消只中断它自己的等待，不会取消共享执行。
- **取消/超时清理**：排队中被取消立即离开队列；已在执行中被取消会释放并发槽并
  **退还配额**；取消的请求不写结果缓存、不计成功。
- 结果缓存仅缓存**成功**结果（TTL，默认 300s，时钟可注入）。失败/超时绝不污染缓存。

## 3. 配额语义

配额按 **caller 独立**、**固定时间窗**计数（`quotas.<caller>.limit` / `window_seconds`）：

- 路由校验通过后、进入并发层**之前**预留一个配额单位；
- 超额立即拒绝 `quota_exceeded`（429），与后端错误（502/504）可明确区分；
- 未注册 caller 的配额为 0；
- **退还规则**：队列/并发快速拒绝、排队或执行中被取消 → 退还；
  真正执行过后端链（成功或最终失败）→ 不退还（已消耗一次调用机会）；
- 各 caller 的计数相互隔离。

## 4. 后端超时、异常与回退

一个版本的 `backends` 是**有序**列表。每次后端尝试带独立超时（默认 5s，测试中注入更小值）：

1. 依次尝试所选版本的后端；
2. 某个后端超时（`backend_timeout`）或异常（`backend_error`）时，**不**对同一后端重试，
   继续下一个；
3. 所选版本全部失败后，按配置中的 `stable_version` 作为**回退版本**，依次尝试其后端；
4. 全部失败 → `all_backends_failed`（502）。

回退保证：

- 每个列出的后端**最多调用一次**，不产生重复后端副作用（`completed_keys` 可验证）；
- 一个逻辑请求**只计一次费/一次成功或失败统计**；跟随合并、缓存命中、幂等重放均
  `charged=false`；
- 响应中的 `attempts[]` 列出每次尝试的 `backend/version/latency/ok/error_code`，
  顶层 `version`、`backend` 即**实际生效**的后端与版本，`fallback=true` 表示发生了版本回退。

### 模拟后端可脚本化的行为

`MockBackend`（见 `gateway/backend.py`）支持：`mode="error"|"timeout"`、按 `request_id`
注入单次失败/超时、`latency` 秒延迟、`set_available(False)` 模拟摘除。
超时模式等待一个永不置位的事件，因此恰好在调用方截止时刻被取消，没有墙钟竞争。

## 5. HTTP 接口与兼容范围

- `GET /healthz`
- `POST /v1/infer`

请求：

```json
{"request_id": "r1", "caller": "team-alpha", "alias": "chat",
 "input": "hello", "group": "beta-testers"}
```

旧客户端依赖的响应字段**语义保持不变**：

```json
{
  "request_id": "r1", "alias": "chat", "version": "v1.0",
  "backend": "local-a", "output": "mock-output(local-a,v1.0,hello)",
  "served_from_cache": false,
  "attempts": [{"backend": "local-a", "version": "v1.0",
                "latency": 0.0, "ok": true, "error_code": null}],
  "charged": true, "source": "backend", "fallback": false
}
```

`charged/source/fallback` 为**追加字段**，旧客户端可忽略。错误响应统一为
`{"error": {"code", "message", "details"}}`，HTTP 状态与 `code` 对应（见上表及
`backend_timeout=504`、`quota_exceeded/queue_limit=429`、`all_backends_failed=502`）。

## 6. 本地验证

```bash
uv sync
uv run pytest -q                     # 全部确定性单测（55 个）
uv run pytest -q -s tests/test_service.py   # 查看 JSON 结构化日志
uv run uvicorn main:app --reload     # 启动服务
```

手动调用：

```bash
curl -s localhost:8000/v1/infer -H 'content-type: application/json' -d '{
  "request_id":"demo-1","caller":"team-alpha","alias":"chat","input":"hello"}'
```

### 测试如何覆盖关键结论（全部确定性、无真实模型）

| 文件 | 覆盖点 |
|---|---|
| `tests/test_router.py` | 灰度边界、分组固定、三类拒绝可区分 |
| `tests/test_service.py` | 回退链路、单次计费、缓存不被失败污染、队列溢出、取消退配额、版本切换同 ID 一致、日志含输入/依据/判定 |
| `tests/test_concurrency_stress.py` | 同时打入下成功/拒绝数量精确可预期、caller 配额隔离 |
| `tests/test_singleflight.py` | 并发合并只执行一次、跟随者取消不影响他人 |
| `tests/test_concurrency.py` | 有界队列快速拒绝、排队取消离队 |
| `tests/test_quota.py` | 独立限额、固定窗滚动、退还 |
| `tests/test_api.py` | HTTP 状态码映射、旧字段兼容、回退响应说明实际后端 |
| 其余 `tests/test_*.py` | 幂等账本、缓存 TTL、配置校验、模拟后端 |

单测日志为单行 JSON，`event` 字段覆盖：`request_received`（含输入）、
`version_selected`（含选择依据）、`backend_attempt/backend_timeout/backend_error`、
`rejected/cache_hit/coalesced/idempotent_replay/request_ok`（最终判定）。

## 7. 代码结构

```
gateway/
  errors.py         错误分类（code + HTTP 状态）
  pipeline.py       FNV-1a 确定性灰度桶
  models.py         领域值对象
  config_loader.py  JSON 配置装配与加载期校验
  router.py         别名->版本（分组/权重）
  backend.py        确定性模拟后端与注册表
  quota.py          按 caller 固定窗配额
  concurrency.py    并发槽 + 有界队列
  singleflight.py   并发请求合并
  idempotency.py    request_id 账本（唯一执行/重放）
  cache.py          成功结果 TTL 缓存
  service.py        编排：路由->幂等->配额->限流->合并->回退
  telemetry.py      JSON 日志与内存指标
  api.py            FastAPI 装配
```
