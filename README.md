# inference-gateway-python

可本地验证的模型推理网关。真实模型由**确定性本地模拟后端**替代：输出仅取决于
`(backend, version, prompt)` 的 SHA-256，无随机性，因此版本切换、并发压力与
故障回退下的结论完全可预期、可复现。

## 运行

```bash
uv sync
uv run uvicorn main:app --reload
# 另开终端：
curl -s -X POST localhost:8000/v1/infer \
  -H 'content-type: application/json' \
  -d '{"model":"echo","prompt":"hello","caller_id":"internal","request_id":"r1"}' | jq
```

## 本地验证（测试）

```bash
uv run pytest -q                          # 全部确定性单测
uv run pytest --log-cli-level=INFO -q     # 查看每个用例的 输入/选择依据/最终判定 日志
```

覆盖范围：`tests/test_routing.py`（别名解析、灰度粘性、版本切换、非法配置）、
`tests/test_concurrency.py`（并发竞争、合并、快速拒绝、取消释放）、
`tests/test_fallback.py`（超时/异常回退、不重复计费）、
`tests/test_quota.py`（独立配额、窗口滚动）、`tests/test_api.py`（HTTP 错误映射）、
`tests/test_governance.py`（重放承诺、跨请求复用、冲突/容量/过期/淘汰、治理运维端点）。

## 路由规则

`POST /v1/infer` 请求体：`{model, prompt, caller_id, request_id?, timeout_ms?}`。

按优先级解析 `model` 别名到具体版本：

1. **调用方分组**：`caller_id` 被分组配置固定到某版本时直接使用（`reason=caller_group`）。
2. **灰度权重**：按 `sha256(alias|caller_id) % 总权重` 分桶选择版本
   （`reason=canary_weight`）。哈希对调用方**粘性**：配置不变时同一 `caller_id`
   永远落到同一版本；版本切换（调整权重/增删版本）期间，未越界的调用方桶位不变，
   同一调用标识不会得到互相矛盾的结果序列。
3. **单版本直通**：仅剩一个可用版本时直接使用（`reason=single_version`）。

拒绝场景（原因可区分，见下表）：别名未注册、版本无任何后端、灰度权重总和 ≤ 0、
分组指向已删除版本、权重为负、版本未声明后端。

## 回退规则

每个版本声明按序排列的后端列表。当前后端**超时**（`timeout_ms` 触发）或
**执行异常**时，按声明次序回退到下一个后端；全部失败返回
`all_backends_failed`（若所有后端均未注册则为 `no_available_backend`）。

- 重试与回退发生在**同一次准入**内：配额只计一次，成功统计只计一次，无重复副作用。
- 响应中 `backend`/`version` 为**实际生效**的后端与版本，`fallback_from` 列出
  被跳过的后端，`attempts` 为尝试次数，`cached=true` 表示共享了在途相同请求的结果。

## 并发与合并

- 有界并发 + 有界队列：槽位满则排队，队列也满时**立即**以 `queue_full` 拒绝，
  不无限阻塞、不静默丢弃。
- 合并键 `(version, backend, prompt, timeout_ms)` 相同的在途请求共享一次后端执行，
  follower 不占用槽位；follower 被取消不影响共享执行。
- 请求被取消（含客户端超时）后：释放并发槽位、退回配额、不计入成功/失败统计。

## 配额语义

- 按 `caller_id` 独立计数，固定窗口（默认 60s）限额；未配置的调用方适用
  `default_limit`，两者皆无则不限。
- 准入时计费一次；请求被取消/超时退回配额；重试与回退不重复计费。
- 超额返回 `quota_exceeded`（429），与后端错误（502/504）明确区分。

## 重放（调用标识结论承诺）

请求携带 `request_id` 时，该标识是**按调用方隔离**的幂等键：
`(caller_id, request_id)`。

- 第一次**成功**后，结论（实际版本、后端、输出、尝试与回退信息）被完整快照，
  在重放保留期 `replay_ttl_seconds` 内：无论重试多少次、是否并发到达、期间
  灰度权重/调用方分组如何调整，都返回**同一份历史结论**，响应 `replayed=true`。
- 同一标识的并发请求只有 leader **真正执行一次**，其余 follower 等待并共享
  同一份结论；follower 不扣额度、不占并发槽位、不重复计后端成功、不触发后端调用。
- 重放命中**不扣**该调用方额度；额度已用尽时重放仍可服务。
- 标识按调用方隔离：调用方 B 携带 A 的 `request_id` 读不到 A 的结论。
- 同一调用方、同一标识但**模型别名或请求内容（prompt/timeout_ms）不同**：
  返回 `idempotency_conflict`（409，detail.phase 为 `inflight`/`committed`），
  与 `quota_exceeded`（429）、后端失败（502/504）明确区分；冲突**不覆盖**旧结论。
- 第一次没有成功完成（后端全失败、取消、超时、解析错误）**不占用标识**，
  follower 收到与 leader 相同的失败；释放后重试按当时路由重新执行。
- 保留期外不再承诺一致性：过期记录按未命中惰性清除（计入 `replay_expired`），
  标识可被重新占用。

## 结果复用（跨请求结论复用）

不带调用标识的新调用，可复用同调用方此前的成功结论。

- 复用键作用域：`(caller_id, model, version, prompt, timeout_ms)`；不同调用方、
  不同模型别名、不同**实际版本**之间绝不串用。
- 命中条件：该调用方在复用保留窗 `reuse_ttl_seconds` 内，对同一模型别名、同一
  实际版本、同一内容有过成功结论。命中响应 `reuse_hit=true`、`reused_from=<来源
  request_id>`，并保留来源结论里的 `version/backend/fallback_from/attempts`，
  本次**不执行后端**（后端调用数与 `per_backend` 不增加）。
- 命中复用仍按该调用方现有配额规则**正常准入**（额度不足返回
  `quota_exceeded`）；统计上区分真实执行（`backend_executions`）与复用命中
  （`reuse_hits`），复用不算一次新的后端成功。
- **失败、取消、超时不进入复用**；灰度/分组变化后新请求按新路由解析版本，
  版本不同即未命中，必须得到新版本的真实结论，不会拿旧版本结果冒充新版本。

## 容量、过期、淘汰与定向治理

配置项（`GovernanceConfig`，全部有界；非法值在网关装配阶段直接抛
`invalid_governance_config`，服务拒绝启动）：

| 配置 | 含义 | 约束 |
|---|---|---|
| `replay_ttl_seconds` | 重放承诺保留秒数（只影响新提交） | 正数、有限 |
| `replay_max_per_caller` | 每调用方重放结论 + 在途预留上限 | 正整数 |
| `reuse_ttl_seconds` | 复用记录保留秒数（只影响新写入） | 正数、有限 |
| `reuse_max_per_caller` | 每调用方复用记录上限 | 正整数 |

- 两类容量都**按调用方隔离**：单个调用方打满不影响其他调用方。
- **过期**：惰性清除，按未命中处理，计入 `replay_expired`/`reuse_expired`。
- **复用区满**：先清过期，仍满则按插入顺序 **FIFO（最旧优先）确定淘汰**，
  计入 `reuse_evicted`；复用不是承诺，允许淘汰。
- **重放区满**：先回收过期；若仍满，**不淘汰**保留期内已承诺的结论，
  而是快速拒绝新请求 `record_capacity_exceeded`（507，detail.scope=`replay`），
  与额度不足、后端失败明确区分；该拒绝发生在扣额度之前。
- 定向治理：
  - `POST /v1/admin/governance/purge`（body: `model?`, `version?`）只清理
    **复用记录**，返回删除条数；清理只影响之后的新请求，**不改写**保留期内已
    承诺的重放结论，也不影响正在执行的请求。
  - `POST /v1/admin/governance/retention`（body: `replay_ttl_seconds?`,
    `reuse_ttl_seconds?`）缩短保留期，仅对之后新提交/新写入生效，不回溯。
- 状态查询 `GET /v1/stats` 在原有字段外新增 `backend_executions`、
  `replay_hits`、`reuse_hits`，以及 `governance` 段：冲突拒绝、容量拒绝、
  过期、淘汰计数、重放/复用当前记录数与在途数（含按调用方拆分）。

## 错误码

| code | HTTP | 含义 |
|---|---|---|
| `unknown_model` | 404 | 模型别名未注册 |
| `invalid_route_config` | 500 | 灰度/分组配置非法 |
| `no_available_backend` | 503 | 版本无可用后端 |
| `quota_exceeded` | 429 | 调用方配额超限 |
| `queue_full` | 429 | 并发队列已满 |
| `backend_timeout` | 504 | 后端超时（回退后仍失败时体现在 detail） |
| `backend_error` | 502 | 后端执行异常（同上） |
| `all_backends_failed` | 502 | 回退链全部失败 |
| `idempotency_conflict` | 409 | 同调用方同标识绑定了不同模型/请求内容 |
| `record_capacity_exceeded` | 507 | 该调用方重放记录区满且无过期项可清 |
| `invalid_governance_config` | 500 | 保留期/容量等治理配置非法（加载即拒绝） |

错误体统一为 `{"error": {"code", "message", "detail"}}`。

## 兼容范围

- 响应字段只做**新增**，既有字段（`request_id/model/version/backend/output`、
  `cached/attempts/fallback_from`）名称与语义不变；旧客户端可忽略新增的
  `replayed/reuse_hit/reused_from`。
- 统计只做**新增**：`backend_executions/replay_hits/reuse_hits/governance`，
  原 `success/failed/rejected/cancelled/per_backend` 含义不变
  （`per_backend` 仅在真正发起后端执行的 leader 上累加，在途合并 follower
  不计重复后端成功）。
- 原有 `GET /` 路由保持不变；`POST /v1/infer` 请求体不变，`request_id`
  由“可粘性重试标识”升级为重放承诺键（不带 `request_id` 的行为除新增复用外不变）。
- 模拟后端对相同输入产生相同输出；保留期外结论可能按当时路由重新执行。

## 代码结构

```
gateway/
  errors.py       错误码体系（含冲突 409 / 容量 507 / 治理配置非法）
  models.py       请求/响应模型（响应只新增 replayed/reuse_hit/reused_from）
  registry.py     别名 -> 版本路由（分组/灰度粘性哈希）
  backends.py     确定性模拟后端 + 脚本化故障注入
  concurrency.py  有界并发/队列 + 在途请求合并
  quotas.py       按调用方的窗口配额（时钟可注入）
  governance.py   重放承诺 / 跨请求复用：有界、按调用方隔离、过期与淘汰
  gateway.py      编排：重放 -> 配额 -> 路由 -> 复用 -> 在途合并 -> 后端/回退
  config.py       本地演示装配（含 GovernanceConfig）
main.py           FastAPI 入口（/v1/infer、/v1/stats、/v1/admin/governance/*）
```

重放的本地快速验证：

```bash
curl -s -X POST localhost:8000/v1/infer -H 'content-type: application/json' \
  -d '{"model":"echo","prompt":"hello","caller_id":"internal","request_id":"r1"}' | jq
# 再次发送完全相同的请求：replayed=true，version/backend/output 与首次一致
curl -s -X POST localhost:8000/v1/infer -H 'content-type: application/json' \
  -d '{"model":"echo","prompt":"hello","caller_id":"internal","request_id":"r1"}' | jq
# 同标识改 prompt：409 idempotency_conflict
curl -s -X POST localhost:8000/v1/infer -H 'content-type: application/json' \
  -d '{"model":"echo","prompt":"changed","caller_id":"internal","request_id":"r1"}' | jq
curl -s localhost:8000/v1/stats | jq
```
