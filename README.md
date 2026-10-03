# inference-gateway-python

可本地验证的模型推理网关。真实模型由**确定性本地模拟后端**替代：输出仅取决于
`(backend, version, prompt)` 的 SHA-256，无随机性，因此版本切换、并发压力、
故障回退、重放与复用下的结论完全可预期、可复现。

## 运行

```bash
uv sync
uv run uvicorn main:app --reload
# 另开终端：
curl -s -X POST localhost:8000/v1/infer \
  -H 'content-type: application/json' \
  -d '{"model":"echo","prompt":"hello","caller_id":"internal","request_id":"r1"}' | jq
# 同一 request_id 重放：返回首次结论并标记 replayed=true，不扣配额、不调后端
curl -s -X POST localhost:8000/v1/infer \
  -H 'content-type: application/json' \
  -d '{"model":"echo","prompt":"hello","caller_id":"internal","request_id":"r1"}' | jq
# 状态查询：真实执行 / 复用命中 / 重放命中 / 冲突 / 过期 / 淘汰 / 当前记录数
curl -s localhost:8000/v1/stats | jq
# 定向清理复用记录（只影响之后的新请求）
curl -s -X POST 'localhost:8000/v1/admin/reuse/purge?model=echo&version=v1' | jq
```

## 本地验证（测试）

```bash
uv run pytest -q                          # 全部确定性单测
uv run pytest --log-cli-level=INFO -q     # 查看每个用例的 输入/选择依据/最终判定 日志
```

覆盖范围：`tests/test_routing.py`（别名解析、灰度粘性、版本切换、非法配置）、
`tests/test_concurrency.py`（并发竞争、合并、快速拒绝、取消释放）、
`tests/test_fallback.py`（超时/异常回退、不重复计费）、
`tests/test_quota.py`（独立配额、窗口滚动）、
`tests/test_replay.py`（并发同标识只执行一次、配置变化仍复用原结论、冲突、
调用方隔离、首次失败不占标识、过期、容量打满快速拒绝、非法配置加载拒绝）、
`tests/test_reuse.py`（跨请求复用命中、版本变化不跨版本复用、失败/取消/超时不入
复用、过期与 LRU 淘汰、复用仍走配额准入、定向清理与缩短保留期、统计口径）、
`tests/test_api.py`（HTTP 错误映射、响应兼容、状态查询与清理端点）。

## 路由规则

`POST /v1/infer` 请求体：`{model, prompt, caller_id, request_id?, timeout_ms?}`。

按优先级解析 `model` 别名到具体版本：

1. **调用方分组**：`caller_id` 被分组配置固定到某版本时直接使用（`reason=caller_group`）。
2. **灰度权重**：按 `sha256(alias|caller_id) % 总权重` 分桶选择版本
   （`reason=canary_weight`）。哈希对调用方**粘性**：配置不变时同一 `caller_id`
   永远落到同一版本；版本切换（调整权重/增删版本）期间，未越界的调用方桶位不变，
   同一调用标识不会得到互相矛盾的结果序列。
3. **单版本直通**：仅剩一个可用版本时直接使用（`reason=single_version`）。

拒绝场景（原因可区分，见错误码表）：别名未注册、版本无任何后端、灰度权重总和 ≤ 0、
分组指向已删除版本、权重为负、版本未声明后端。

## 重放（调用标识幂等）

同一 `caller_id` 携带同一 `request_id` 的请求：

- **首次成功后**，在 `idempotency_ttl_seconds` 保留期内，无论重试多少次、是否并发
  到达、期间路由或分组怎么调整，都返回**同一份结论**（当时实际生效的
  `version/backend/output/attempts/fallback_from`），并标记 `replayed=true`。
- **并发同标识只真正执行一次**：先到者为 leader 执行，其余等待其结论后按重放返回；
  leader 失败则释放标识，等待者可按当时路由重新认领执行。
- **重放零副作用**：不扣配额、不计入后端成功、不触发后端调用。
- **按调用方隔离**：`(caller_id, request_id)` 为键，另一个调用方拿相同标识互不可见。
- **冲突拒绝**：同一调用方拿同一标识配**不同模型或不同 prompt**，返回
  `idempotency_conflict`（409），与配额不足（429 `quota_exceeded`）、后端异常
  （502）明确区分；冲突**不覆盖**已承诺的旧结论。
- **首次未成功不占标识**：失败/取消/超时的请求不留下记录，之后重试按当时路由重新执行。
- **保留期外不再承诺一致性**：过期记录按不存在处理，同标识可重新真实执行。

## 结果复用

已成功且允许复用的结论，在 `reuse_ttl_seconds` 窗口内可服务给**同一调用方、同一
模型别名、同一实际版本、同一 prompt** 的新调用（`request_id` 可不同）：

- 命中时保留最初结论的实际版本、后端与回退信息，标记 `reused=true`，
  本次不重新执行后端。
- 复用命中**仍按调用方现有配额规则正常准入**（配额不足照样 429），但不计入
  后端成功：`stats.success` 只统计真实后端执行，复用计入
  `retention.counters.reuse_hits`。
- 失败、取消、超时**不得进入复用**；不同调用方、不同模型别名、不同实际版本之间
  不能串用结果。
- 灰度或分组变化后，新请求按新路由解析到新版本，复用键中的版本不同自然未命中，
  不会拿旧版本结果冒充新版本。

## 保留与容量策略

两类记录均**按调用方隔离**（单调用方打满不影响他人），由 `RetentionConfig`
配置，非法值（非正 TTL、容量 < 1）在加载阶段直接抛 `invalid_retention_config`：

| 配置 | 含义 |
|---|---|
| `idempotency_ttl_seconds` | 调用标识结论的保留期（承诺有效期） |
| `idempotency_max_entries_per_caller` | 每调用方的标识记录上限 |
| `reuse_ttl_seconds` | 复用记录保留窗口 |
| `reuse_max_entries_per_caller` | 每调用方的复用记录上限 |

- **过期**：两类记录过期即删除并计数（`replay_expired`/`reuse_expired`），
  过期复用记录按未命中处理。
- **淘汰**：复用记录满时按确定顺序淘汰（最久未访问，键名字典序兜底），计
  `reuse_evicted`；**保留期内已承诺的调用标识结论不参与淘汰**。
- **容量打满快速拒绝**：标识记录满且均在保留期内时，新标识立即以
  `idempotency_store_full`（429）拒绝，发生在配额准入之前，与
  `quota_exceeded`、后端失败可区分。
- **定向治理**：`POST /v1/admin/reuse/purge?model=..&version=..` 清理复用记录，
  `ttl_seconds` 参数缩短复用保留期。清理只影响之后的新请求，不改写保留期内
  已承诺的调用标识结论，也不影响正在执行的请求（在途记录不在复用结构中）。

## 状态查询

`GET /v1/stats` 在原有字段（`success/failed/rejected/cancelled/per_backend`）
之上新增 `retention` 键：

```json
{
  "success": 1,
  "retention": {
    "counters": {"replay_hits": 1, "reuse_hits": 0, "conflict_rejected": 0,
                 "store_full_rejected": 0, "replay_expired": 0,
                 "reuse_expired": 0, "reuse_evicted": 0},
    "records": {"idempotency": {"c1": 1}, "reuse": {"c1": 1}},
    "policy": {"idempotency_ttl_seconds": 300.0, "...": "..."}
  }
}
```

`success` 只计真实后端执行；复用命中看 `reuse_hits`，重放命中看 `replay_hits`。

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
- 请求被取消（含客户端超时）后：释放并发槽位、退回配额、不计入成功/失败统计，
  也不进入复用、不占用调用标识。

## 配额语义

- 按 `caller_id` 独立计数，固定窗口（默认 60s）限额；未配置的调用方适用
  `default_limit`，两者皆无则不限。
- 准入时计费一次；请求被取消/超时退回配额；重试与回退不重复计费。
- **重放命中不扣配额**（在配额准入之前返回）；**复用命中正常扣配额**。
- 超额返回 `quota_exceeded`（429），与后端错误（502/504）、容量拒绝明确区分。

## 错误码

| code | HTTP | 含义 |
|---|---|---|
| `unknown_model` | 404 | 模型别名未注册 |
| `invalid_route_config` | 500 | 灰度/分组配置非法 |
| `invalid_retention_config` | 500 | 保留/容量配置非法（加载阶段拒绝） |
| `no_available_backend` | 503 | 版本无可用后端 |
| `quota_exceeded` | 429 | 调用方配额超限 |
| `queue_full` | 429 | 并发队列已满 |
| `idempotency_store_full` | 429 | 调用标识记录容量满且均在保留期内 |
| `idempotency_conflict` | 409 | 同标识复用于不同模型/内容 |
| `backend_timeout` | 504 | 后端超时（回退后仍失败时体现在 detail） |
| `backend_error` | 502 | 后端执行异常（同上） |
| `all_backends_failed` | 502 | 回退链全部失败 |

错误体统一为 `{"error": {"code", "message", "detail"}}`。

## 兼容范围

- 响应字段只做**新增**：本轮新增 `replayed`、`reused`；既有字段
  （`request_id/model/version/backend/output/cached/attempts/fallback_from`）
  名称与语义不变，旧客户端可忽略新字段。
- `/v1/stats` 只做新增键（`retention`），原有键不动。
- 原有 `GET /` 路由保持不变。
- 模拟后端对相同输入产生相同输出，客户端可安全重试。

## 代码结构

```
gateway/
  errors.py       错误码体系
  models.py       请求/响应模型
  registry.py     别名 -> 版本路由（分组/灰度粘性哈希）
  backends.py     确定性模拟后端 + 脚本化故障注入
  concurrency.py  有界并发/队列 + 在途请求合并
  quotas.py       按调用方的窗口配额（时钟可注入）
  retention.py    重放（调用标识幂等）+ 结果复用：保留期、容量、淘汰、统计
  gateway.py      编排：重放/复用 -> 配额 -> 路由 -> 并发 -> 后端/回退
  config.py       本地演示装配
main.py           FastAPI 入口（create_app 工厂便于测试）
```
