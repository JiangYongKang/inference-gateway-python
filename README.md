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
`tests/test_quota.py`（独立配额、窗口滚动）、`tests/test_api.py`（HTTP 错误映射）。

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

错误体统一为 `{"error": {"code", "message", "detail"}}`。

## 兼容范围

- 响应字段只做**新增**，既有字段（`request_id/model/version/backend/output`）
  名称与语义不变，旧客户端可忽略 `cached/attempts/fallback_from`。
- 原有 `GET /` 路由保持不变。
- 模拟后端对相同输入产生相同输出，客户端可安全重试；相同 `request_id` 的
  重试在配置不变时得到相同版本与输出。

## 代码结构

```
gateway/
  errors.py       错误码体系
  models.py       请求/响应模型
  registry.py     别名 -> 版本路由（分组/灰度粘性哈希）
  backends.py     确定性模拟后端 + 脚本化故障注入
  concurrency.py  有界并发/队列 + 在途请求合并
  quotas.py       按调用方的窗口配额（时钟可注入）
  gateway.py      编排：配额 -> 路由 -> 并发 -> 后端/回退
  config.py       本地演示装配
main.py           FastAPI 入口（create_app 工厂便于测试）
```
