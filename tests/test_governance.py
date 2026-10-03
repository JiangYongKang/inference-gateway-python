"""重放与结果复用治理单测。

覆盖：并发同标识只执行一次、配置变化仍复用原结论、同标识不同内容冲突、
调用方标识隔离、跨请求复用命中、版本变化不跨版本复用、失败/取消/超时不进入复用、
过期与容量淘汰、容量打满快速拒绝；日志含输入/派发方式/版本后端/最终判定。
"""

import asyncio
import logging

import pytest

from gateway.backends import DeterministicBackend
from gateway.errors import (
    AllBackendsFailedError,
    IdempotencyConflictError,
    InvalidGovernanceConfigError,
    QuotaExceededError,
    RecordCapacityError,
)
from gateway.governance import GovernanceConfig
from gateway.models import InferRequest

log = logging.getLogger("gateway.test")


def req(prompt="p", caller="c1", request_id=None, alias="m", timeout_ms=None):
    return InferRequest(model=alias, caller_id=caller, prompt=prompt,
                        request_id=request_id, timeout_ms=timeout_ms)


def gov(**kw):
    base = dict(replay_ttl_seconds=100.0, replay_max_per_caller=4,
                reuse_ttl_seconds=100.0, reuse_max_per_caller=4)
    base.update(kw)
    return GovernanceConfig(**base)


VERSIONS_V1 = [("m", "v1", ["b"], 1)]


class TestReplay:
    async def test_concurrent_same_id_executes_once(self, make_gateway):
        backend = DeterministicBackend("b", latency_ms=50)
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            limits={"c1": 1000}, governance=gov(),
        )
        results = await asyncio.gather(*[
            gw.infer(req(request_id="idem-1")) for _ in range(6)
        ])
        log.info(
            "input=6x(c1,idem-1) dispatch=replay version=%s backend=%s calls=%d "
            "judgement=single-execution",
            results[0].version, results[0].backend, backend.calls,
        )
        assert backend.calls == 1                          # 只真正执行一次
        assert results[0].replayed is False
        assert [r.replayed for r in results[1:]] == [True] * 5
        assert all(r.output == results[0].output for r in results)
        assert all(r.version == "v1" and r.backend == "b" for r in results)
        # follower 不扣额度：仅 leader 计一次
        assert gw._quota.usage("c1") == 1
        assert gw.stats.backend_executions == 1
        assert gw.stats.replay_hits == 5

    async def test_same_id_after_success_replays_even_with_quota_exhausted(self, make_gateway):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            limits={"c1": 1}, governance=gov(),
        )
        first = await gw.infer(req(request_id="k"))
        assert first.replayed is False
        # 额度已用尽（leader 已扣 1）；重放不再扣额度，仍然成功
        for _ in range(5):
            r = await gw.infer(req(request_id="k"))
            assert r.replayed is True
            assert r.output == first.output
        log.info("input=6x(c1,k) quota=1 dispatch=replay judgement=served-without-quota")
        assert backend.calls == 1
        assert gw.stats.backend_executions == 1

    async def test_replay_keeps_historical_conclusion_after_route_change(self, make_gateway):
        """成功后即使路由/分组变化，保留期内同标识必须返回当时的版本/后端/输出。"""
        b1 = DeterministicBackend("b1")
        b2 = DeterministicBackend("b2")
        gw = make_gateway(
            versions=[
                ("m", "v1", ["b1"], 100),
                ("m", "v2", ["b2"], 0),
            ],
            backends={"b1": b1, "b2": b2},
            governance=gov(),
        )
        first = await gw.infer(req(request_id="route-shift"))
        assert (first.version, first.backend) == ("v1", "b1")
        # 灰度翻转 + 分组改配：该调用方现在会被路由到 v2
        gw._registry.register_version("m", "v1", ["b1"], weight=0)
        gw._registry.register_version("m", "v2", ["b2"], weight=100)
        gw._registry.set_caller_group("m", "c1", "v2")
        replayed = await gw.infer(req(request_id="route-shift"))
        log.info(
            "input=(c1,route-shift) after=canary+group-flip dispatch=replay "
            "version=%s backend=%s judgement=historical-conclusion-preserved",
            replayed.version, replayed.backend,
        )
        assert replayed.replayed is True
        assert (replayed.version, replayed.backend, replayed.output) == (
            "v1", "b1", first.output,
        )
        assert b2.calls == 0  # 绝不拿新版本结果冒充旧结论

    async def test_same_id_different_content_conflicts_and_keeps_old(self, make_gateway):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            limits={"c1": 10}, governance=gov(),
        )
        first = await gw.infer(req(prompt="orig", request_id="k"))
        # 同标识 + 不同请求内容
        with pytest.raises(IdempotencyConflictError) as e1:
            await gw.infer(req(prompt="different", request_id="k"))
        assert e1.value.code == "idempotency_conflict"
        assert e1.value.http_status == 409
        # 同标识 + 不同模型别名
        with pytest.raises(IdempotencyConflictError) as e2:
            await gw.infer(req(prompt="orig", alias="other-model", request_id="k"))
        assert e2.value.code == "idempotency_conflict"
        log.info(
            "input=(c1,k) variants=prompt/model dispatch=replay judgement=%s,%s",
            e1.value.code, e2.value.code,
        )
        # 旧结论未被覆盖；冲突不扣额度、不触发后端
        again = await gw.infer(req(prompt="orig", request_id="k"))
        assert again.replayed is True and again.output == first.output
        assert backend.calls == 1
        assert gw._quota.usage("c1") == 1
        assert gw._gov_counters.replay_conflicts == 2

    async def test_request_id_is_isolated_per_caller(self, make_gateway):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend}, governance=gov(),
        )
        r1 = await gw.infer(req(caller="alice", request_id="same-key"))
        r2 = await gw.infer(req(caller="bob", request_id="same-key"))
        log.info("input=(alice,same-key)/(bob,same-key) dispatch=execute,execute "
                 "judgement=isolated")
        assert r1.replayed is False and r2.replayed is False
        assert backend.calls == 2
        # bob 不能读到 alice 的结论；各自重放自己的
        assert (await gw.infer(req(caller="alice", request_id="same-key"))).replayed is True
        assert (await gw.infer(req(caller="bob", request_id="same-key"))).replayed is True
        assert gw.replay_store.record_count("alice") == 1
        assert gw.replay_store.record_count("bob") == 1

    async def test_replay_preserves_attempts_and_fallback_chain(self, make_gateway):
        primary = DeterministicBackend("primary", fault_plan=["error"])
        backup = DeterministicBackend("backup")
        gw = make_gateway(
            versions=[("m", "v1", ["primary", "backup"], 1)],
            backends={"primary": primary, "backup": backup}, governance=gov(),
        )
        first = await gw.infer(req(request_id="fb"))
        assert (first.backend, first.fallback_from, first.attempts) == (
            "backup", ["primary"], 2,
        )
        again = await gw.infer(req(request_id="fb"))
        log.info("input=(c1,fb) dispatch=replay version=%s backend=%s fallback=%s "
                 "judgement=historical-attempts-preserved",
                 again.version, again.backend, again.fallback_from)
        assert again.replayed is True
        assert (again.backend, again.fallback_from, again.attempts) == (
            "backup", ["primary"], 2,
        )
        assert primary.calls == 1 and backup.calls == 1

    async def test_failed_first_attempt_does_not_occupy_id(self, make_gateway):
        backend = DeterministicBackend("b", fault_plan=["error", "ok"])
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            limits={"c1": 10}, governance=gov(),
        )
        with pytest.raises(AllBackendsFailedError):
            await gw.infer(req(request_id="retry-me"))
        assert gw.replay_store.record_count("c1") == 0
        assert gw.replay_store.inflight_count("c1") == 0
        second = await gw.infer(req(request_id="retry-me"))
        log.info("input=(c1,retry-me) first=failure dispatch=retry version=%s "
                 "judgement=id-released-rerouted-executed", second.version)
        assert second.replayed is False and second.output
        assert backend.calls == 2

    async def test_follower_failure_propagates_and_releases_id(self, make_gateway):
        backend = DeterministicBackend("b", fault_plan=["error"])
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            max_concurrent=4, max_queue=8, governance=gov(),
        )
        backend.latency_ms = 30
        backend._fault_plan = ["error"]  # 失败发生在等待后
        leader = asyncio.ensure_future(gw.infer(req(request_id="boom")))
        follower_task = asyncio.ensure_future(gw.infer(req(request_id="boom")))
        results = await asyncio.gather(leader, follower_task, return_exceptions=True)
        assert all(isinstance(r, AllBackendsFailedError) for r in results)
        log.info("input=2x(c1,boom) leader=error judgement=both-failed-id-released")
        assert gw.replay_store.inflight_count("c1") == 0
        assert gw.replay_store.record_count("c1") == 0

    async def test_replay_expires_outside_retention(self, make_gateway, clock):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            governance=gov(replay_ttl_seconds=10.0, reuse_ttl_seconds=1.0),
        )
        first = await gw.infer(req(request_id="ttl"))
        clock.advance(11)
        again = await gw.infer(req(request_id="ttl"))
        log.info("input=(c1,ttl) after=11s dispatch=execute judgement=expired-rerun")
        assert again.replayed is False
        assert backend.calls == 2 and first.output == again.output
        assert gw._gov_counters.replay_expired >= 1

    async def test_replay_capacity_full_fast_rejects_per_caller(self, make_gateway):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            governance=gov(replay_max_per_caller=2),
        )
        await gw.infer(req(request_id="k1"))
        await gw.infer(req(request_id="k2"))
        with pytest.raises(RecordCapacityError) as ei:
            await gw.infer(req(request_id="k3"))
        log.info("input=(c1,k3) capacity=2 dispatch=register judgement=%s", ei.value.code)
        assert ei.value.code == "record_capacity_exceeded"
        assert ei.value.http_status == 507
        assert ei.value.detail["scope"] == "replay"
        assert gw._quota.usage("c1") == 2  # 容量拒绝发生在扣额度之前
        # 容量按调用方隔离：其他调用方不受影响
        other = await gw.infer(req(caller="c2", request_id="k1"))
        assert other.replayed is False and other.output
        # 在保留期内的承诺没有被挤掉
        assert (await gw.infer(req(request_id="k1"))).replayed is True


class TestReuse:
    async def test_cross_request_reuse_hit(self, make_gateway):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            limits={"c1": 100}, governance=gov(),
        )
        first = await gw.infer(req(prompt="share-me"))
        assert first.reuse_hit is False
        second = await gw.infer(req(prompt="share-me"))
        log.info(
            "input=2x(c1,share-me,no-id) dispatch=execute,reuse version=%s backend=%s "
            "reused_from=%s judgement=conclusion-reused",
            second.version, second.backend, second.reused_from,
        )
        assert second.reuse_hit is True and second.replayed is False
        assert second.reused_from == first.request_id
        assert (second.version, second.backend, second.output) == (
            first.version, first.backend, first.output,
        )
        assert second.fallback_from == first.fallback_from
        assert backend.calls == 1                      # 未重新执行后端
        assert gw.stats.backend_executions == 1
        assert gw.stats.reuse_hits == 1
        # 复用仍按正常准入计一次额度
        assert gw._quota.usage("c1") == 2

    async def test_reuse_hit_denied_by_quota(self, make_gateway):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            limits={"c1": 1}, governance=gov(),
        )
        await gw.infer(req(prompt="share-me"))
        with pytest.raises(QuotaExceededError) as ei:
            await gw.infer(req(prompt="share-me"))
        log.info("input=reuse-with-exhausted-quota judgement=%s", ei.value.code)
        assert ei.value.code == "quota_exceeded"  # 复用也要先过配额

    async def test_version_change_does_not_reuse_across_versions(self, make_gateway):
        b1 = DeterministicBackend("b1")
        b2 = DeterministicBackend("b2")
        gw = make_gateway(
            versions=[
                ("m", "v1", ["b1"], 100),
                ("m", "v2", ["b2"], 0),
            ],
            backends={"b1": b1, "b2": b2},
            governance=gov(),
        )
        v1 = await gw.infer(req(prompt="p"))
        assert v1.version == "v1"
        # 灰度翻转：新请求必须按新路由得到 v2 结论，不能拿 v1 结果冒充
        gw._registry.register_version("m", "v1", ["b1"], weight=0)
        gw._registry.register_version("m", "v2", ["b2"], weight=100)
        v2 = await gw.infer(req(prompt="p"))
        log.info("input=(c1,p) after=canary-flip dispatch=execute version=%s backend=%s "
                 "judgement=rerouted-no-cross-version-reuse", v2.version, v2.backend)
        assert (v2.version, v2.backend) == ("v2", "b2")
        assert v2.reuse_hit is False and v1.output != v2.output
        assert b1.calls == 1 and b2.calls == 1

    async def test_reuse_isolated_by_model_and_caller(self, make_gateway):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=[
                ("m", "v1", ["b"], 1),
                ("n", "v1", ["b"], 1),
            ],
            backends={"b": backend}, governance=gov(),
        )
        await gw.infer(req(alias="m", prompt="p"))
        # 不同模型别名不串用
        assert (await gw.infer(req(alias="n", prompt="p"))).reuse_hit is False
        # 不同调用方不串用
        assert (await gw.infer(req(caller="c2", prompt="p"))).reuse_hit is False
        # 同一调用方同一模型才命中
        assert (await gw.infer(req(prompt="p"))).reuse_hit is True
        log.info("input=model/caller variants judgement=isolated,hit")
        assert backend.calls == 3

    async def test_failure_cancel_timeout_not_reused(self, make_gateway, clock):
        # 1) 后端失败不进复用：同内容下一次必须重新执行
        bad = DeterministicBackend("bad", fault_plan=["error", "ok"])
        gw = make_gateway(
            versions=[("m", "v1", ["bad"], 1)], backends={"bad": bad},
            governance=gov(),
        )
        with pytest.raises(AllBackendsFailedError):
            await gw.infer(req(prompt="f"))
        assert gw.reuse_store.record_count("c1") == 0
        r = await gw.infer(req(prompt="f"))
        assert r.reuse_hit is False and bad.calls == 2
        log.info("input=failure-then-retry judgement=rerun-no-reuse")

        # 2) 超时后不进复用
        slow = DeterministicBackend("slow", fault_plan=["timeout", "ok"])
        gw2 = make_gateway(
            versions=[("m", "v1", ["slow"], 1)], backends={"slow": slow},
            governance=gov(),
        )
        with pytest.raises(AllBackendsFailedError):
            await gw2.infer(req(prompt="t", timeout_ms=20))
        assert gw2.reuse_store.record_count("c1") == 0
        rt = await gw2.infer(req(prompt="t", timeout_ms=20))
        assert rt.reuse_hit is False and slow.calls == 2
        log.info("input=timeout-then-retry judgement=rerun-no-reuse")

        # 3) 取消不进复用
        hang = DeterministicBackend("hang", latency_ms=300)
        gw3 = make_gateway(
            versions=[("m", "v1", ["hang"], 1)], backends={"hang": hang},
            max_concurrent=2, max_queue=4, governance=gov(),
        )
        t = asyncio.ensure_future(gw3.infer(req(prompt="c")))
        await asyncio.sleep(0.02)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert gw3.reuse_store.record_count("c1") == 0
        assert gw3.stats.cancelled == 1
        log.info("input=cancelled judgement=no-reuse-quota-refunded")
        assert gw3._quota.usage("c1") == 0

    async def test_reuse_expiry_treated_as_miss(self, make_gateway, clock):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            governance=gov(reuse_ttl_seconds=10.0),
        )
        await gw.infer(req(prompt="e"))
        clock.advance(11)
        r = await gw.infer(req(prompt="e"))
        log.info("input=(c1,e) after=11s dispatch=execute judgement=expired-miss")
        assert r.reuse_hit is False and backend.calls == 2
        assert gw._gov_counters.reuse_expired >= 1

    async def test_reuse_capacity_evicts_oldest_deterministically(self, make_gateway):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=VERSIONS_V1, backends={"b": backend},
            governance=gov(reuse_max_per_caller=2),
        )
        await gw.infer(req(prompt="p1"))
        await gw.infer(req(prompt="p2"))
        # p3 写入时容量满，按 FIFO 淘汰最旧的 p1
        await gw.infer(req(prompt="p3"))
        assert gw.reuse_store.record_count("c1") == 2
        assert gw._gov_counters.reuse_evicted == 1
        miss = await gw.infer(req(prompt="p1"))
        hit = await gw.infer(req(prompt="p3"))
        log.info("input=p1(miss),p3(hit) after=fifo-evict judgement=rerun,reused")
        assert miss.reuse_hit is False
        assert hit.reuse_hit is True

    async def test_purge_only_affects_later_requests(self, make_gateway):
        backend = DeterministicBackend("b")
        gw = make_gateway(
            versions=[
                ("m", "v1", ["b"], 1),
                ("m", "v2", ["b"], 1),
            ],
            backends={"b": backend}, governance=gov(),
        )
        # v1 复用记录
        gw._registry.register_version("m", "v1", ["b"], weight=100)
        gw._registry.register_version("m", "v2", ["b"], weight=0)
        await gw.infer(req(prompt="p"))
        # 定向清理 v1
        removed = await gw.purge_reuse(version="v1")
        log.info("input=purge(version=v1) removed=%d judgement=only-future-affected", removed)
        assert removed == 1 and gw.reuse_store.record_count("c1") == 0
        assert (await gw.infer(req(prompt="p"))).reuse_hit is False  # 之后新请求重新执行
        # 重放承诺不受 purge 影响
        await gw.infer(req(prompt="promised", request_id="keep"))
        await gw.purge_reuse(model="m")
        kept = await gw.infer(req(prompt="promised", request_id="keep"))
        assert kept.replayed is True and kept.version == "v1"


class TestGovernanceConfig:
    @pytest.mark.parametrize("kwargs", [
        {"replay_ttl_seconds": 0},
        {"replay_ttl_seconds": -1.0},
        {"replay_ttl_seconds": float("inf")},
        {"reuse_ttl_seconds": float("nan")},
        {"replay_max_per_caller": 0},
        {"reuse_max_per_caller": -3},
        {"replay_max_per_caller": True},
    ])
    def test_invalid_config_rejected_at_load(self, kwargs):
        with pytest.raises(InvalidGovernanceConfigError) as ei:
            GovernanceConfig(**kwargs)
        log.info("input=bad-config %s judgement=%s", kwargs, ei.value.code)
        assert ei.value.code == "invalid_governance_config"

    def test_set_retention_validates(self, make_gateway):
        gw = make_gateway(versions=VERSIONS_V1, backends={"b": DeterministicBackend("b")},
                          governance=gov())
        with pytest.raises(InvalidGovernanceConfigError):
            gw.reuse_store.set_retention(0)


class TestGovernanceApi:
    """HTTP 层：新增错误码映射、响应新增字段、stats 快照与 admin 运维端点。"""

    @pytest.fixture
    def client(self, make_gateway):
        from fastapi.testclient import TestClient
        from main import create_app

        gw = make_gateway(
            versions=[("m", "v1", ["b"], 1)],
            backends={"b": DeterministicBackend("b")},
            limits={"c1": 100},
            governance=gov(),
        )
        c = TestClient(create_app(gw))
        c.gateway = gw
        return c

    def test_replay_response_markers_and_no_duplicate_call(self, client):
        payload = {"model": "m", "prompt": "hi", "caller_id": "c1", "request_id": "h1"}
        r1 = client.post("/v1/infer", json=payload)
        assert r1.status_code == 200
        b1 = r1.json()
        assert b1["replayed"] is False and b1["reuse_hit"] is False
        # 旧字段语义不变
        assert set(b1) >= {"request_id", "model", "version", "backend", "output",
                           "cached", "attempts", "fallback_from"}
        r2 = client.post("/v1/infer", json=payload)
        b2 = r2.json()
        log.info("input=/v1/infer h1#2 judgement=%s", b2)
        assert r2.status_code == 200
        assert b2["replayed"] is True
        assert (b2["version"], b2["backend"], b2["output"]) == (
            b1["version"], b1["backend"], b1["output"],
        )

    def test_conflict_409_distinct_from_quota_and_backend(self, client):
        client.post("/v1/infer", json={
            "model": "m", "prompt": "a", "caller_id": "c1", "request_id": "dup"})
        r = client.post("/v1/infer", json={
            "model": "m", "prompt": "b", "caller_id": "c1", "request_id": "dup"})
        log.info("input=conflict judgement=%s", r.json())
        assert r.status_code == 409
        assert r.json()["error"]["code"] == "idempotency_conflict"
        assert r.json()["error"]["detail"]["phase"] == "committed"

    def test_stats_expose_governance_counters(self, client):
        client.post("/v1/infer", json={
            "model": "m", "prompt": "s", "caller_id": "c1", "request_id": "g1"})
        client.post("/v1/infer", json={
            "model": "m", "prompt": "s", "caller_id": "c1", "request_id": "g1"})  # replay
        client.post("/v1/infer", json={"model": "m", "prompt": "s2", "caller_id": "c1"})
        client.post("/v1/infer", json={"model": "m", "prompt": "s2", "caller_id": "c1"})  # reuse
        r = client.get("/v1/stats")
        body = r.json()
        log.info("input=/v1/stats judgement=%s", body)
        assert body["backend_executions"] == 2
        assert body["replay_hits"] == 1
        assert body["reuse_hits"] == 1
        g = body["governance"]
        assert g["replay_records"] >= 1 and g["reuse_records"] >= 1
        assert g["per_caller"]["replay_records"]["c1"] >= 1
        assert g["counters"]["replay_hits"] == 1
        assert g["counters"]["reuse_hits"] == 1

    def test_admin_purge_endpoint(self, client):
        client.post("/v1/infer", json={"model": "m", "prompt": "x", "caller_id": "c1"})
        r = client.post("/v1/admin/governance/purge", json={"model": "m", "version": "v1"})
        assert r.status_code == 200
        assert r.json()["purged_reuse_records"] == 1
        # 清理后新请求重新执行
        again = client.post("/v1/infer", json={"model": "m", "prompt": "x", "caller_id": "c1"})
        assert again.json()["reuse_hit"] is False

    def test_admin_retention_endpoint_validates(self, client):
        ok = client.post("/v1/admin/governance/retention", json={"reuse_ttl_seconds": 5})
        assert ok.status_code == 200 and ok.json()["reuse_ttl_seconds"] == 5
        bad = client.post("/v1/admin/governance/retention", json={"reuse_ttl_seconds": -1})
        assert bad.status_code == 422  # pydantic 入参校验
