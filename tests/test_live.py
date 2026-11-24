from dataclasses import replace
from datetime import datetime, timezone

import requests

from generator.live import post_sale, run
from generator.simulate import Config


class FakeResponse:
    def __init__(self, status):
        self.status_code = status


class FakeSession:
    """Answers with a scripted list of statuses (or exceptions), then 202 forever."""

    def __init__(self, script=()):
        self.script = list(script)
        self.posts = []

    def post(self, url, json, headers, timeout):
        self.posts.append((url, json, headers))
        step = self.script.pop(0) if self.script else 202
        if isinstance(step, Exception):
            raise step
        return FakeResponse(step)


NOW = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
NO_SLEEP = lambda _s: None  # noqa: E731


def test_retries_503_and_network_errors_with_the_same_sale():
    s = FakeSession([503, requests.ConnectionError("down"), 202])
    sale = {"receipt_id": "r-1"}
    assert post_sale(s, "http://g/v1/sales", "k", sale, sleep=NO_SLEEP) == 202
    assert len(s.posts) == 3 and all(p[1] is sale for p in s.posts), "a retry must resend the SAME receipt"


def test_does_not_retry_a_rejected_sale():
    s = FakeSession([400])
    assert post_sale(s, "http://g/v1/sales", "k", {}, sleep=NO_SLEEP) == 400
    assert len(s.posts) == 1


def test_gives_up_after_max_attempts():
    s = FakeSession([503] * 10)
    assert post_sale(s, "u", "k", {}, max_attempts=3, sleep=NO_SLEEP) == "gave_up"
    assert len(s.posts) == 3


def test_run_sends_auth_header_and_resends_accepted_receipts():
    s = FakeSession()
    cfg = replace(Config(), seed=5, retry_rate=0.5, bad_rate=0, late_rate=0)
    stats = run("http://g/", 1, "dk_k", rate=0, count=20, cfg=cfg, session=s, sleep=NO_SLEEP, now=lambda: NOW)
    assert stats[202] == 20
    assert all(p[0] == "http://g/v1/sales" and p[2]["Authorization"] == "Bearer dk_k" for p in s.posts)
    receipts = [p[1]["receipt_id"] for p in s.posts]
    assert len(receipts) > len(set(receipts)), "resends reuse receipt ids, to exercise idempotency"
