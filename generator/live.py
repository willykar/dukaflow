"""Live mode: act as a shop's tills and post sales to the till gateway in real time.

    python -m generator.live --gateway http://localhost:8080 --shop 1 --key dk_... --rate 5 --count 500

It behaves like a real till, including the awkward parts the pipeline must survive:
  * offline-till sales from hours ago syncing now (late_rate)
  * deliberately invalid sales, which the gateway must reject and dead-letter (bad_rate)
  * retries of an already-accepted sale with the SAME receipt_id (retry_rate), which must not double-count
  * on 503 or a network error it retries the same sale with backoff, as a real till should
"""

import argparse
import random
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime, timezone

import requests

from generator.simulate import Config, make_live_sale

RETRYABLE = {502, 503, 504}


def post_sale(session, url, key, sale, max_attempts=5, sleep=time.sleep):
    """POST one sale; retry only when the gateway says it is safe to (503) or the network failed."""
    for attempt in range(1, max_attempts + 1):
        try:
            r = session.post(url, json=sale, headers={"Authorization": f"Bearer {key}"}, timeout=10)
            if r.status_code not in RETRYABLE:
                return r.status_code
        except requests.RequestException:
            pass
        if attempt < max_attempts:
            sleep(min(0.2 * 2 ** attempt, 5))
    return "gave_up"


def run(gateway, shop_id, key, rate, count, cfg, session=None, sleep=time.sleep, now=None):
    session = session or requests.Session()
    url = gateway.rstrip("/") + "/v1/sales"
    rng = random.Random(cfg.seed)
    stats = Counter()
    accepted = []
    for _ in range(count):
        sale = make_live_sale(rng, cfg, shop_id, (now or (lambda: datetime.now(timezone.utc)))())
        status = post_sale(session, url, key, sale, sleep=sleep)
        stats[status] += 1
        if status == 202:
            accepted.append(sale)
        if accepted and rng.random() < cfg.retry_rate:  # the till re-sends something it already sent
            stats[f"resend:{post_sale(session, url, key, rng.choice(accepted), sleep=sleep)}"] += 1
        if rate > 0:
            sleep(1 / rate)
    return stats


def main(argv=None):
    p = argparse.ArgumentParser(description="Post live sales to the DukaFlow till gateway")
    p.add_argument("--gateway", default="http://localhost:8080")
    p.add_argument("--shop", type=int, required=True)
    p.add_argument("--key", required=True, help="API key for that shop (go run ./cmd/admin create-key)")
    p.add_argument("--rate", type=float, default=2.0, help="sales per second (0 = as fast as possible)")
    p.add_argument("--count", type=int, default=100)
    p.add_argument("--seed", type=int, default=int(time.time()))
    p.add_argument("--late-rate", type=float, default=0.05)
    p.add_argument("--bad-rate", type=float, default=0.02)
    p.add_argument("--retry-rate", type=float, default=0.03)
    args = p.parse_args(argv)

    cfg = replace(Config(), seed=args.seed, late_rate=args.late_rate, bad_rate=args.bad_rate,
                  retry_rate=args.retry_rate)
    started = time.time()
    stats = run(args.gateway, args.shop, args.key, args.rate, args.count, cfg)
    elapsed = time.time() - started
    print(f"posted {args.count} sales to shop {args.shop} in {elapsed:.1f}s")
    for status, n in sorted(stats.items(), key=lambda kv: str(kv[0])):
        print(f"  {status}: {n}")


if __name__ == "__main__":
    main()
