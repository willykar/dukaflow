"""Write simulated data into the lake raw zone, partitioned by sync date.

    python -m generator.cli --shops 5 --days 30 --out data/lake/raw

TODO (phase 2.3): --live --gateway URL posts sales to the Go till gateway in real time instead.
"""

import argparse
import json
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

from generator.simulate import Config, simulate


def write_raw(data, out):
    by_day = defaultdict(list)
    for sale in data["sales"]:
        by_day[sale["synced_at"][:10]].append(sale)
    for sync_date, sales in by_day.items():
        part = Path(out) / "sales" / f"sync_date={sync_date}"
        part.mkdir(parents=True, exist_ok=True)
        with open(part / "part-0000.json", "w", encoding="utf-8") as f:
            for sale in sales:
                f.write(json.dumps(sale) + "\n")
    mpesa_dir = Path(out) / "mpesa"
    mpesa_dir.mkdir(parents=True, exist_ok=True)
    with open(mpesa_dir / "transactions.json", "w", encoding="utf-8") as f:
        for txn in data["mpesa"]:
            f.write(json.dumps(txn) + "\n")
    return len(by_day)


def main(argv=None):
    p = argparse.ArgumentParser(description="Generate synthetic duka data")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--shops", type=int, default=3)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--out", default="data/lake/raw")
    args = p.parse_args(argv)

    data = simulate(replace(Config(), seed=args.seed, shops=args.shops, days=args.days))
    partitions = write_raw(data, args.out)
    print(f"{len(data['sales'])} sales and {len(data['mpesa'])} M-Pesa rows -> {args.out} ({partitions} sync dates)")


if __name__ == "__main__":
    main()
