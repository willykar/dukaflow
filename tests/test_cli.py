import json

from generator.cli import main


def test_cli_writes_partitions_by_sync_date(tmp_path):
    main(["--seed", "1", "--shops", "2", "--days", "3", "--out", str(tmp_path)])
    parts = sorted(p.name for p in (tmp_path / "sales").iterdir())
    assert parts and all(p.startswith("sync_date=") for p in parts)
    first = (tmp_path / "sales" / parts[0] / "part-0000.json").read_text(encoding="utf-8").splitlines()[0]
    assert "receipt_id" in json.loads(first)
    assert (tmp_path / "mpesa" / "transactions.json").exists()
