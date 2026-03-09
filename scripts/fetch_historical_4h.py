from __future__ import annotations

import argparse
from pathlib import Path

from reckless_binance.ingest_4h import fetch_all_to_parquet


def main(argv: list[str] | None = None) -> None:
    """Fetch raw 4h historical data from Binance, Bybit, and a supply source."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-root", type=Path, default=Path("outputs"))
    parser.add_argument("--limit-assets", type=int, default=None)
    args = parser.parse_args(argv)

    fetch_all_to_parquet(
        start_date=args.start_date,
        end_date=args.end_date,
        output_root=args.output_root,
        limit_assets=args.limit_assets,
    )


if __name__ == "__main__":
    main()
