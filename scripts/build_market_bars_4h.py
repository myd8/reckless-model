from __future__ import annotations

import argparse

from reckless_binance.parquet_io import read_parquet, write_parquet
from reckless_binance.pipeline_4h import build_canonical_market_bars


def main(argv: list[str] | None = None) -> None:
    """Build canonical 4h market bars from raw Binance and Bybit kline parquet."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binance-klines-path", required=True)
    parser.add_argument("--bybit-klines-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args(argv)

    market_bars = build_canonical_market_bars(
        binance_klines=read_parquet(args.binance_klines_path),
        bybit_klines=read_parquet(args.bybit_klines_path),
    )
    write_parquet(market_bars, args.output_path)


if __name__ == "__main__":
    main()
