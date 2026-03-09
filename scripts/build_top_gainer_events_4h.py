from __future__ import annotations

import argparse

from reckless_binance.pipeline_4h import build_top_gainer_events_artifact


def main(argv: list[str] | None = None) -> None:
    """Build top-gainer entry events from cached raw 4h klines."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset-map-path", required=True)
    parser.add_argument("--binance-klines-path", required=True)
    parser.add_argument("--bybit-klines-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--min-volume-quote", type=float, default=10_000.0)
    args = parser.parse_args(argv)

    build_top_gainer_events_artifact(
        asset_map_path=args.asset_map_path,
        binance_klines_path=args.binance_klines_path,
        bybit_klines_path=args.bybit_klines_path,
        output_path=args.output_path,
        top_n=args.top_n,
        min_volume_quote=args.min_volume_quote,
    )


if __name__ == "__main__":
    main()
