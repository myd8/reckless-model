from __future__ import annotations

import argparse

from reckless_binance.pipeline_4h import build_event_features_artifact


def main(argv: list[str] | None = None) -> None:
    """Build the symmetric 4h event feature panel from cached parquet inputs."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events-path", required=True)
    parser.add_argument("--market-bars-path", required=True)
    parser.add_argument("--binance-oi-path", required=True)
    parser.add_argument("--bybit-oi-path", required=True)
    parser.add_argument("--binance-funding-path", required=True)
    parser.add_argument("--bybit-funding-path", required=True)
    parser.add_argument("--sentiment-path", required=True)
    parser.add_argument("--supply-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--bars-each-side", type=int, default=84)
    parser.add_argument("--funding-window-bars", type=int, default=6)
    parser.add_argument("--oi-z-window", type=int, default=30)
    args = parser.parse_args(argv)

    build_event_features_artifact(
        events_path=args.events_path,
        market_bars_path=args.market_bars_path,
        binance_oi_path=args.binance_oi_path,
        bybit_oi_path=args.bybit_oi_path,
        binance_funding_path=args.binance_funding_path,
        bybit_funding_path=args.bybit_funding_path,
        sentiment_path=args.sentiment_path,
        supply_path=args.supply_path,
        output_path=args.output_path,
        bars_each_side=args.bars_each_side,
        funding_window_bars=args.funding_window_bars,
        oi_z_window=args.oi_z_window,
    )


if __name__ == "__main__":
    main()
