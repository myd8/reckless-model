from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import requests

from reckless_binance.binance_client import (
    fetch_daily_klines,
    fetch_exchange_info,
    parse_active_usdt_perpetuals,
)
from reckless_binance.events import attach_forward_returns, build_top20_entry_events
from reckless_binance.paths import output_dir
from reckless_binance.reporting import (
    bucket_forward_paths,
    compare_reversal_groups,
    summarize_forward_returns,
    write_svg_placeholder,
)
from reckless_binance.universe import with_days_since_listing


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Binance perp top-gainer event study")
    parser.add_argument("--lookback-years", type=int, default=3)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--lookforward-days", type=int, default=14)
    return parser


def _timestamp_to_date(value: int) -> pd.Timestamp:
    return pd.to_datetime(value, unit="ms", utc=True).tz_localize(None).normalize()


def _date_to_ms(value: pd.Timestamp) -> int:
    return int(value.tz_localize("UTC").timestamp() * 1000)


def _klines_to_frame(symbol: str, rows: list[list[object]]) -> pd.DataFrame:
    frame = pd.DataFrame(
        rows,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trade_count",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ],
    )
    if frame.empty:
        return frame
    frame["symbol"] = symbol
    frame["date"] = pd.to_datetime(frame["open_time"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    numeric_columns = ["open", "high", "low", "close", "volume", "quote_volume"]
    for column in numeric_columns:
        frame[column] = frame[column].astype(float)
    frame["trade_count"] = frame["trade_count"].astype(int)
    return frame[["date", "symbol", "open", "high", "low", "close", "volume", "quote_volume", "trade_count"]]


def _metadata_frame(payload: dict[str, object]) -> pd.DataFrame:
    perps = parse_active_usdt_perpetuals(payload)
    frame = pd.DataFrame(perps)
    if frame.empty:
        return frame
    frame = frame.rename(columns={"symbol": "symbol", "onboardDate": "onboard_ms"})
    frame["onboard_date"] = frame["onboard_ms"].map(_timestamp_to_date)
    return frame[["symbol", "pair", "onboard_date", "status"]]


def _load_price_history(
    session: requests.Session,
    metadata: pd.DataFrame,
    lookback_years: int,
) -> pd.DataFrame:
    end_date = pd.Timestamp.now("UTC").tz_localize(None).normalize()
    start_date = end_date - pd.DateOffset(years=lookback_years)
    frames: list[pd.DataFrame] = []

    for row in metadata.itertuples(index=False):
        symbol_start = max(start_date, row.onboard_date)
        if symbol_start > end_date:
            continue
        klines = fetch_daily_klines(
            session=session,
            symbol=row.symbol,
            start_ms=_date_to_ms(symbol_start),
            end_ms=_date_to_ms(end_date + pd.Timedelta(days=1)),
        )
        frame = _klines_to_frame(row.symbol, klines)
        if frame.empty:
            continue
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    prices = pd.concat(frames, ignore_index=True)
    prices = prices.merge(metadata[["symbol", "onboard_date"]], on="symbol", how="left")
    prices = with_days_since_listing(prices)
    prices["ret_7d"] = prices.groupby("symbol")["close"].pct_change(7)
    prices["rank_7d"] = prices.groupby("date")["ret_7d"].rank(ascending=False, method="first")
    return prices.sort_values(["date", "rank_7d", "symbol"]).reset_index(drop=True)


def _build_events(prices: pd.DataFrame, top_n: int, lookback_days: int, lookforward_days: int) -> pd.DataFrame:
    candidate_columns = [
        "date",
        "symbol",
        "ret_7d",
        "rank_7d",
        "days_since_listing",
        "close",
        "volume",
        "quote_volume",
        "trade_count",
    ]
    candidates = prices.loc[prices["days_since_listing"] >= 7, candidate_columns].dropna(subset=["ret_7d", "rank_7d"])
    events = build_top20_entry_events(candidates, top_n=top_n)
    return attach_forward_returns(
        events,
        prices[["date", "symbol", "close"]],
        lookback_days=lookback_days,
        lookforward_days=lookforward_days,
    )


def run_pipeline(args: argparse.Namespace) -> dict[str, Path]:
    out_dir = output_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    exchange_info = fetch_exchange_info(session)
    metadata = _metadata_frame(exchange_info)
    metadata.to_csv(raw_dir / "binance_active_usdt_perps.csv", index=False)

    prices = _load_price_history(session, metadata, lookback_years=args.lookback_years)
    prices.to_csv(raw_dir / "binance_daily_prices.csv", index=False)

    events = _build_events(
        prices,
        top_n=args.top_n,
        lookback_days=args.lookback_days,
        lookforward_days=args.lookforward_days,
    )
    events.to_csv(out_dir / "events.csv", index=False)

    forward = summarize_forward_returns(events, max_horizon=args.lookforward_days)
    forward.to_csv(out_dir / "forward_returns_by_day.csv", index=False)

    bucket_paths = bucket_forward_paths(events, max_horizon=args.lookforward_days)
    bucket_paths.to_csv(out_dir / "bucket_paths.csv", index=False)

    feature_columns = [
        column
        for column in ["ret_7d", "top20_streak_length", "prior_top20_entries_30d", "days_since_listing", "volume", "quote_volume", "trade_count"]
        if column in events.columns
    ]
    feature_summary = compare_reversal_groups(events, feature_columns=feature_columns)
    feature_summary.to_csv(out_dir / "feature_comparison.csv", index=False)

    write_svg_placeholder(out_dir / "forward_return_buckets.svg")

    summary = {
        "event_count": int(events.shape[0]),
        "symbol_count": int(prices["symbol"].nunique()) if not prices.empty else 0,
        "date_min": prices["date"].min().isoformat() if not prices.empty else None,
        "date_max": prices["date"].max().isoformat() if not prices.empty else None,
    }
    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return {
        "events": out_dir / "events.csv",
        "forward": out_dir / "forward_returns_by_day.csv",
        "buckets": out_dir / "bucket_paths.csv",
        "features": out_dir / "feature_comparison.csv",
        "chart": out_dir / "forward_return_buckets.svg",
        "summary": summary_path,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_pipeline(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
