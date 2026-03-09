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
from reckless_binance.filter_analysis import evaluate_candidate_filters
from reckless_binance.paths import output_dir
from reckless_binance.reporting import (
    bucket_forward_paths,
    compare_reversal_groups,
    render_forward_return_chart,
    summarize_forward_returns,
)
from reckless_binance.signals import build_signal_table, summarize_signal_candidates
from reckless_binance.universe import with_days_since_listing
from reckless_binance.walk_forward import (
    evaluate_monthly_walk_forward,
    evaluate_secondary_conditions_walk_forward,
    evaluate_tertiary_conditions_walk_forward,
)


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


def _jsonable_record(record: dict[str, object] | None) -> dict[str, object] | None:
    if record is None:
        return None
    clean: dict[str, object] = {}
    for key, value in record.items():
        if pd.isna(value):
            clean[key] = None
        elif hasattr(value, "item"):
            clean[key] = value.item()
        else:
            clean[key] = value
    return clean


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

    filter_results = evaluate_candidate_filters(events)
    filter_results.to_csv(out_dir / "filter_oos_results.csv", index=False)

    walk_forward_results, walk_forward_top, walk_forward_summary = evaluate_monthly_walk_forward(events)
    walk_forward_results.to_csv(out_dir / "walk_forward_results.csv", index=False)
    walk_forward_top.to_csv(out_dir / "walk_forward_top_filters.csv", index=False)
    walk_forward_summary.to_csv(out_dir / "walk_forward_summary.csv", index=False)

    secondary_results, secondary_summary = evaluate_secondary_conditions_walk_forward(
        events,
        base_filter_name="trade_count_top_half",
    )
    secondary_results.to_csv(out_dir / "secondary_filter_walk_forward_results.csv", index=False)
    secondary_summary.to_csv(out_dir / "secondary_filter_walk_forward_summary.csv", index=False)

    tertiary_results, tertiary_summary = evaluate_tertiary_conditions_walk_forward(
        events,
        base_filter_names=["trade_count_top_half", "quote_vol_top_half"],
    )
    tertiary_results.to_csv(out_dir / "tertiary_filter_walk_forward_results.csv", index=False)
    tertiary_summary.to_csv(out_dir / "tertiary_filter_walk_forward_summary.csv", index=False)

    signal_table, signal_candidates = build_signal_table(events)
    signal_table.to_csv(out_dir / "signal_table.csv", index=False)
    signal_candidates.to_csv(out_dir / "signal_candidates.csv", index=False)
    signal_candidate_summary = summarize_signal_candidates(signal_candidates)

    render_forward_return_chart(bucket_paths, out_dir / "forward_return_buckets.svg")

    summary = {
        "event_count": int(events.shape[0]),
        "symbol_count": int(prices["symbol"].nunique()) if not prices.empty else 0,
        "date_min": prices["date"].min().isoformat() if not prices.empty else None,
        "date_max": prices["date"].max().isoformat() if not prices.empty else None,
        "top_filter": _jsonable_record(filter_results.iloc[0].to_dict()) if not filter_results.empty else None,
        "top_walk_forward_filter": _jsonable_record(walk_forward_summary.iloc[0].to_dict()) if not walk_forward_summary.empty else None,
        "top_secondary_filter_on_trade_count": _jsonable_record(secondary_summary.iloc[0].to_dict()) if not secondary_summary.empty else None,
        "top_tertiary_filter_on_trade_count_quote_vol": _jsonable_record(tertiary_summary.iloc[0].to_dict()) if not tertiary_summary.empty else None,
        "signal_candidate_summary": signal_candidate_summary,
    }
    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return {
        "events": out_dir / "events.csv",
        "forward": out_dir / "forward_returns_by_day.csv",
        "buckets": out_dir / "bucket_paths.csv",
        "features": out_dir / "feature_comparison.csv",
        "filters": out_dir / "filter_oos_results.csv",
        "walk_forward_results": out_dir / "walk_forward_results.csv",
        "walk_forward_top_filters": out_dir / "walk_forward_top_filters.csv",
        "walk_forward_summary": out_dir / "walk_forward_summary.csv",
        "secondary_walk_forward_results": out_dir / "secondary_filter_walk_forward_results.csv",
        "secondary_walk_forward_summary": out_dir / "secondary_filter_walk_forward_summary.csv",
        "tertiary_walk_forward_results": out_dir / "tertiary_filter_walk_forward_results.csv",
        "tertiary_walk_forward_summary": out_dir / "tertiary_filter_walk_forward_summary.csv",
        "signal_table": out_dir / "signal_table.csv",
        "signal_candidates": out_dir / "signal_candidates.csv",
        "chart": out_dir / "forward_return_buckets.svg",
        "summary": summary_path,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_pipeline(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
