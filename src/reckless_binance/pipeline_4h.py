from __future__ import annotations

from pathlib import Path

import pandas as pd

from reckless_binance.events_4h import build_top_gainer_events
from reckless_binance.features_4h import build_event_feature_panel
from reckless_binance.labels import build_event_labels, summarize_labels
from reckless_binance.parquet_io import read_parquet, write_csv, write_parquet


def build_canonical_market_bars(
    *,
    binance_klines: pd.DataFrame,
    bybit_klines: pd.DataFrame,
    assets: list[str] | None = None,
) -> pd.DataFrame:
    """Aggregate exchange-level 4h bars into a canonical asset bar series."""

    combined = pd.concat([binance_klines, bybit_klines], ignore_index=True)
    if assets is not None:
        combined = combined.loc[combined["asset"].isin(assets)].copy()
    combined["timestamp"] = pd.to_datetime(combined["timestamp"], utc=True)
    grouped = (
        combined.groupby(["asset", "timestamp"], as_index=False)
        .agg(close=("close", "mean"), volume_quote=("volume_quote", "sum"))
        .sort_values(["asset", "timestamp"], kind="stable")
        .reset_index(drop=True)
    )
    grouped["return_4h"] = grouped.groupby("asset")["close"].pct_change().round(10)
    return grouped


def combine_sentiment_frames(*frames: pd.DataFrame) -> pd.DataFrame:
    """Combine exchange-level sentiment histories to a single asset-level ratio series."""

    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=["asset", "timestamp", "long_short_ratio"])
    combined = pd.concat(non_empty, ignore_index=True)
    combined["timestamp"] = pd.to_datetime(combined["timestamp"], utc=True)
    return (
        combined.groupby(["asset", "timestamp"], as_index=False)["long_short_ratio"]
        .mean()
        .sort_values(["asset", "timestamp"], kind="stable")
        .reset_index(drop=True)
    )


def build_top_gainer_events_artifact(
    *,
    asset_map_path: str | Path,
    binance_klines_path: str | Path,
    bybit_klines_path: str | Path,
    output_path: str | Path,
    top_n: int = 10,
    min_volume_quote: float = 10_000.0,
) -> pd.DataFrame:
    """Build and persist top-gainer entry events from cached raw kline parquet."""

    asset_map = read_parquet(asset_map_path)
    market_bars = build_canonical_market_bars(
        binance_klines=read_parquet(binance_klines_path),
        bybit_klines=read_parquet(bybit_klines_path),
        assets=asset_map["asset"].tolist() if "asset" in asset_map else None,
    )
    events = build_top_gainer_events(market_bars, top_n=top_n, min_volume_quote=min_volume_quote)
    write_parquet(events, output_path)
    return events


def build_event_features_artifact(
    *,
    events_path: str | Path,
    market_bars_path: str | Path,
    binance_oi_path: str | Path,
    bybit_oi_path: str | Path,
    binance_funding_path: str | Path,
    bybit_funding_path: str | Path,
    sentiment_path: str | Path,
    supply_path: str | Path,
    output_path: str | Path,
    bars_each_side: int = 84,
    funding_window_bars: int = 6,
    oi_z_window: int = 30,
) -> pd.DataFrame:
    """Build and persist the symmetric 4h event feature panel."""

    panel = build_event_feature_panel(
        events=read_parquet(events_path),
        market_bars=read_parquet(market_bars_path),
        binance_oi=read_parquet(binance_oi_path),
        bybit_oi=read_parquet(bybit_oi_path),
        binance_funding=read_parquet(binance_funding_path),
        bybit_funding=read_parquet(bybit_funding_path),
        sentiment=read_parquet(sentiment_path),
        supply=read_parquet(supply_path),
        bars_each_side=bars_each_side,
        funding_window_bars=funding_window_bars,
        oi_z_window=oi_z_window,
    )
    write_parquet(panel, output_path)
    return panel


def build_event_labels_artifact(
    *,
    feature_path: str | Path,
    labels_path: str | Path,
    summary_path: str | Path,
    bars_per_day: int = 6,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build and persist event labels and the label comparison summary."""

    panel = read_parquet(feature_path)
    labels = build_event_labels(panel, bars_per_day=bars_per_day)
    summary = summarize_labels(labels, panel)
    write_parquet(labels, labels_path)
    write_csv(summary, summary_path)
    return labels, summary
