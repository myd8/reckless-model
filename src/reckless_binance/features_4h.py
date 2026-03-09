from __future__ import annotations

import pandas as pd


def build_event_feature_panel(
    *,
    events: pd.DataFrame,
    market_bars: pd.DataFrame,
    binance_oi: pd.DataFrame,
    bybit_oi: pd.DataFrame,
    binance_funding: pd.DataFrame,
    bybit_funding: pd.DataFrame,
    sentiment: pd.DataFrame,
    supply: pd.DataFrame,
    bars_each_side: int = 84,
    funding_window_bars: int = 6,
    oi_z_window: int = 30,
) -> pd.DataFrame:
    """Build a symmetric 4h feature panel around each top-gainer event."""

    bars = market_bars.copy()
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
    bars = bars.sort_values(["asset", "timestamp"]).reset_index(drop=True)

    bars = bars.merge(
        _prepare_series(binance_oi, "oi_usd", "oi_usd_binance"),
        on=["asset", "timestamp"],
        how="left",
    )
    bars = bars.merge(
        _prepare_series(bybit_oi, "oi_usd", "oi_usd_bybit"),
        on=["asset", "timestamp"],
        how="left",
    )
    bars["oi_usd_total"] = bars[["oi_usd_binance", "oi_usd_bybit"]].fillna(0.0).sum(axis=1).astype(float)
    bars["oi_share_binance"] = _safe_ratio(bars["oi_usd_binance"], bars["oi_usd_total"])
    bars["oi_share_bybit"] = _safe_ratio(bars["oi_usd_bybit"], bars["oi_usd_total"])
    prev_4h = bars.groupby("asset")["oi_usd_total"].shift(1)
    prev_24h = bars.groupby("asset")["oi_usd_total"].shift(6)
    oi_change_4h = (bars["oi_usd_total"] / prev_4h.replace(0, pd.NA)) - 1.0
    oi_change_24h = (bars["oi_usd_total"] / prev_24h.replace(0, pd.NA)) - 1.0
    bars["oi_change_4h"] = oi_change_4h.map(lambda value: None if pd.isna(value) else round(float(value), 10))
    bars["oi_change_24h"] = oi_change_24h.map(lambda value: None if pd.isna(value) else round(float(value), 10))
    change_mean = bars.groupby("asset")["oi_change_4h"].transform(
        lambda series: series.rolling(oi_z_window, min_periods=1).mean()
    )
    change_std = bars.groupby("asset")["oi_change_4h"].transform(
        lambda series: series.rolling(oi_z_window, min_periods=1).std(ddof=0)
    )
    oi_z = (bars["oi_change_4h"] - change_mean) / change_std.replace(0, pd.NA)
    bars["oi_zscore_lookback"] = oi_z.map(lambda value: None if pd.isna(value) else round(float(value), 10))
    bars["oi_change_4h_total"] = bars["oi_change_4h"]
    bars["oi_change_z_30"] = bars["oi_zscore_lookback"]

    bars = bars.merge(
        _prepare_series(binance_funding, "funding_rate", "funding_binance"),
        on=["asset", "timestamp"],
        how="left",
    )
    bars = bars.merge(
        _prepare_series(bybit_funding, "funding_rate", "funding_bybit"),
        on=["asset", "timestamp"],
        how="left",
    )
    bars["funding_rate"] = bars[["funding_binance", "funding_bybit"]].mean(axis=1).round(10)
    bars["funding_rolling_mean"] = (
        bars.groupby("asset")["funding_rate"]
        .transform(lambda series: series.rolling(funding_window_bars, min_periods=1).mean())
        .round(10)
    )
    funding_mean = bars.groupby("asset")["funding_rate"].transform(
        lambda series: series.rolling(funding_window_bars, min_periods=1).mean()
    )
    funding_std = bars.groupby("asset")["funding_rate"].transform(
        lambda series: series.rolling(funding_window_bars, min_periods=1).std(ddof=0)
    )
    funding_z = (bars["funding_rate"] - funding_mean) / funding_std.replace(0, pd.NA)
    bars["funding_zscore"] = funding_z.map(lambda value: None if pd.isna(value) else round(float(value), 10))
    bars["funding_mean_1d"] = bars["funding_rolling_mean"]
    bars = bars.merge(
        _prepare_series(sentiment, "long_short_ratio", "long_short_ratio"),
        on=["asset", "timestamp"],
        how="left",
    )
    bars = _merge_supply_asof(bars, supply)

    event_rows: list[pd.DataFrame] = []
    events_frame = events.copy()
    events_frame["event_time"] = pd.to_datetime(events_frame["event_time"], utc=True)
    for event in events_frame.itertuples(index=False):
        asset_bars = bars.loc[bars["asset"] == event.asset].copy().reset_index(drop=True)
        anchor = asset_bars.index[asset_bars["timestamp"] == event.event_time]
        if len(anchor) == 0:
            continue
        anchor_idx = int(anchor[0])
        lower = max(anchor_idx - bars_each_side, 0)
        upper = min(anchor_idx + bars_each_side, len(asset_bars) - 1)
        window = asset_bars.iloc[lower : upper + 1].copy()
        window["event_id"] = event.event_id
        window["event_time"] = event.event_time
        window["rel_bar"] = range(lower - anchor_idx, upper - anchor_idx + 1)
        event_rows.append(window)

    if not event_rows:
        return pd.DataFrame()

    panel = pd.concat(event_rows, ignore_index=True)
    columns = [
        "event_id",
        "asset",
        "event_time",
        "timestamp",
        "rel_bar",
        "close",
        "return_4h",
        "volume_quote",
        "oi_usd_binance",
        "oi_usd_bybit",
        "oi_usd_total",
        "oi_share_binance",
        "oi_share_bybit",
        "oi_change_4h",
        "oi_change_24h",
        "oi_zscore_lookback",
        "oi_change_4h_total",
        "oi_change_z_30",
        "funding_binance",
        "funding_bybit",
        "funding_rate",
        "funding_rolling_mean",
        "funding_zscore",
        "funding_mean_1d",
        "long_short_ratio",
        "circ_supply",
        "max_supply",
        "float_ratio",
        "mcap_est",
    ]
    return panel.reindex(columns=columns)


def _prepare_series(frame: pd.DataFrame, source_column: str, target_column: str) -> pd.DataFrame:
    prepared = frame.copy()
    if "asset" not in prepared.columns or "timestamp" not in prepared.columns or source_column not in prepared.columns:
        return pd.DataFrame(columns=["asset", "timestamp", target_column])
    prepared["timestamp"] = pd.to_datetime(prepared["timestamp"], utc=True)
    return prepared.reindex(columns=["asset", "timestamp", source_column]).rename(columns={source_column: target_column})


def _merge_supply_asof(bars: pd.DataFrame, supply: pd.DataFrame) -> pd.DataFrame:
    if supply.empty:
        return bars

    supply_frame = supply.copy()
    supply_frame["timestamp"] = pd.to_datetime(supply_frame["timestamp"], utc=True)
    supply_frame = supply_frame.sort_values(["asset", "timestamp"]).reset_index(drop=True)

    merged_frames: list[pd.DataFrame] = []
    for asset, asset_bars in bars.groupby("asset", sort=False):
        asset_supply = supply_frame.loc[supply_frame["asset"] == asset]
        if asset_supply.empty:
            merged_frames.append(asset_bars.copy())
            continue
        merged_frames.append(
            pd.merge_asof(
                asset_bars.sort_values("timestamp"),
                asset_supply.sort_values("timestamp"),
                on="timestamp",
                by="asset",
                direction="backward",
            )
        )
    return pd.concat(merged_frames, ignore_index=True)


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return (numerator / denominator.where(denominator != 0)).round(10)
