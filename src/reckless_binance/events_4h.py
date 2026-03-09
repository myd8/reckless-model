from __future__ import annotations

import pandas as pd


def build_top_gainer_events(
    bars: pd.DataFrame,
    *,
    top_n: int = 10,
    min_volume_quote: float = 10_000.0,
) -> pd.DataFrame:
    """Build first-entry top-gainer events from top-N 4h return rankings."""

    ordered = bars.copy()
    ordered["timestamp"] = pd.to_datetime(ordered["timestamp"], utc=True)
    ordered = ordered.sort_values(["asset", "timestamp"]).reset_index(drop=True)
    if "return_4h" not in ordered.columns:
        ordered["return_4h"] = ordered.groupby("asset")["close"].pct_change().round(10)
    ranked = ordered.dropna(subset=["return_4h"]).copy()
    ranked = ranked.loc[ranked["volume_quote"].fillna(0.0) >= min_volume_quote].copy()
    ranked["rank_ret_4h"] = (
        ranked.groupby("timestamp")["return_4h"].rank(method="first", ascending=False).astype(int)
    )
    ranked["in_top"] = ranked["rank_ret_4h"] <= top_n
    previous = ranked.groupby("asset")["in_top"].shift(1)
    ranked["prev_in_top"] = previous.where(previous.notna(), False).astype(bool)

    events = ranked.loc[ranked["in_top"] & ranked["prev_in_top"].eq(False)].copy()
    events["event_time"] = events["timestamp"]
    events["event_id"] = events["asset"] + "|" + events["event_time"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    events["event_id"] = events["event_id"].str.replace(r"(\+0000)$", "+00:00", regex=True)
    events["prior_top_gainer_entries_30d"] = _compute_prior_entries_30d(events)
    events = events.sort_values(["event_time", "rank_ret_4h", "asset"], kind="stable").reset_index(drop=True)

    return events[
        [
            "event_id",
            "asset",
            "event_time",
            "close",
            "return_4h",
            "rank_ret_4h",
            "volume_quote",
            "prior_top_gainer_entries_30d",
        ]
    ].rename(columns={"return_4h": "ret_4h"}).reset_index(drop=True)


def _compute_prior_entries_30d(events: pd.DataFrame) -> pd.Series:
    counts: list[int] = []
    prior_times_by_asset: dict[str, list[pd.Timestamp]] = {}
    for row in events.itertuples(index=False):
        history = prior_times_by_asset.get(row.asset, [])
        window_start = row.event_time - pd.Timedelta(days=30)
        counts.append(sum(event_time >= window_start for event_time in history))
        history.append(row.event_time)
        prior_times_by_asset[row.asset] = history
    return pd.Series(counts, index=events.index, dtype="int64")
