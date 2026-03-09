from __future__ import annotations

from typing import Any

import pandas as pd


def build_event_labels(panel: pd.DataFrame, *, bars_per_day: int = 6) -> pd.DataFrame:
    """Compute forward-path statistics and breakout/blowoff labels per event."""

    rows: list[dict[str, Any]] = []
    for event_id, event_panel in panel.groupby("event_id", sort=False):
        ordered = event_panel.sort_values("rel_bar").reset_index(drop=True)
        base_row = ordered.loc[ordered["rel_bar"] == 0].iloc[0]
        event_close = float(base_row["close"])

        def rel_return(day: int) -> float | None:
            target = ordered.loc[ordered["rel_bar"] == day * bars_per_day, "close"]
            if target.empty:
                return None
            return round((float(target.iloc[0]) / event_close) - 1.0, 10)

        future_7d = ordered.loc[(ordered["rel_bar"] >= 1) & (ordered["rel_bar"] <= 7 * bars_per_day)].copy()
        future_14d = ordered.loc[(ordered["rel_bar"] >= 1) & (ordered["rel_bar"] <= 14 * bars_per_day)].copy()
        future_7d["event_return"] = (future_7d["close"] / event_close) - 1.0

        peak_idx = future_7d["close"].idxmax()
        peak_row = future_7d.loc[peak_idx]
        peak_return = round(float(peak_row["event_return"]), 10)
        peak_day = int(peak_row["rel_bar"] / bars_per_day)
        trough_return = round(float(future_7d["event_return"].min()), 10)

        close_day7 = ordered.loc[ordered["rel_bar"] == 7 * bars_per_day, "close"].iloc[0]
        close_day14 = ordered.loc[ordered["rel_bar"] == 14 * bars_per_day, "close"].iloc[0]
        drawdown_to_day7 = round((float(close_day7) / float(peak_row["close"])) - 1.0, 10)
        drawdown_to_day14 = round((float(close_day14) / float(peak_row["close"])) - 1.0, 10)

        ret_1d = rel_return(1)
        ret_3d = rel_return(3)
        ret_7d = rel_return(7)
        ret_14d = rel_return(14)

        rows.append(
            {
                "event_id": event_id,
                "asset": base_row.get("asset"),
                "event_time": base_row.get("event_time"),
                "ret_1d": ret_1d,
                "ret_3d": ret_3d,
                "ret_7d": ret_7d,
                "ret_14d": ret_14d,
                "peak_return_first_7d": peak_return,
                "peak_day_first_7d": peak_day,
                "trough_return_first_7d": trough_return,
                "drawdown_from_peak_to_day7": drawdown_to_day7,
                "drawdown_from_peak_to_day14": drawdown_to_day14,
                "label": _classify_label(
                    ret_1d=ret_1d,
                    ret_3d=ret_3d,
                    ret_7d=ret_7d,
                    peak_day_first_7d=peak_day,
                    peak_return_first_7d=peak_return,
                    drawdown_from_peak_to_day7=drawdown_to_day7,
                    drawdown_from_peak_to_day14=drawdown_to_day14,
                ),
            }
        )

    return pd.DataFrame(rows)


def summarize_labels(labels: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    """Summarize return and event-time feature medians by label."""

    event_rows = panel.loc[panel["rel_bar"] == 0, ["event_id", "oi_change_4h_total", "funding_mean_1d", "float_ratio"]]
    merged = labels.merge(event_rows, on="event_id", how="left")
    summary = (
        merged.groupby("label", sort=True)
        .agg(
            count=("event_id", "size"),
            median_ret_3d=("ret_3d", "median"),
            median_ret_7d=("ret_7d", "median"),
            median_ret_14d=("ret_14d", "median"),
            median_oi_change_4h_total=("oi_change_4h_total", "median"),
            median_funding_at_event=("funding_mean_1d", "median"),
            median_float_ratio=("float_ratio", "median"),
        )
        .reset_index()
    )
    return summary


def _classify_label(
    *,
    ret_1d: float | None,
    ret_3d: float | None,
    ret_7d: float | None,
    peak_day_first_7d: int,
    peak_return_first_7d: float,
    drawdown_from_peak_to_day7: float,
    drawdown_from_peak_to_day14: float,
) -> str:
    if ret_1d is not None and ret_1d < 0:
        return "immediate_reversal"

    if ret_1d is not None and ret_1d > 0.05:
        if ret_3d is not None and ret_3d <= 0:
            return "delayed_blowoff"
        if (
            peak_day_first_7d in {2, 3, 4, 5}
            and peak_return_first_7d > 0.10
            and drawdown_from_peak_to_day14 <= -0.15
        ):
            return "delayed_blowoff"

    if (
        ret_1d is not None
        and ret_3d is not None
        and ret_7d is not None
        and ret_1d > 0.05
        and ret_3d > 0
        and ret_7d > 0
        and drawdown_from_peak_to_day7 > -0.10
    ):
        return "breakout"

    return "unclassified"
