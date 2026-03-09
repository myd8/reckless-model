from __future__ import annotations

import pandas as pd


def build_signal_table(events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = events.copy()
    thresholds = {
        "trade_count_median": float(frame["trade_count"].median()),
        "quote_volume_median": float(frame["quote_volume"].median()),
        "ret_7d_median": float(frame["ret_7d"].median()),
    }

    frame["trade_count_top_half"] = frame["trade_count"] >= thresholds["trade_count_median"]
    frame["quote_vol_top_half"] = frame["quote_volume"] >= thresholds["quote_volume_median"]
    frame["ret_7d_top_half"] = frame["ret_7d"] >= thresholds["ret_7d_median"]

    rule_columns = ["trade_count_top_half", "quote_vol_top_half", "ret_7d_top_half"]
    frame["signal_strength_score"] = frame[rule_columns].sum(axis=1).astype(int)
    frame["signal_active"] = frame["signal_strength_score"] == 3
    frame["signal_name"] = "trade_count_top_half & quote_vol_top_half & ret_7d_top_half"

    candidates = frame.loc[frame["signal_active"]].copy().reset_index(drop=True)
    return frame.reset_index(drop=True), candidates


def summarize_signal_candidates(candidates: pd.DataFrame) -> dict[str, float | int | None]:
    if candidates.empty:
        return {
            "signal_count": 0,
            "reversal_rate": None,
            "median_forward_14d_return": None,
            "mean_forward_14d_return": None,
        }
    return {
        "signal_count": int(candidates.shape[0]),
        "reversal_rate": float(candidates["reversal_14d"].mean()),
        "median_forward_14d_return": float(candidates["forward_14d_return"].median()),
        "mean_forward_14d_return": float(candidates["forward_14d_return"].mean()),
    }
