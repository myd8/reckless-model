from __future__ import annotations

from pathlib import Path

import pandas as pd


def summarize_forward_returns(events: pd.DataFrame, max_horizon: int) -> pd.DataFrame:
    rows = []
    for day in range(1, max_horizon + 1):
        column = f"ret_+{day}"
        values = events[column].dropna()
        rows.append(
            {
                "horizon_day": day,
                "count": int(values.shape[0]),
                "mean_return": values.mean() if not values.empty else None,
                "median_return": values.median() if not values.empty else None,
                "reversal_probability": (values < 0).mean() if not values.empty else None,
            }
        )
    return pd.DataFrame(rows)


def assign_performance_buckets(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    labels = [
        "25th percentile and below",
        "25th percentile - 50th percentile",
        "50th percentile - 75th percentile",
        "75th percentile & up",
    ]
    frame["performance_bucket"] = pd.qcut(
        frame["ret_7d"],
        q=4,
        labels=labels,
        duplicates="drop",
    )
    return frame


def bucket_forward_paths(events: pd.DataFrame, max_horizon: int) -> pd.DataFrame:
    bucketed = assign_performance_buckets(events)
    rows = []
    for bucket, group in bucketed.groupby("performance_bucket", observed=True):
        for day in range(1, max_horizon + 1):
            column = f"ret_+{day}"
            values = group[column].dropna()
            rows.append(
                {
                    "bucket": bucket,
                    "horizon_day": day,
                    "count": int(values.shape[0]),
                    "mean_return": values.mean() if not values.empty else None,
                    "median_return": values.median() if not values.empty else None,
                }
            )
    return pd.DataFrame(rows)


def compare_reversal_groups(events: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    rows = []
    for feature in feature_columns:
        reversed_values = events.loc[events["reversal_14d"] == 1, feature].dropna()
        continued_values = events.loc[events["reversal_14d"] == 0, feature].dropna()
        rows.append(
            {
                "feature": feature,
                "reversed_mean": reversed_values.mean() if not reversed_values.empty else None,
                "reversed_median": reversed_values.median() if not reversed_values.empty else None,
                "continued_mean": continued_values.mean() if not continued_values.empty else None,
                "continued_median": continued_values.median() if not continued_values.empty else None,
            }
        )
    return pd.DataFrame(rows)


def write_svg_placeholder(out_path: Path) -> None:
    out_path.write_text(
        "<svg xmlns='http://www.w3.org/2000/svg' width='600' height='300'>"
        "<text x='20' y='40'>Forward return chart placeholder</text>"
        "</svg>",
        encoding="utf-8",
    )
