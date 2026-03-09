from __future__ import annotations

from itertools import combinations
from typing import Callable

import pandas as pd


FilterFn = Callable[[pd.DataFrame, dict[str, float]], pd.Series]


def split_events_train_test(
    events: pd.DataFrame,
    train_fraction: float = 2 / 3,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = events.dropna(subset=["forward_14d_return", "reversal_14d"]).copy()
    frame["date"] = pd.to_datetime(frame["date"])
    unique_dates = sorted(frame["date"].drop_duplicates())
    split_index = max(1, min(len(unique_dates) - 1, int(len(unique_dates) * train_fraction)))
    train_dates = set(unique_dates[:split_index])
    train = frame.loc[frame["date"].isin(train_dates)].copy()
    test = frame.loc[~frame["date"].isin(train_dates)].copy()
    return train.reset_index(drop=True), test.reset_index(drop=True)


def evaluate_candidate_filters(
    events: pd.DataFrame,
    train_fraction: float = 2 / 3,
    min_train_samples: int = 100,
    min_test_samples: int = 50,
) -> pd.DataFrame:
    labeled_events = events.dropna(subset=["forward_14d_return", "reversal_14d"]).copy()
    train, test = split_events_train_test(labeled_events, train_fraction=train_fraction)
    thresholds = _build_thresholds(train)
    specs = _candidate_specs()
    train_total = max(int(train.shape[0]), 1)
    test_total = max(int(test.shape[0]), 1)

    rows = []
    for name, rule in specs.items():
        train_mask = rule(train, thresholds)
        test_mask = rule(test, thresholds)
        train_subset = train.loc[train_mask]
        test_subset = test.loc[test_mask]
        if len(train_subset) < min_train_samples or len(test_subset) < min_test_samples:
            continue
        rows.append(
            {
                "filter_name": name,
                "train_start_date": train["date"].min().date().isoformat(),
                "train_end_date": train["date"].max().date().isoformat(),
                "test_start_date": test["date"].min().date().isoformat(),
                "test_end_date": test["date"].max().date().isoformat(),
                **_metrics_for_subset(train_subset, "train", total_count=train_total),
                **_metrics_for_subset(test_subset, "test", total_count=test_total),
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result["train_score"] = (
        result["train_reversal_rate"] * result["train_share"] * (-result["train_forward_14d_median"])
    )
    return result.sort_values(
        ["train_score", "train_reversal_rate", "train_forward_14d_median", "train_count"],
        ascending=[False, False, True, False],
    ).reset_index(drop=True)


def _build_thresholds(train: pd.DataFrame) -> dict[str, float]:
    return {
        "ret_7d_q50": float(train["ret_7d"].quantile(0.50)),
        "ret_7d_q75": float(train["ret_7d"].quantile(0.75)),
        "days_since_listing_median": float(train["days_since_listing"].median()),
        "quote_volume_median": float(train["quote_volume"].median()),
        "trade_count_median": float(train["trade_count"].median()),
    }


def _candidate_specs() -> dict[str, FilterFn]:
    singles: dict[str, FilterFn] = {
        "ret_7d_top_half": lambda frame, t: frame["ret_7d"] >= t["ret_7d_q50"],
        "ret_7d_top_quartile": lambda frame, t: frame["ret_7d"] >= t["ret_7d_q75"],
        "younger_than_median": lambda frame, t: frame["days_since_listing"] <= t["days_since_listing_median"],
        "quote_vol_top_half": lambda frame, t: frame["quote_volume"] >= t["quote_volume_median"],
        "trade_count_top_half": lambda frame, t: frame["trade_count"] >= t["trade_count_median"],
        "repeat_entry": lambda frame, t: frame["prior_top20_entries_30d"] >= 1,
    }

    specs = dict(singles)
    keys = list(singles.keys())
    for left, right in combinations(keys, 2):
        specs[f"{left} & {right}"] = _combine(singles[left], singles[right])
    return specs


def _combine(left: FilterFn, right: FilterFn) -> FilterFn:
    return lambda frame, thresholds: left(frame, thresholds) & right(frame, thresholds)


def _metrics_for_subset(frame: pd.DataFrame, prefix: str, total_count: int) -> dict[str, float]:
    return {
        f"{prefix}_count": int(frame.shape[0]),
        f"{prefix}_share": float(frame.shape[0] / total_count),
        f"{prefix}_reversal_rate": float(frame["reversal_14d"].mean()),
        f"{prefix}_forward_14d_median": float(frame["forward_14d_return"].median()),
        f"{prefix}_forward_14d_mean": float(frame["forward_14d_return"].mean()),
    }
