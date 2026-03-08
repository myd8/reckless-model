from __future__ import annotations

import pandas as pd

from reckless_binance.filter_analysis import _build_thresholds, _candidate_specs, _metrics_for_subset


def evaluate_monthly_walk_forward(
    events: pd.DataFrame,
    min_train_months: int = 12,
    min_train_samples: int = 100,
    min_test_samples: int = 50,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    frame = events.dropna(subset=["forward_14d_return", "reversal_14d"]).copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["month"] = frame["date"].dt.to_period("M")

    months = sorted(frame["month"].drop_duplicates())
    specs = _candidate_specs()
    result_rows: list[dict[str, object]] = []
    top_rows: list[dict[str, object]] = []

    for index in range(min_train_months, len(months)):
        train_months = months[:index]
        test_month = months[index]

        train = frame.loc[frame["month"].isin(train_months)].copy()
        test = frame.loc[frame["month"] == test_month].copy()
        if train.empty or test.empty:
            continue

        thresholds = _build_thresholds(train)
        train_total = max(int(train.shape[0]), 1)
        test_total = max(int(test.shape[0]), 1)
        month_rows: list[dict[str, object]] = []

        for name, rule in specs.items():
            train_subset = train.loc[rule(train, thresholds)]
            test_subset = test.loc[rule(test, thresholds)]
            if len(train_subset) < min_train_samples or len(test_subset) < min_test_samples:
                continue

            row = {
                "filter_name": name,
                "train_start_date": train["date"].min().date().isoformat(),
                "train_end_date": train["date"].max().date().isoformat(),
                "test_start_date": test["date"].min().date().isoformat(),
                "test_end_date": test["date"].max().date().isoformat(),
                "test_month": str(test_month),
                **_metrics_for_subset(train_subset, "train", total_count=train_total),
                **_metrics_for_subset(test_subset, "test", total_count=test_total),
            }
            row["train_score"] = (
                row["train_reversal_rate"] * row["train_share"] * (-row["train_forward_14d_median"])
            )
            month_rows.append(row)

        if not month_rows:
            continue

        month_df = pd.DataFrame(month_rows).sort_values(
            ["train_score", "train_reversal_rate", "train_forward_14d_median", "train_count"],
            ascending=[False, False, True, False],
        )
        month_df["selected"] = False
        month_df.iloc[0, month_df.columns.get_loc("selected")] = True
        result_rows.extend(month_df.to_dict("records"))
        top_rows.append(month_df.iloc[0].to_dict())

    results = pd.DataFrame(result_rows)
    top_filters = pd.DataFrame(top_rows)
    summary = summarize_walk_forward(top_filters)
    return results, top_filters, summary


def summarize_walk_forward(top_filters: pd.DataFrame) -> pd.DataFrame:
    if top_filters.empty:
        return pd.DataFrame(
            columns=[
                "filter_name",
                "selected_count",
                "avg_test_reversal_rate",
                "median_test_forward_14d_median",
                "avg_test_forward_14d_mean",
                "total_test_count",
            ]
        )

    summary = (
        top_filters.groupby("filter_name", observed=True)
        .agg(
            selected_count=("filter_name", "size"),
            avg_test_reversal_rate=("test_reversal_rate", "mean"),
            median_test_forward_14d_median=("test_forward_14d_median", "median"),
            avg_test_forward_14d_mean=("test_forward_14d_mean", "mean"),
            total_test_count=("test_count", "sum"),
        )
        .reset_index()
        .sort_values(
            ["selected_count", "avg_test_reversal_rate", "median_test_forward_14d_median"],
            ascending=[False, False, True],
        )
        .reset_index(drop=True)
    )
    return summary


def evaluate_secondary_conditions_walk_forward(
    events: pd.DataFrame,
    base_filter_name: str,
    min_train_months: int = 12,
    min_train_samples: int = 100,
    min_test_samples: int = 50,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = events.dropna(subset=["forward_14d_return", "reversal_14d"]).copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["month"] = frame["date"].dt.to_period("M")

    specs = _candidate_specs()
    if base_filter_name not in specs:
        raise ValueError(f"Unknown base filter: {base_filter_name}")

    base_rule = specs[base_filter_name]
    secondary_names = [
        name for name in specs.keys()
        if name != base_filter_name and " & " not in name
    ]

    months = sorted(frame["month"].drop_duplicates())
    result_rows: list[dict[str, object]] = []
    top_rows: list[dict[str, object]] = []

    for index in range(min_train_months, len(months)):
        train = frame.loc[frame["month"].isin(months[:index])].copy()
        test = frame.loc[frame["month"] == months[index]].copy()
        if train.empty or test.empty:
            continue

        thresholds = _build_thresholds(train)
        base_train = train.loc[base_rule(train, thresholds)].copy()
        base_test = test.loc[base_rule(test, thresholds)].copy()
        if len(base_train) < min_train_samples or len(base_test) < min_test_samples:
            continue

        month_rows: list[dict[str, object]] = []
        for secondary_name in secondary_names:
            secondary_rule = specs[secondary_name]
            train_subset = base_train.loc[secondary_rule(base_train, thresholds)].copy()
            test_subset = base_test.loc[secondary_rule(base_test, thresholds)].copy()
            if len(train_subset) < min_train_samples or len(test_subset) < min_test_samples:
                continue

            row = {
                "base_filter_name": base_filter_name,
                "secondary_filter_name": secondary_name,
                "combined_filter_name": f"{base_filter_name} & {secondary_name}",
                "train_start_date": train["date"].min().date().isoformat(),
                "train_end_date": train["date"].max().date().isoformat(),
                "test_start_date": test["date"].min().date().isoformat(),
                "test_end_date": test["date"].max().date().isoformat(),
                "test_month": str(months[index]),
                **_metrics_for_subset(train_subset, "train", total_count=max(len(base_train), 1)),
                **_metrics_for_subset(test_subset, "test", total_count=max(len(base_test), 1)),
            }
            row["train_score"] = (
                row["train_reversal_rate"] * row["train_share"] * (-row["train_forward_14d_median"])
            )
            month_rows.append(row)

        if not month_rows:
            continue

        month_df = pd.DataFrame(month_rows).sort_values(
            ["train_score", "train_reversal_rate", "train_forward_14d_median", "train_count"],
            ascending=[False, False, True, False],
        )
        month_df["selected"] = False
        month_df.iloc[0, month_df.columns.get_loc("selected")] = True
        result_rows.extend(month_df.to_dict("records"))
        top_rows.append(month_df.iloc[0].to_dict())

    results = pd.DataFrame(result_rows)
    top_monthly = pd.DataFrame(top_rows)
    summary = _summarize_secondary_walk_forward(top_monthly)
    return results, summary


def _summarize_secondary_walk_forward(top_monthly: pd.DataFrame) -> pd.DataFrame:
    if top_monthly.empty:
        return pd.DataFrame(
            columns=[
                "secondary_filter_name",
                "selected_count",
                "avg_test_reversal_rate",
                "median_test_forward_14d_median",
                "avg_test_forward_14d_mean",
                "total_test_count",
            ]
        )

    return (
        top_monthly.groupby("secondary_filter_name", observed=True)
        .agg(
            selected_count=("secondary_filter_name", "size"),
            avg_test_reversal_rate=("test_reversal_rate", "mean"),
            median_test_forward_14d_median=("test_forward_14d_median", "median"),
            avg_test_forward_14d_mean=("test_forward_14d_mean", "mean"),
            total_test_count=("test_count", "sum"),
        )
        .reset_index()
        .sort_values(
            ["selected_count", "avg_test_reversal_rate", "median_test_forward_14d_median"],
            ascending=[False, False, True],
        )
        .reset_index(drop=True)
    )


def evaluate_tertiary_conditions_walk_forward(
    events: pd.DataFrame,
    base_filter_names: list[str],
    min_train_months: int = 12,
    min_train_samples: int = 100,
    min_test_samples: int = 50,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = events.dropna(subset=["forward_14d_return", "reversal_14d"]).copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["month"] = frame["date"].dt.to_period("M")

    specs = _candidate_specs()
    for name in base_filter_names:
        if name not in specs:
            raise ValueError(f"Unknown base filter: {name}")

    base_rules = [specs[name] for name in base_filter_names]
    tertiary_names = [
        name for name in specs.keys()
        if name not in base_filter_names and " & " not in name
    ]

    months = sorted(frame["month"].drop_duplicates())
    result_rows: list[dict[str, object]] = []
    top_rows: list[dict[str, object]] = []

    for index in range(min_train_months, len(months)):
        train = frame.loc[frame["month"].isin(months[:index])].copy()
        test = frame.loc[frame["month"] == months[index]].copy()
        if train.empty or test.empty:
            continue

        thresholds = _build_thresholds(train)
        train_mask = pd.Series(True, index=train.index)
        test_mask = pd.Series(True, index=test.index)
        for rule in base_rules:
            train_mask &= rule(train, thresholds)
            test_mask &= rule(test, thresholds)

        base_train = train.loc[train_mask].copy()
        base_test = test.loc[test_mask].copy()
        if len(base_train) < min_train_samples or len(base_test) < min_test_samples:
            continue

        month_rows: list[dict[str, object]] = []
        for tertiary_name in tertiary_names:
            tertiary_rule = specs[tertiary_name]
            train_subset = base_train.loc[tertiary_rule(base_train, thresholds)].copy()
            test_subset = base_test.loc[tertiary_rule(base_test, thresholds)].copy()
            if len(train_subset) < min_train_samples or len(test_subset) < min_test_samples:
                continue

            combined_name = " & ".join(base_filter_names + [tertiary_name])
            row = {
                "base_filter_names": " & ".join(base_filter_names),
                "tertiary_filter_name": tertiary_name,
                "combined_filter_name": combined_name,
                "train_start_date": train["date"].min().date().isoformat(),
                "train_end_date": train["date"].max().date().isoformat(),
                "test_start_date": test["date"].min().date().isoformat(),
                "test_end_date": test["date"].max().date().isoformat(),
                "test_month": str(months[index]),
                **_metrics_for_subset(train_subset, "train", total_count=max(len(base_train), 1)),
                **_metrics_for_subset(test_subset, "test", total_count=max(len(base_test), 1)),
            }
            row["train_score"] = (
                row["train_reversal_rate"] * row["train_share"] * (-row["train_forward_14d_median"])
            )
            month_rows.append(row)

        if not month_rows:
            continue

        month_df = pd.DataFrame(month_rows).sort_values(
            ["train_score", "train_reversal_rate", "train_forward_14d_median", "train_count"],
            ascending=[False, False, True, False],
        )
        month_df["selected"] = False
        month_df.iloc[0, month_df.columns.get_loc("selected")] = True
        result_rows.extend(month_df.to_dict("records"))
        top_rows.append(month_df.iloc[0].to_dict())

    results = pd.DataFrame(result_rows)
    top_monthly = pd.DataFrame(top_rows)
    summary = _summarize_tertiary_walk_forward(top_monthly)
    return results, summary


def _summarize_tertiary_walk_forward(top_monthly: pd.DataFrame) -> pd.DataFrame:
    if top_monthly.empty:
        return pd.DataFrame(
            columns=[
                "tertiary_filter_name",
                "selected_count",
                "avg_test_reversal_rate",
                "median_test_forward_14d_median",
                "avg_test_forward_14d_mean",
                "total_test_count",
            ]
        )

    return (
        top_monthly.groupby("tertiary_filter_name", observed=True)
        .agg(
            selected_count=("tertiary_filter_name", "size"),
            avg_test_reversal_rate=("test_reversal_rate", "mean"),
            median_test_forward_14d_median=("test_forward_14d_median", "median"),
            avg_test_forward_14d_mean=("test_forward_14d_mean", "mean"),
            total_test_count=("test_count", "sum"),
        )
        .reset_index()
        .sort_values(
            ["selected_count", "avg_test_reversal_rate", "median_test_forward_14d_median"],
            ascending=[False, False, True],
        )
        .reset_index(drop=True)
    )
