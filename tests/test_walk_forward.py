import pandas as pd

from reckless_binance.walk_forward import (
    evaluate_monthly_walk_forward,
    evaluate_secondary_conditions_walk_forward,
    evaluate_tertiary_conditions_walk_forward,
)


def test_monthly_walk_forward_uses_prior_months_only():
    events = pd.DataFrame(
        [
            {"date": "2024-01-05", "symbol": "A", "ret_7d": 0.10, "days_since_listing": 300, "quote_volume": 10, "trade_count": 10, "prior_top20_entries_30d": 0, "forward_14d_return": 0.02, "reversal_14d": 0},
            {"date": "2024-01-20", "symbol": "B", "ret_7d": 0.20, "days_since_listing": 280, "quote_volume": 20, "trade_count": 20, "prior_top20_entries_30d": 0, "forward_14d_return": -0.01, "reversal_14d": 1},
            {"date": "2024-02-07", "symbol": "C", "ret_7d": 0.30, "days_since_listing": 250, "quote_volume": 30, "trade_count": 30, "prior_top20_entries_30d": 1, "forward_14d_return": -0.03, "reversal_14d": 1},
            {"date": "2024-02-22", "symbol": "D", "ret_7d": 0.40, "days_since_listing": 230, "quote_volume": 40, "trade_count": 40, "prior_top20_entries_30d": 1, "forward_14d_return": -0.04, "reversal_14d": 1},
            {"date": "2024-03-08", "symbol": "E", "ret_7d": 0.50, "days_since_listing": 200, "quote_volume": 50, "trade_count": 50, "prior_top20_entries_30d": 1, "forward_14d_return": -0.05, "reversal_14d": 1},
            {"date": "2024-03-25", "symbol": "F", "ret_7d": 0.60, "days_since_listing": 180, "quote_volume": 60, "trade_count": 60, "prior_top20_entries_30d": 1, "forward_14d_return": -0.02, "reversal_14d": 1},
            {"date": "2024-04-09", "symbol": "G", "ret_7d": 0.35, "days_since_listing": 210, "quote_volume": 35, "trade_count": 35, "prior_top20_entries_30d": 1, "forward_14d_return": -0.01, "reversal_14d": 1},
            {"date": "2024-04-22", "symbol": "H", "ret_7d": 0.15, "days_since_listing": 310, "quote_volume": 15, "trade_count": 15, "prior_top20_entries_30d": 0, "forward_14d_return": 0.03, "reversal_14d": 0},
        ]
    )

    results, top_filters, summary = evaluate_monthly_walk_forward(
        events,
        min_train_months=2,
        min_train_samples=1,
        min_test_samples=1,
    )

    assert set(results["test_month"]) == {"2024-03", "2024-04"}
    assert set(top_filters["test_month"]) == {"2024-03", "2024-04"}
    assert "selected_count" in summary.columns
    assert (results["train_end_date"] < results["test_start_date"]).all()


def test_monthly_walk_forward_returns_selected_top_filter_per_month():
    events = pd.DataFrame(
        [
            {"date": "2024-01-05", "symbol": "A", "ret_7d": 0.10, "days_since_listing": 300, "quote_volume": 10, "trade_count": 10, "prior_top20_entries_30d": 0, "forward_14d_return": 0.02, "reversal_14d": 0},
            {"date": "2024-01-20", "symbol": "B", "ret_7d": 0.20, "days_since_listing": 280, "quote_volume": 20, "trade_count": 20, "prior_top20_entries_30d": 0, "forward_14d_return": -0.01, "reversal_14d": 1},
            {"date": "2024-02-07", "symbol": "C", "ret_7d": 0.30, "days_since_listing": 250, "quote_volume": 30, "trade_count": 30, "prior_top20_entries_30d": 1, "forward_14d_return": -0.03, "reversal_14d": 1},
            {"date": "2024-02-22", "symbol": "D", "ret_7d": 0.40, "days_since_listing": 230, "quote_volume": 40, "trade_count": 40, "prior_top20_entries_30d": 1, "forward_14d_return": -0.04, "reversal_14d": 1},
            {"date": "2024-03-08", "symbol": "E", "ret_7d": 0.50, "days_since_listing": 200, "quote_volume": 50, "trade_count": 50, "prior_top20_entries_30d": 1, "forward_14d_return": -0.05, "reversal_14d": 1},
            {"date": "2024-03-25", "symbol": "F", "ret_7d": 0.60, "days_since_listing": 180, "quote_volume": 60, "trade_count": 60, "prior_top20_entries_30d": 1, "forward_14d_return": -0.02, "reversal_14d": 1},
            {"date": "2024-04-09", "symbol": "G", "ret_7d": 0.35, "days_since_listing": 210, "quote_volume": 35, "trade_count": 35, "prior_top20_entries_30d": 1, "forward_14d_return": -0.01, "reversal_14d": 1},
            {"date": "2024-04-22", "symbol": "H", "ret_7d": 0.15, "days_since_listing": 310, "quote_volume": 15, "trade_count": 15, "prior_top20_entries_30d": 0, "forward_14d_return": 0.03, "reversal_14d": 0},
        ]
    )

    _, top_filters, summary = evaluate_monthly_walk_forward(
        events,
        min_train_months=2,
        min_train_samples=1,
        min_test_samples=1,
    )

    assert len(top_filters) == 2
    assert top_filters["filter_name"].notna().all()
    assert summary["selected_count"].sum() == 2


def test_secondary_conditions_walk_forward_returns_ranked_summary():
    events = pd.DataFrame(
        [
            {"date": "2024-01-05", "symbol": "A", "ret_7d": 0.10, "days_since_listing": 300, "quote_volume": 10, "trade_count": 10, "prior_top20_entries_30d": 0, "forward_14d_return": 0.02, "reversal_14d": 0},
            {"date": "2024-01-20", "symbol": "B", "ret_7d": 0.20, "days_since_listing": 280, "quote_volume": 20, "trade_count": 20, "prior_top20_entries_30d": 0, "forward_14d_return": -0.01, "reversal_14d": 1},
            {"date": "2024-02-07", "symbol": "C", "ret_7d": 0.30, "days_since_listing": 250, "quote_volume": 30, "trade_count": 30, "prior_top20_entries_30d": 1, "forward_14d_return": -0.03, "reversal_14d": 1},
            {"date": "2024-02-22", "symbol": "D", "ret_7d": 0.40, "days_since_listing": 230, "quote_volume": 40, "trade_count": 40, "prior_top20_entries_30d": 1, "forward_14d_return": -0.04, "reversal_14d": 1},
            {"date": "2024-03-08", "symbol": "E", "ret_7d": 0.50, "days_since_listing": 200, "quote_volume": 50, "trade_count": 50, "prior_top20_entries_30d": 1, "forward_14d_return": -0.05, "reversal_14d": 1},
            {"date": "2024-03-25", "symbol": "F", "ret_7d": 0.60, "days_since_listing": 180, "quote_volume": 60, "trade_count": 60, "prior_top20_entries_30d": 1, "forward_14d_return": -0.02, "reversal_14d": 1},
            {"date": "2024-04-09", "symbol": "G", "ret_7d": 0.35, "days_since_listing": 210, "quote_volume": 35, "trade_count": 35, "prior_top20_entries_30d": 1, "forward_14d_return": -0.01, "reversal_14d": 1},
            {"date": "2024-04-22", "symbol": "H", "ret_7d": 0.15, "days_since_listing": 310, "quote_volume": 15, "trade_count": 15, "prior_top20_entries_30d": 0, "forward_14d_return": 0.03, "reversal_14d": 0},
        ]
    )

    results, summary = evaluate_secondary_conditions_walk_forward(
        events,
        base_filter_name="trade_count_top_half",
        min_train_months=2,
        min_train_samples=1,
        min_test_samples=1,
    )

    assert "secondary_filter_name" in results.columns
    assert "combined_filter_name" in results.columns
    assert "selected_count" in summary.columns
    assert summary["secondary_filter_name"].notna().all()
    assert "trade_count_top_half" not in set(summary["secondary_filter_name"])


def test_tertiary_conditions_walk_forward_returns_ranked_summary():
    events = pd.DataFrame(
        [
            {"date": "2024-01-05", "symbol": "A", "ret_7d": 0.10, "days_since_listing": 300, "quote_volume": 10, "trade_count": 10, "prior_top20_entries_30d": 0, "forward_14d_return": 0.02, "reversal_14d": 0},
            {"date": "2024-01-20", "symbol": "B", "ret_7d": 0.20, "days_since_listing": 280, "quote_volume": 20, "trade_count": 20, "prior_top20_entries_30d": 0, "forward_14d_return": -0.01, "reversal_14d": 1},
            {"date": "2024-02-07", "symbol": "C", "ret_7d": 0.30, "days_since_listing": 250, "quote_volume": 30, "trade_count": 30, "prior_top20_entries_30d": 1, "forward_14d_return": -0.03, "reversal_14d": 1},
            {"date": "2024-02-22", "symbol": "D", "ret_7d": 0.40, "days_since_listing": 230, "quote_volume": 40, "trade_count": 40, "prior_top20_entries_30d": 1, "forward_14d_return": -0.04, "reversal_14d": 1},
            {"date": "2024-03-08", "symbol": "E", "ret_7d": 0.50, "days_since_listing": 200, "quote_volume": 50, "trade_count": 50, "prior_top20_entries_30d": 1, "forward_14d_return": -0.05, "reversal_14d": 1},
            {"date": "2024-03-25", "symbol": "F", "ret_7d": 0.60, "days_since_listing": 180, "quote_volume": 60, "trade_count": 60, "prior_top20_entries_30d": 1, "forward_14d_return": -0.02, "reversal_14d": 1},
            {"date": "2024-04-09", "symbol": "G", "ret_7d": 0.35, "days_since_listing": 210, "quote_volume": 35, "trade_count": 35, "prior_top20_entries_30d": 1, "forward_14d_return": -0.01, "reversal_14d": 1},
            {"date": "2024-04-22", "symbol": "H", "ret_7d": 0.15, "days_since_listing": 310, "quote_volume": 15, "trade_count": 15, "prior_top20_entries_30d": 0, "forward_14d_return": 0.03, "reversal_14d": 0},
        ]
    )

    results, summary = evaluate_tertiary_conditions_walk_forward(
        events,
        base_filter_names=["trade_count_top_half", "quote_vol_top_half"],
        min_train_months=2,
        min_train_samples=1,
        min_test_samples=1,
    )

    assert "tertiary_filter_name" in results.columns
    assert "combined_filter_name" in results.columns
    assert "selected_count" in summary.columns
    assert summary["tertiary_filter_name"].notna().all()
