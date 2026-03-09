import pandas as pd

from reckless_binance.filter_analysis import evaluate_candidate_filters, split_events_train_test


def test_split_events_train_test_is_chronological():
    events = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=6, freq="D"),
            "symbol": ["AAAUSDT"] * 6,
            "ret_7d": [0.1] * 6,
            "days_since_listing": [100] * 6,
            "quote_volume": [10] * 6,
            "trade_count": [10] * 6,
            "prior_top20_entries_30d": [0] * 6,
            "forward_14d_return": [-0.01] * 6,
            "reversal_14d": [1] * 6,
        }
    )

    train, test = split_events_train_test(events, train_fraction=2 / 3)

    assert list(train["date"].dt.strftime("%Y-%m-%d")) == ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
    assert list(test["date"].dt.strftime("%Y-%m-%d")) == ["2024-01-05", "2024-01-06"]


def test_evaluate_candidate_filters_returns_train_and_test_metrics():
    events = pd.DataFrame(
        [
            {"date": "2024-01-01", "symbol": "A", "ret_7d": 0.10, "days_since_listing": 300, "quote_volume": 10, "trade_count": 10, "prior_top20_entries_30d": 0, "forward_14d_return": 0.02, "reversal_14d": 0},
            {"date": "2024-01-02", "symbol": "B", "ret_7d": 0.20, "days_since_listing": 250, "quote_volume": 20, "trade_count": 20, "prior_top20_entries_30d": 0, "forward_14d_return": -0.01, "reversal_14d": 1},
            {"date": "2024-01-03", "symbol": "C", "ret_7d": 0.30, "days_since_listing": 200, "quote_volume": 30, "trade_count": 30, "prior_top20_entries_30d": 1, "forward_14d_return": -0.03, "reversal_14d": 1},
            {"date": "2024-01-04", "symbol": "D", "ret_7d": 0.40, "days_since_listing": 150, "quote_volume": 40, "trade_count": 40, "prior_top20_entries_30d": 1, "forward_14d_return": -0.05, "reversal_14d": 1},
            {"date": "2024-01-05", "symbol": "E", "ret_7d": 0.50, "days_since_listing": 120, "quote_volume": 50, "trade_count": 50, "prior_top20_entries_30d": 1, "forward_14d_return": -0.04, "reversal_14d": 1},
            {"date": "2024-01-06", "symbol": "F", "ret_7d": 0.60, "days_since_listing": 90, "quote_volume": 60, "trade_count": 60, "prior_top20_entries_30d": 1, "forward_14d_return": -0.02, "reversal_14d": 1},
        ]
    )

    result = evaluate_candidate_filters(events, train_fraction=2 / 3, min_train_samples=1, min_test_samples=1)

    assert "filter_name" in result.columns
    assert "train_reversal_rate" in result.columns
    assert "test_reversal_rate" in result.columns
    assert "ret_7d_top_quartile" in set(result["filter_name"])
