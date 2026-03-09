import pandas as pd

from reckless_binance.signals import build_signal_table, summarize_signal_candidates


def test_build_signal_table_adds_rule_flags_and_score():
    events = pd.DataFrame(
        [
            {"date": "2024-01-01", "symbol": "A", "ret_7d": 0.10, "rank_7d": 5, "days_since_listing": 300, "trade_count": 10, "quote_volume": 10, "prior_top20_entries_30d": 0, "forward_14d_return": 0.01, "reversal_14d": 0},
            {"date": "2024-01-02", "symbol": "B", "ret_7d": 0.50, "rank_7d": 2, "days_since_listing": 100, "trade_count": 100, "quote_volume": 100, "prior_top20_entries_30d": 1, "forward_14d_return": -0.05, "reversal_14d": 1},
        ]
    )

    signal_table, candidates = build_signal_table(events)

    assert "trade_count_top_half" in signal_table.columns
    assert "quote_vol_top_half" in signal_table.columns
    assert "ret_7d_top_half" in signal_table.columns
    assert "signal_strength_score" in signal_table.columns
    assert "signal_active" in signal_table.columns
    assert signal_table.loc[signal_table["symbol"] == "B", "signal_strength_score"].item() == 3
    assert list(candidates["symbol"]) == ["B"]


def test_summarize_signal_candidates_reports_core_metrics():
    candidates = pd.DataFrame(
        [
            {"symbol": "A", "forward_14d_return": -0.10, "reversal_14d": 1},
            {"symbol": "B", "forward_14d_return": 0.05, "reversal_14d": 0},
        ]
    )

    summary = summarize_signal_candidates(candidates)

    assert summary["signal_count"] == 2
    assert summary["reversal_rate"] == 0.5
    assert summary["median_forward_14d_return"] == -0.025
