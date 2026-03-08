import pandas as pd

from reckless_binance.events import attach_forward_returns


def test_forward_returns_cover_day_1_through_day_14():
    prices = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=20, freq="D"),
            "symbol": ["AAAUSDT"] * 20,
            "close": [100 + i for i in range(20)],
        }
    )
    events = pd.DataFrame([{"date": pd.Timestamp("2026-01-06"), "symbol": "AAAUSDT"}])

    result = attach_forward_returns(events, prices, lookback_days=14, lookforward_days=14)

    assert "ret_+14" in result.columns
    assert result.loc[0, "ret_+1"] == (106 / 105) - 1
