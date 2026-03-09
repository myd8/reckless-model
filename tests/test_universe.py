import pandas as pd

from reckless_binance.universe import eligible_on_date, with_days_since_listing


def test_symbol_is_ineligible_before_onboard_date():
    row = {"symbol": "AAAUSDT", "onboard_date": pd.Timestamp("2026-01-10")}

    assert eligible_on_date(row, pd.Timestamp("2026-01-09")) is False
    assert eligible_on_date(row, pd.Timestamp("2026-01-10")) is True


def test_with_days_since_listing_annotates_active_rows():
    frame = pd.DataFrame(
        [
            {"symbol": "AAAUSDT", "date": "2026-01-10", "onboard_date": "2026-01-08"},
            {"symbol": "AAAUSDT", "date": "2026-01-09", "onboard_date": "2026-01-10"},
        ]
    )

    result = with_days_since_listing(frame)

    assert list(result["days_since_listing"]) == [2, -1]
