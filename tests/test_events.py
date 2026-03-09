import pandas as pd

from reckless_binance.events import build_top20_entry_events


def test_event_only_triggers_on_first_day_of_top20_entry():
    frame = pd.DataFrame(
        [
            {"date": "2026-01-08", "symbol": "AAAUSDT", "ret_7d": 0.50, "rank_7d": 5},
            {"date": "2026-01-09", "symbol": "AAAUSDT", "ret_7d": 0.48, "rank_7d": 7},
            {"date": "2026-01-10", "symbol": "AAAUSDT", "ret_7d": 0.20, "rank_7d": 22},
            {"date": "2026-01-11", "symbol": "AAAUSDT", "ret_7d": 0.41, "rank_7d": 8},
        ]
    )

    events = build_top20_entry_events(frame, top_n=20)

    assert list(events["date"]) == ["2026-01-08", "2026-01-11"]


def test_event_features_capture_current_streak_and_recent_entries():
    frame = pd.DataFrame(
        [
            {"date": "2026-01-01", "symbol": "AAAUSDT", "ret_7d": 0.10, "rank_7d": 30},
            {"date": "2026-01-02", "symbol": "AAAUSDT", "ret_7d": 0.35, "rank_7d": 12},
            {"date": "2026-01-03", "symbol": "AAAUSDT", "ret_7d": 0.37, "rank_7d": 10},
            {"date": "2026-01-04", "symbol": "AAAUSDT", "ret_7d": 0.08, "rank_7d": 25},
            {"date": "2026-01-05", "symbol": "AAAUSDT", "ret_7d": 0.40, "rank_7d": 7},
            {"date": "2026-01-06", "symbol": "AAAUSDT", "ret_7d": 0.41, "rank_7d": 5},
        ]
    )

    events = build_top20_entry_events(frame, top_n=20)

    assert list(events["top20_streak_length"]) == [1, 1]
    assert list(events["prior_top20_entries_30d"]) == [0, 1]
