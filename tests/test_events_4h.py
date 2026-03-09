import pandas as pd

from reckless_binance.events_4h import build_top_gainer_events


def test_build_top_gainer_events_ranks_by_4h_return_descending():
    frame = pd.DataFrame(
        [
            {"asset": "AAA", "timestamp": "2026-01-01T00:00:00Z", "close": 100.0, "volume_quote": 20000.0},
            {"asset": "AAA", "timestamp": "2026-01-01T04:00:00Z", "close": 120.0, "volume_quote": 22000.0},
            {"asset": "AAA", "timestamp": "2026-01-01T08:00:00Z", "close": 121.0, "volume_quote": 23000.0},
            {"asset": "BBB", "timestamp": "2026-01-01T00:00:00Z", "close": 100.0, "volume_quote": 21000.0},
            {"asset": "BBB", "timestamp": "2026-01-01T04:00:00Z", "close": 105.0, "volume_quote": 20500.0},
            {"asset": "BBB", "timestamp": "2026-01-01T08:00:00Z", "close": 110.0, "volume_quote": 24000.0},
            {"asset": "CCC", "timestamp": "2026-01-01T00:00:00Z", "close": 100.0, "volume_quote": 22000.0},
            {"asset": "CCC", "timestamp": "2026-01-01T04:00:00Z", "close": 90.0, "volume_quote": 22500.0},
            {"asset": "CCC", "timestamp": "2026-01-01T08:00:00Z", "close": 130.0, "volume_quote": 26000.0},
        ]
    )

    events = build_top_gainer_events(frame, top_n=1, min_volume_quote=10_000.0)

    assert events[["asset", "event_time", "ret_4h", "rank_ret_4h"]].to_dict("records") == [
        {
            "asset": "AAA",
            "event_time": pd.Timestamp("2026-01-01 04:00:00+00:00"),
            "ret_4h": 0.2,
            "rank_ret_4h": 1,
        },
        {
            "asset": "CCC",
            "event_time": pd.Timestamp("2026-01-01 08:00:00+00:00"),
            "ret_4h": round((130.0 / 90.0) - 1.0, 10),
            "rank_ret_4h": 1,
        },
    ]


def test_build_top_gainer_events_filters_low_liquidity_before_ranking():
    frame = pd.DataFrame(
        [
            {"asset": "AAA", "timestamp": "2026-01-01T00:00:00Z", "close": 100.0, "volume_quote": 20000.0},
            {"asset": "AAA", "timestamp": "2026-01-01T04:00:00Z", "close": 102.0, "volume_quote": 20000.0},
            {"asset": "BBB", "timestamp": "2026-01-01T00:00:00Z", "close": 100.0, "volume_quote": 100.0},
            {"asset": "BBB", "timestamp": "2026-01-01T04:00:00Z", "close": 140.0, "volume_quote": 100.0},
            {"asset": "CCC", "timestamp": "2026-01-01T00:00:00Z", "close": 100.0, "volume_quote": 21000.0},
            {"asset": "CCC", "timestamp": "2026-01-01T04:00:00Z", "close": 101.0, "volume_quote": 21000.0},
        ]
    )

    events = build_top_gainer_events(frame, top_n=2, min_volume_quote=10_000.0)

    assert events["asset"].tolist() == ["AAA", "CCC"]
    assert events["rank_ret_4h"].tolist() == [1, 2]


def test_build_top_gainer_events_adds_event_id_and_prior_entry_count():
    frame = pd.DataFrame(
        [
            {"asset": "AAA", "timestamp": "2026-01-01T00:00:00Z", "close": 100.0, "volume_quote": 20000.0},
            {"asset": "AAA", "timestamp": "2026-01-01T04:00:00Z", "close": 120.0, "volume_quote": 22000.0},
            {"asset": "AAA", "timestamp": "2026-01-01T08:00:00Z", "close": 100.0, "volume_quote": 21000.0},
            {"asset": "AAA", "timestamp": "2026-01-01T12:00:00Z", "close": 130.0, "volume_quote": 25000.0},
            {"asset": "BBB", "timestamp": "2026-01-01T00:00:00Z", "close": 100.0, "volume_quote": 20000.0},
            {"asset": "BBB", "timestamp": "2026-01-01T04:00:00Z", "close": 90.0, "volume_quote": 22000.0},
            {"asset": "BBB", "timestamp": "2026-01-01T08:00:00Z", "close": 89.0, "volume_quote": 22000.0},
            {"asset": "BBB", "timestamp": "2026-01-01T12:00:00Z", "close": 88.0, "volume_quote": 22000.0},
        ]
    )

    events = build_top_gainer_events(frame, top_n=1, min_volume_quote=10_000.0)

    assert events["event_id"].tolist() == [
        "AAA|2026-01-01T04:00:00+00:00",
        "BBB|2026-01-01T08:00:00+00:00",
        "AAA|2026-01-01T12:00:00+00:00",
    ]
    assert events["prior_top_gainer_entries_30d"].tolist() == [0, 0, 1]


def test_build_top_gainer_events_defaults_to_top_10():
    rows = []
    for index in range(12):
        asset = f"A{index:02d}"
        rows.append({"asset": asset, "timestamp": "2026-01-01T00:00:00Z", "close": 100.0, "volume_quote": 20_000.0})
        rows.append(
            {
                "asset": asset,
                "timestamp": "2026-01-01T04:00:00Z",
                "close": 100.0 + index,
                "volume_quote": 20_000.0 + index,
            }
        )
    frame = pd.DataFrame(rows)

    events = build_top_gainer_events(frame)

    assert len(events) == 10
    assert events["rank_ret_4h"].tolist() == list(range(1, 11))
