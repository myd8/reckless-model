import pandas as pd

from reckless_binance.labels import build_event_labels, summarize_labels


def _panel_rows(event_id: str, closes: list[float], oi_change: float, funding: float, float_ratio: float) -> list[dict]:
    event_time = pd.Timestamp("2026-01-01 00:00:00+00:00")
    rows = []
    for rel_bar, close in enumerate(closes):
        rel_bar = rel_bar - 14
        rows.append(
            {
                "event_id": event_id,
                "asset": event_id.split("|")[0],
                "event_time": event_time,
                "timestamp": event_time + pd.Timedelta(days=rel_bar),
                "rel_bar": rel_bar,
                "close": close,
                "oi_change_4h_total": oi_change if rel_bar == 0 else None,
                "funding_mean_1d": funding if rel_bar == 0 else None,
                "float_ratio": float_ratio if rel_bar == 0 else None,
            }
        )
    return rows


def test_build_event_labels_assigns_priority_classes():
    panel = pd.DataFrame(
        _panel_rows("AAA|e1", [80] * 14 + [100, 90, 88, 85, 82, 80, 78, 70, 68, 66, 64, 62, 61, 60, 59], 0.1, 0.001, 0.4)
        + _panel_rows("BBB|e2", [80] * 14 + [100, 106, 104, 100, 112, 108, 105, 103, 100, 98, 96, 94, 92, 91, 90], 0.2, 0.002, 0.5)
        + _panel_rows("CCC|e3", [80] * 14 + [100, 106, 108, 110, 112, 115, 118, 120, 122, 124, 125, 124, 123, 122, 121], 0.3, 0.003, 0.6)
        + _panel_rows("DDD|e4", [80] * 14 + [100, 102, 101, 99, 100, 100, 101, 100, 99, 100, 100, 100, 99, 100, 100], 0.4, 0.004, 0.7)
    )

    labels = build_event_labels(panel, bars_per_day=1)

    assert labels.set_index("event_id")["label"].to_dict() == {
        "AAA|e1": "immediate_reversal",
        "BBB|e2": "delayed_blowoff",
        "CCC|e3": "breakout",
        "DDD|e4": "unclassified",
    }


def test_summarize_labels_compares_event_time_features():
    labels = pd.DataFrame(
        [
            {"event_id": "AAA|e1", "label": "immediate_reversal", "ret_3d": -0.12, "ret_7d": -0.30, "ret_14d": -0.41},
            {"event_id": "BBB|e2", "label": "breakout", "ret_3d": 0.08, "ret_7d": 0.20, "ret_14d": 0.21},
        ]
    )
    panel = pd.DataFrame(
        [
            {"event_id": "AAA|e1", "rel_bar": 0, "oi_change_4h_total": 0.10, "funding_mean_1d": 0.001, "float_ratio": 0.4},
            {"event_id": "BBB|e2", "rel_bar": 0, "oi_change_4h_total": 0.30, "funding_mean_1d": 0.003, "float_ratio": 0.6},
        ]
    )

    summary = summarize_labels(labels, panel)

    assert summary.to_dict("records") == [
        {
            "label": "breakout",
            "count": 1,
            "median_ret_3d": 0.08,
            "median_ret_7d": 0.2,
            "median_ret_14d": 0.21,
            "median_oi_change_4h_total": 0.3,
            "median_funding_at_event": 0.003,
            "median_float_ratio": 0.6,
        },
        {
            "label": "immediate_reversal",
            "count": 1,
            "median_ret_3d": -0.12,
            "median_ret_7d": -0.3,
            "median_ret_14d": -0.41,
            "median_oi_change_4h_total": 0.1,
            "median_funding_at_event": 0.001,
            "median_float_ratio": 0.4,
        },
    ]
