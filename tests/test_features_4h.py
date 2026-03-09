import pandas as pd

from reckless_binance.features_4h import build_event_feature_panel


def test_build_event_feature_panel_joins_exchange_metrics_and_supply_snapshots():
    events = pd.DataFrame(
        [
            {
                "event_id": "DOGE|2026-01-02T00:00:00+00:00",
                "asset": "DOGE",
                "event_time": pd.Timestamp("2026-01-02 00:00:00+00:00"),
            }
        ]
    )
    timestamps = [
        pd.Timestamp("2026-01-01 00:00:00+00:00"),
        pd.Timestamp("2026-01-01 04:00:00+00:00"),
        pd.Timestamp("2026-01-01 08:00:00+00:00"),
        pd.Timestamp("2026-01-01 12:00:00+00:00"),
        pd.Timestamp("2026-01-01 16:00:00+00:00"),
        pd.Timestamp("2026-01-01 20:00:00+00:00"),
        pd.Timestamp("2026-01-02 00:00:00+00:00"),
        pd.Timestamp("2026-01-02 04:00:00+00:00"),
    ]
    closes = [0.095, 0.096, 0.097, 0.099, 0.10, 0.10, 0.11, 0.105]
    volumes = [80000.0, 82000.0, 83000.0, 85000.0, 90000.0, 100000.0, 120000.0, 90000.0]
    returns = [None]
    for prev, current in zip(closes, closes[1:]):
        returns.append(round((current / prev) - 1.0, 10))
    market_bars = pd.DataFrame(
        [
            {
                "asset": "DOGE",
                "timestamp": timestamp,
                "close": close,
                "return_4h": ret,
                "volume_quote": volume,
            }
            for timestamp, close, ret, volume in zip(timestamps, closes, returns, volumes)
        ]
    )
    binance_oi = pd.DataFrame(
        [
            {"asset": "DOGE", "timestamp": timestamp, "oi_usd": oi}
            for timestamp, oi in zip(timestamps, [80.0, 85.0, 90.0, 95.0, 98.0, 100.0, 120.0, 90.0])
        ]
    )
    bybit_oi = pd.DataFrame(
        [
            {"asset": "DOGE", "timestamp": timestamp, "oi_usd": oi}
            for timestamp, oi in zip(timestamps, [40.0, 45.0, 50.0, 55.0, 58.0, 50.0, 80.0, 60.0])
        ]
    )
    binance_funding = pd.DataFrame(
        [
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 16:00:00+00:00"), "funding_rate": 0.00005},
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 20:00:00+00:00"), "funding_rate": 0.0001},
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "funding_rate": 0.0002},
        ]
    )
    bybit_funding = pd.DataFrame(
        [
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 16:00:00+00:00"), "funding_rate": 0.00015},
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 20:00:00+00:00"), "funding_rate": 0.0002},
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "funding_rate": 0.0004},
        ]
    )
    sentiment = pd.DataFrame(
        [
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "long_short_ratio": 1.4}
        ]
    )
    supply = pd.DataFrame(
        [
            {
                "asset": "DOGE",
                "timestamp": pd.Timestamp("2026-01-01 00:00:00+00:00"),
                "circ_supply": 1000.0,
                "max_supply": 2000.0,
                "float_ratio": 0.5,
                "mcap_est": 110.0,
            }
        ]
    )

    panel = build_event_feature_panel(
        events=events,
        market_bars=market_bars,
        binance_oi=binance_oi,
        bybit_oi=bybit_oi,
        binance_funding=binance_funding,
        bybit_funding=bybit_funding,
        sentiment=sentiment,
        supply=supply,
        bars_each_side=1,
        funding_window_bars=2,
        oi_z_window=2,
    )

    assert panel["rel_bar"].tolist() == [-1, 0, 1]
    event_row = panel.loc[panel["rel_bar"] == 0].iloc[0].to_dict()
    assert event_row["oi_usd_binance"] == 120.0
    assert event_row["oi_usd_bybit"] == 80.0
    assert event_row["oi_usd_total"] == 200.0
    assert event_row["oi_share_binance"] == 0.6
    assert event_row["oi_share_bybit"] == 0.4
    assert event_row["oi_change_4h"] == round((200.0 / 150.0) - 1.0, 10)
    assert event_row["oi_change_24h"] == round((200.0 / 120.0) - 1.0, 10)
    assert event_row["oi_zscore_lookback"] == 1.0
    assert event_row["funding_binance"] == 0.0002
    assert event_row["funding_bybit"] == 0.0004
    assert event_row["funding_rate"] == 0.0003
    assert event_row["funding_rolling_mean"] == 0.000225
    assert event_row["funding_zscore"] == 1.0
    assert event_row["funding_mean_1d"] == 0.000225
    assert event_row["long_short_ratio"] == 1.4
    assert event_row["circ_supply"] == 1000.0
    assert event_row["max_supply"] == 2000.0
    assert event_row["float_ratio"] == 0.5
    assert event_row["mcap_est"] == 110.0


def test_build_event_feature_panel_handles_empty_exchange_feature_frames():
    events = pd.DataFrame(
        [
            {
                "event_id": "DOGE|2026-01-02T00:00:00+00:00",
                "asset": "DOGE",
                "event_time": pd.Timestamp("2026-01-02 00:00:00+00:00"),
            }
        ]
    )
    market_bars = pd.DataFrame(
        [
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 20:00:00+00:00"), "close": 0.10, "return_4h": 0.01, "volume_quote": 100000.0},
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "close": 0.11, "return_4h": 0.10, "volume_quote": 120000.0},
        ]
    )

    panel = build_event_feature_panel(
        events=events,
        market_bars=market_bars,
        binance_oi=pd.DataFrame(),
        bybit_oi=pd.DataFrame(),
        binance_funding=pd.DataFrame(),
        bybit_funding=pd.DataFrame(),
        sentiment=pd.DataFrame(),
        supply=pd.DataFrame(),
        bars_each_side=1,
        funding_window_bars=2,
        oi_z_window=2,
    )

    event_row = panel.loc[panel["rel_bar"] == 0].iloc[0].to_dict()
    assert event_row["oi_usd_total"] == 0.0
    assert pd.isna(event_row["funding_rate"])
