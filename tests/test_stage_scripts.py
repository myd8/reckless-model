from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


def _load_script_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_build_top_gainer_events_script_writes_parquet(tmp_path):
    asset_map_path = tmp_path / "asset_map.parquet"
    binance_klines_path = tmp_path / "binance_klines.parquet"
    bybit_klines_path = tmp_path / "bybit_klines.parquet"
    output_path = tmp_path / "top_gainer_events.parquet"

    pd.DataFrame([{"asset": "DOGE", "binance_symbol": "DOGEUSDT", "bybit_symbol": "DOGEUSDT"}]).to_parquet(asset_map_path)
    pd.DataFrame(
        [
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 00:00:00+00:00"), "close": 0.10, "volume_quote": 100.0},
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 04:00:00+00:00"), "close": 0.12, "volume_quote": 120.0},
        ]
    ).to_parquet(binance_klines_path)
    pd.DataFrame(
        [
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 00:00:00+00:00"), "close": 0.10, "volume_quote": 90.0},
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 04:00:00+00:00"), "close": 0.13, "volume_quote": 110.0},
        ]
    ).to_parquet(bybit_klines_path)

    module = _load_script_module(Path("scripts/build_top_gainer_events_4h.py"), "build_top_gainer_events_4h")
    module.main(
        [
            "--asset-map-path",
            str(asset_map_path),
            "--binance-klines-path",
            str(binance_klines_path),
            "--bybit-klines-path",
            str(bybit_klines_path),
            "--output-path",
            str(output_path),
            "--top-n",
            "1",
            "--min-volume-quote",
            "1",
        ]
    )

    events = pd.read_parquet(output_path)
    assert events["asset"].tolist() == ["DOGE"]


def test_build_event_features_script_writes_panel(tmp_path):
    events_path = tmp_path / "top_gainer_events.parquet"
    market_bars_path = tmp_path / "market_bars.parquet"
    binance_oi_path = tmp_path / "binance_oi.parquet"
    bybit_oi_path = tmp_path / "bybit_oi.parquet"
    binance_funding_path = tmp_path / "binance_funding.parquet"
    bybit_funding_path = tmp_path / "bybit_funding.parquet"
    sentiment_path = tmp_path / "sentiment.parquet"
    supply_path = tmp_path / "supply.parquet"
    output_path = tmp_path / "event_features_4h.parquet"

    pd.DataFrame(
        [{"event_id": "DOGE|2026-01-02T00:00:00+00:00", "asset": "DOGE", "event_time": pd.Timestamp("2026-01-02 00:00:00+00:00")}]
    ).to_parquet(events_path)
    pd.DataFrame(
        [
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 20:00:00+00:00"), "close": 0.10, "return_4h": 0.01, "volume_quote": 100000.0},
            {"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "close": 0.11, "return_4h": 0.10, "volume_quote": 120000.0},
        ]
    ).to_parquet(market_bars_path)
    pd.DataFrame(
        [{"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "oi_usd": 120.0}]
    ).to_parquet(binance_oi_path)
    pd.DataFrame(
        [{"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "oi_usd": 80.0}]
    ).to_parquet(bybit_oi_path)
    pd.DataFrame(
        [{"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "funding_rate": 0.0002}]
    ).to_parquet(binance_funding_path)
    pd.DataFrame(
        [{"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "funding_rate": 0.0004}]
    ).to_parquet(bybit_funding_path)
    pd.DataFrame(
        [{"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "long_short_ratio": 1.4}]
    ).to_parquet(sentiment_path)
    pd.DataFrame(
        [{"asset": "DOGE", "timestamp": pd.Timestamp("2026-01-01 00:00:00+00:00"), "circ_supply": 1000.0, "max_supply": 2000.0, "float_ratio": 0.5, "mcap_est": 110.0}]
    ).to_parquet(supply_path)

    module = _load_script_module(Path("scripts/build_event_features_4h.py"), "build_event_features_4h")
    module.main(
        [
            "--events-path",
            str(events_path),
            "--market-bars-path",
            str(market_bars_path),
            "--binance-oi-path",
            str(binance_oi_path),
            "--bybit-oi-path",
            str(bybit_oi_path),
            "--binance-funding-path",
            str(binance_funding_path),
            "--bybit-funding-path",
            str(bybit_funding_path),
            "--sentiment-path",
            str(sentiment_path),
            "--supply-path",
            str(supply_path),
            "--output-path",
            str(output_path),
            "--bars-each-side",
            "1",
        ]
    )

    panel = pd.read_parquet(output_path)
    assert panel["event_id"].tolist() == ["DOGE|2026-01-02T00:00:00+00:00", "DOGE|2026-01-02T00:00:00+00:00"]


def test_build_event_labels_script_writes_labels_and_summary(tmp_path):
    feature_path = tmp_path / "event_features_4h.parquet"
    labels_path = tmp_path / "event_labels.parquet"
    summary_path = tmp_path / "breakout_blowoff_label_summary.csv"

    pd.DataFrame(
        [
            {"event_id": "AAA|e1", "asset": "AAA", "event_time": pd.Timestamp("2026-01-01 00:00:00+00:00"), "timestamp": pd.Timestamp("2026-01-01 00:00:00+00:00"), "rel_bar": 0, "close": 100.0, "oi_change_4h_total": 0.1, "funding_mean_1d": 0.001, "float_ratio": 0.4},
            {"event_id": "AAA|e1", "asset": "AAA", "event_time": pd.Timestamp("2026-01-01 00:00:00+00:00"), "timestamp": pd.Timestamp("2026-01-02 00:00:00+00:00"), "rel_bar": 1, "close": 90.0, "oi_change_4h_total": None, "funding_mean_1d": None, "float_ratio": None},
            {"event_id": "AAA|e1", "asset": "AAA", "event_time": pd.Timestamp("2026-01-01 00:00:00+00:00"), "timestamp": pd.Timestamp("2026-01-04 00:00:00+00:00"), "rel_bar": 3, "close": 88.0, "oi_change_4h_total": None, "funding_mean_1d": None, "float_ratio": None},
            {"event_id": "AAA|e1", "asset": "AAA", "event_time": pd.Timestamp("2026-01-01 00:00:00+00:00"), "timestamp": pd.Timestamp("2026-01-08 00:00:00+00:00"), "rel_bar": 7, "close": 70.0, "oi_change_4h_total": None, "funding_mean_1d": None, "float_ratio": None},
            {"event_id": "AAA|e1", "asset": "AAA", "event_time": pd.Timestamp("2026-01-01 00:00:00+00:00"), "timestamp": pd.Timestamp("2026-01-15 00:00:00+00:00"), "rel_bar": 14, "close": 60.0, "oi_change_4h_total": None, "funding_mean_1d": None, "float_ratio": None},
        ]
    ).to_parquet(feature_path)

    module = _load_script_module(Path("scripts/build_event_labels.py"), "build_event_labels")
    module.main(
        [
            "--feature-path",
            str(feature_path),
            "--labels-path",
            str(labels_path),
            "--summary-path",
            str(summary_path),
            "--bars-per-day",
            "1",
        ]
    )

    labels = pd.read_parquet(labels_path)
    summary = pd.read_csv(summary_path)
    assert labels["label"].tolist() == ["immediate_reversal"]
    assert summary["label"].tolist() == ["immediate_reversal"]


def test_fetch_historical_script_delegates_to_ingestion(monkeypatch, tmp_path):
    calls = {}

    def fake_fetch_all_to_parquet(**kwargs):
        calls.update(kwargs)

    module = _load_script_module(Path("scripts/fetch_historical_4h.py"), "fetch_historical_4h")
    monkeypatch.setattr(module, "fetch_all_to_parquet", fake_fetch_all_to_parquet)

    module.main(
        [
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-01-31",
            "--output-root",
            str(tmp_path),
            "--limit-assets",
            "5",
        ]
    )

    assert calls["start_date"] == "2025-01-01"
    assert calls["end_date"] == "2025-01-31"
    assert calls["output_root"] == tmp_path
    assert calls["limit_assets"] == 5
