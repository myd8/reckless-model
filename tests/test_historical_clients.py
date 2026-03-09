import pandas as pd

from reckless_binance.binance_historical import (
    fetch_4h_klines as fetch_binance_4h_klines,
    parse_4h_klines as parse_binance_4h_klines,
    parse_funding_history as parse_binance_funding_history,
    parse_open_interest_history as parse_binance_open_interest_history,
)
from reckless_binance.bybit_historical import (
    fetch_4h_klines as fetch_bybit_4h_klines,
    parse_4h_klines as parse_bybit_4h_klines,
    parse_funding_history as parse_bybit_funding_history,
    parse_long_short_ratio_history,
    parse_open_interest_history as parse_bybit_open_interest_history,
)
from reckless_binance.supply_historical import parse_supply_snapshot
from reckless_binance.supply_historical import build_supply_history_frame


def test_parse_binance_4h_klines_normalizes_numeric_columns():
    payload = [
        [
            1710000000000,
            "0.10",
            "0.12",
            "0.09",
            "0.11",
            "1000",
            1710014399999,
            "110000",
            1234,
            "600",
            "66000",
            "0",
        ]
    ]

    frame = parse_binance_4h_klines(symbol="DOGEUSDT", payload=payload)

    assert frame.to_dict("records") == [
        {
            "symbol": "DOGEUSDT",
            "timestamp": pd.Timestamp("2024-03-09 16:00:00+00:00"),
            "close": 0.11,
            "return_4h": None,
            "volume_quote": 110000.0,
        }
    ]


def test_parse_binance_open_interest_history_keeps_usd_notional():
    payload = [
        {
            "symbol": "DOGEUSDT",
            "sumOpenInterest": "2000000",
            "sumOpenInterestValue": "220000",
            "timestamp": 1710000000000,
        }
    ]

    frame = parse_binance_open_interest_history(payload)

    assert frame.to_dict("records") == [
        {
            "symbol": "DOGEUSDT",
            "timestamp": pd.Timestamp("2024-03-09 16:00:00+00:00"),
            "oi_contracts": 2000000.0,
            "oi_usd": 220000.0,
        }
    ]


def test_parse_binance_funding_history_converts_rates():
    payload = [
        {
            "symbol": "DOGEUSDT",
            "fundingRate": "0.0001",
            "fundingTime": 1710000000000,
        }
    ]

    frame = parse_binance_funding_history(payload)

    assert frame.to_dict("records") == [
        {
            "symbol": "DOGEUSDT",
            "timestamp": pd.Timestamp("2024-03-09 16:00:00+00:00"),
            "funding_rate": 0.0001,
        }
    ]


def test_parse_bybit_4h_klines_sorts_oldest_first_and_computes_returns():
    payload = {
        "result": {
            "list": [
                ["1710014400000", "0.11", "0.13", "0.10", "0.12", "800000", "96000"],
                ["1710000000000", "0.10", "0.12", "0.09", "0.11", "1000000", "110000"],
            ]
        }
    }

    frame = parse_bybit_4h_klines(symbol="DOGEUSDT", payload=payload)

    assert frame.to_dict("records") == [
        {
            "symbol": "DOGEUSDT",
            "timestamp": pd.Timestamp("2024-03-09 16:00:00+00:00"),
            "close": 0.11,
            "return_4h": None,
            "volume_quote": 110000.0,
        },
        {
            "symbol": "DOGEUSDT",
            "timestamp": pd.Timestamp("2024-03-09 20:00:00+00:00"),
            "close": 0.12,
            "return_4h": (0.12 / 0.11) - 1.0,
            "volume_quote": 96000.0,
        },
    ]


def test_parse_bybit_open_interest_history_uses_notional_field():
    payload = {
        "result": {
            "list": [
                {"timestamp": "1710000000000", "openInterest": "220000"}
            ]
        }
    }

    frame = parse_bybit_open_interest_history(symbol="DOGEUSDT", payload=payload)

    assert frame.to_dict("records") == [
        {
            "symbol": "DOGEUSDT",
            "timestamp": pd.Timestamp("2024-03-09 16:00:00+00:00"),
            "oi_contracts": None,
            "oi_usd": 220000.0,
        }
    ]


def test_parse_bybit_funding_and_sentiment_histories():
    funding_payload = {
        "result": {"list": [{"symbol": "DOGEUSDT", "fundingRate": "0.0002", "fundingRateTimestamp": "1710000000000"}]}
    }
    ratio_payload = {
        "result": {"list": [{"symbol": "DOGEUSDT", "timestamp": "1710000000000", "buyRatio": "0.6", "sellRatio": "0.4"}]}
    }

    funding = parse_bybit_funding_history(funding_payload)
    ratio = parse_long_short_ratio_history(ratio_payload)

    assert funding.to_dict("records") == [
        {
            "symbol": "DOGEUSDT",
            "timestamp": pd.Timestamp("2024-03-09 16:00:00+00:00"),
            "funding_rate": 0.0002,
        }
    ]
    assert ratio.to_dict("records") == [
        {
            "symbol": "DOGEUSDT",
            "timestamp": pd.Timestamp("2024-03-09 16:00:00+00:00"),
            "long_short_ratio": 1.5,
        }
    ]


def test_parse_supply_snapshot_extracts_float_and_market_cap_inputs():
    payload = {
        "id": "dogecoin",
        "symbol": "doge",
        "name": "Dogecoin",
        "market_data": {
            "circulating_supply": 120_000_000_000,
            "max_supply": 150_000_000_000,
            "current_price": {"usd": 0.11},
            "market_cap": {"usd": 13_200_000_000},
        },
    }

    snapshot = parse_supply_snapshot(asset="DOGE", timestamp="2024-03-09", payload=payload)

    assert snapshot == {
        "asset": "DOGE",
        "timestamp": pd.Timestamp("2024-03-09 00:00:00+00:00"),
        "supply_id": "dogecoin",
        "name": "Dogecoin",
        "circ_supply": 120_000_000_000.0,
        "max_supply": 150_000_000_000.0,
        "float_ratio": 0.8,
        "mcap_est": 13_200_000_000.0,
    }


def test_build_supply_history_frame_derives_circulating_supply_from_market_cap_and_price():
    frame = build_supply_history_frame(
        asset="DOGE",
        coin_id="dogecoin",
        name="Dogecoin",
        max_supply=150_000_000_000.0,
        prices=[
            [1710000000000, 0.10],
            [1710086400000, 0.12],
        ],
        market_caps=[
            [1710000000000, 12_000_000_000.0],
            [1710086400000, 13_200_000_000.0],
        ],
    )

    assert frame.to_dict("records") == [
        {
            "asset": "DOGE",
            "timestamp": pd.Timestamp("2024-03-09 00:00:00+00:00"),
            "supply_id": "dogecoin",
            "name": "Dogecoin",
            "circ_supply": 120_000_000_000.0,
            "max_supply": 150_000_000_000.0,
            "float_ratio": 0.8,
            "mcap_est": 12_000_000_000.0,
        },
        {
            "asset": "DOGE",
            "timestamp": pd.Timestamp("2024-03-10 00:00:00+00:00"),
            "supply_id": "dogecoin",
            "name": "Dogecoin",
            "circ_supply": 110_000_000_000.0,
            "max_supply": 150_000_000_000.0,
            "float_ratio": 0.7333333333,
            "mcap_est": 13_200_000_000.0,
        },
    ]


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {})})
        return _FakeResponse(self.payloads.pop(0))


def test_fetch_binance_4h_klines_paginates_until_end_time():
    session = _FakeSession(
        [
            [
                [1710000000000, "0.10", "0.12", "0.09", "0.11", "0", 1710014399999, "110000", 0, "0", "0", "0"],
                [1710014400000, "0.11", "0.13", "0.10", "0.12", "0", 1710028799999, "120000", 0, "0", "0", "0"],
            ],
            [
                [1710028800000, "0.12", "0.14", "0.11", "0.13", "0", 1710043199999, "130000", 0, "0", "0", "0"],
            ],
        ]
    )

    frame = fetch_binance_4h_klines(
        session=session,
        symbol="DOGEUSDT",
        asset="DOGE",
        start_ms=1710000000000,
        end_ms=1710028800000,
        limit=2,
    )

    assert frame["close"].tolist() == [0.11, 0.12, 0.13]
    assert frame["return_4h"].tolist() == [None, (0.12 / 0.11) - 1.0, (0.13 / 0.12) - 1.0]
    assert len(session.calls) == 2
    assert session.calls[1]["params"]["startTime"] == 1710014400001


def test_fetch_bybit_4h_klines_paginates_until_end_time():
    session = _FakeSession(
        [
            {
                "result": {
                    "list": [
                        ["1710014400000", "0.11", "0.13", "0.10", "0.12", "0", "96000"],
                        ["1710000000000", "0.10", "0.12", "0.09", "0.11", "0", "110000"],
                    ]
                }
            },
            {
                "result": {
                    "list": [
                        ["1710028800000", "0.12", "0.14", "0.11", "0.13", "0", "130000"],
                    ]
                }
            },
        ]
    )

    frame = fetch_bybit_4h_klines(
        session=session,
        symbol="DOGEUSDT",
        asset="DOGE",
        start_ms=1710000000000,
        end_ms=1710028800000,
        limit=2,
    )

    assert frame["close"].tolist() == [0.11, 0.12, 0.13]
    assert frame["return_4h"].tolist() == [None, (0.12 / 0.11) - 1.0, (0.13 / 0.12) - 1.0]
    assert len(session.calls) == 2
    assert session.calls[1]["params"]["start"] == 1710014400001
