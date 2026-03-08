from reckless_binance.binance_client import (
    active_usdt_perps_url,
    klines_url,
    parse_active_usdt_perpetuals,
)


def test_binance_urls_match_futures_endpoints():
    assert active_usdt_perps_url().endswith("/fapi/v1/exchangeInfo")
    assert klines_url().endswith("/fapi/v1/klines")


def test_parse_active_usdt_perpetuals_filters_contracts():
    payload = {
        "symbols": [
            {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "TRADING"},
            {"symbol": "BTCUSD_PERP", "contractType": "PERPETUAL", "quoteAsset": "USD", "status": "TRADING"},
            {"symbol": "ETHUSDT_240628", "contractType": "CURRENT_QUARTER", "quoteAsset": "USDT", "status": "TRADING"},
            {"symbol": "XRPUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "BREAK"},
        ]
    }

    symbols = parse_active_usdt_perpetuals(payload)

    assert [row["symbol"] for row in symbols] == ["BTCUSDT"]
