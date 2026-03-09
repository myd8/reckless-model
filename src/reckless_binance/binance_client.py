from __future__ import annotations

from typing import Any

import requests


BASE_URL = "https://fapi.binance.com"


def active_usdt_perps_url() -> str:
    return f"{BASE_URL}/fapi/v1/exchangeInfo"


def klines_url() -> str:
    return f"{BASE_URL}/fapi/v1/klines"


def fetch_exchange_info(session: requests.Session | None = None) -> dict[str, Any]:
    session = session or requests.Session()
    response = session.get(active_usdt_perps_url(), timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise TypeError("Expected Binance exchange info payload to be a dict")
    return payload


def parse_active_usdt_perpetuals(payload: dict[str, Any]) -> list[dict[str, Any]]:
    symbols = payload.get("symbols", [])
    active = []
    for symbol in symbols:
        if symbol.get("contractType") != "PERPETUAL":
            continue
        if symbol.get("quoteAsset") != "USDT":
            continue
        if symbol.get("status") != "TRADING":
            continue
        active.append(symbol)
    return active


def fetch_daily_klines(
    session: requests.Session | None,
    symbol: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
) -> list[list[Any]]:
    session = session or requests.Session()
    response = session.get(
        klines_url(),
        params={
            "symbol": symbol,
            "interval": "1d",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise TypeError("Expected Binance klines payload to be a list")
    return payload
