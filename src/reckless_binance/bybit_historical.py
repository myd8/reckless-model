from __future__ import annotations

from typing import Any

import pandas as pd
import requests


BASE_URL = "https://api.bybit.com"


def _timestamp_from_ms(value: int | str) -> pd.Timestamp:
    return pd.to_datetime(int(value), unit="ms", utc=True)


def _get(session: requests.Session, path: str, *, params: dict[str, Any]) -> dict[str, Any]:
    response = session.get(f"{BASE_URL}{path}", params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise TypeError("Expected Bybit payload to be a dict")
    return payload


def fetch_instruments(session: requests.Session) -> list[dict[str, Any]]:
    """Fetch current Bybit linear instruments."""

    payload = _get(session, "/v5/market/instruments-info", params={"category": "linear", "limit": 1000})
    return payload.get("result", {}).get("list", [])


def fetch_4h_klines(
    *,
    session: requests.Session,
    symbol: str,
    asset: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> pd.DataFrame:
    """Fetch Bybit 4h klines and return normalized rows."""

    pages: list[list[Any]] = []
    current_start = start_ms
    while current_start <= end_ms:
        payload = _get(
            session,
            "/v5/market/kline",
            params={"category": "linear", "symbol": symbol, "interval": "240", "start": current_start, "end": end_ms, "limit": limit},
        )
        page = payload.get("result", {}).get("list", [])
        if not page:
            break
        pages.extend(page)
        last_ms = max(int(item[0]) for item in page)
        if len(page) < limit or last_ms >= end_ms:
            break
        current_start = last_ms + 1
    payload = {"result": {"list": pages}}
    frame = parse_4h_klines(symbol=symbol, payload=payload)
    if frame.empty:
        return frame
    frame["asset"] = asset
    return frame[["asset", "symbol", "timestamp", "close", "return_4h", "volume_quote"]]


def fetch_open_interest_history(
    *,
    session: requests.Session,
    symbol: str,
    asset: str,
    start_ms: int,
    end_ms: int,
    limit: int = 200,
) -> pd.DataFrame:
    """Fetch Bybit open interest history."""

    pages: list[dict[str, Any]] = []
    current_start = start_ms
    while current_start <= end_ms:
        payload = _get(
            session,
            "/v5/market/open-interest",
            params={"category": "linear", "symbol": symbol, "intervalTime": "4h", "startTime": current_start, "endTime": end_ms, "limit": limit},
        )
        page = payload.get("result", {}).get("list", [])
        if not page:
            break
        pages.extend(page)
        last_ms = max(int(item["timestamp"]) for item in page)
        if len(page) < limit or last_ms >= end_ms:
            break
        current_start = last_ms + 1
    payload = {"result": {"list": pages}}
    frame = parse_open_interest_history(symbol=symbol, payload=payload)
    if frame.empty:
        return frame
    frame["asset"] = asset
    return frame[["asset", "symbol", "timestamp", "oi_contracts", "oi_usd"]]


def fetch_funding_history(
    *,
    session: requests.Session,
    symbol: str,
    asset: str,
    start_ms: int,
    end_ms: int,
    limit: int = 200,
) -> pd.DataFrame:
    """Fetch Bybit funding history."""

    pages: list[dict[str, Any]] = []
    current_start = start_ms
    while current_start <= end_ms:
        payload = _get(
            session,
            "/v5/market/funding/history",
            params={"category": "linear", "symbol": symbol, "startTime": current_start, "endTime": end_ms, "limit": limit},
        )
        page = payload.get("result", {}).get("list", [])
        if not page:
            break
        pages.extend(page)
        last_ms = max(int(item["fundingRateTimestamp"]) for item in page)
        if len(page) < limit or last_ms >= end_ms:
            break
        current_start = last_ms + 1
    payload = {"result": {"list": pages}}
    frame = parse_funding_history(payload)
    if frame.empty:
        return frame
    frame["asset"] = asset
    return frame[["asset", "symbol", "timestamp", "funding_rate"]]


def fetch_long_short_ratio_history(
    *,
    session: requests.Session,
    symbol: str,
    asset: str,
    start_ms: int,
    end_ms: int,
    limit: int = 500,
) -> pd.DataFrame:
    """Fetch Bybit long/short account ratio history."""

    pages: list[dict[str, Any]] = []
    current_start = start_ms
    while current_start <= end_ms:
        payload = _get(
            session,
            "/v5/market/account-ratio",
            params={"category": "linear", "symbol": symbol, "period": "4h", "startTime": current_start, "endTime": end_ms, "limit": limit},
        )
        page = payload.get("result", {}).get("list", [])
        if not page:
            break
        pages.extend(page)
        last_ms = max(int(item["timestamp"]) for item in page)
        if len(page) < limit or last_ms >= end_ms:
            break
        current_start = last_ms + 1
    payload = {"result": {"list": pages}}
    frame = parse_long_short_ratio_history(payload)
    if frame.empty:
        return frame
    frame["asset"] = asset
    return frame[["asset", "symbol", "timestamp", "long_short_ratio"]]


def parse_4h_klines(symbol: str, payload: dict[str, Any]) -> pd.DataFrame:
    """Normalize Bybit 4h kline rows into a compact feature-ready frame."""

    raw_rows = payload.get("result", {}).get("list", [])
    ordered = sorted(raw_rows, key=lambda row: int(row[0]))
    rows: list[dict[str, Any]] = []
    previous_close: float | None = None
    for item in ordered:
        close = float(item[4])
        rows.append(
            {
                "symbol": symbol,
                "timestamp": _timestamp_from_ms(item[0]),
                "close": close,
                "return_4h": None if previous_close is None else (close / previous_close) - 1.0,
                "volume_quote": float(item[6]),
            }
        )
        previous_close = close
    frame = pd.DataFrame(rows, dtype=object)
    return frame.where(pd.notna(frame), None)


def parse_open_interest_history(symbol: str, payload: dict[str, Any]) -> pd.DataFrame:
    """Normalize Bybit open interest history to USD notional rows."""

    raw_rows = payload.get("result", {}).get("list", [])
    rows = [
        {
            "symbol": symbol,
            "timestamp": _timestamp_from_ms(row["timestamp"]),
            "oi_contracts": None,
            "oi_usd": float(row["openInterest"]),
        }
        for row in raw_rows
    ]
    return pd.DataFrame(rows, dtype=object).where(lambda frame: pd.notna(frame), None)


def parse_funding_history(payload: dict[str, Any]) -> pd.DataFrame:
    """Normalize Bybit funding history rows."""

    raw_rows = payload.get("result", {}).get("list", [])
    rows = [
        {
            "symbol": row["symbol"],
            "timestamp": _timestamp_from_ms(row["fundingRateTimestamp"]),
            "funding_rate": float(row["fundingRate"]),
        }
        for row in raw_rows
    ]
    return pd.DataFrame(rows)


def parse_long_short_ratio_history(payload: dict[str, Any]) -> pd.DataFrame:
    """Normalize Bybit account ratio history to a long/short ratio."""

    raw_rows = payload.get("result", {}).get("list", [])
    rows = []
    for row in raw_rows:
        buy_ratio = float(row["buyRatio"])
        sell_ratio = float(row["sellRatio"])
        ratio = None if sell_ratio == 0 else round(buy_ratio / sell_ratio, 10)
        rows.append(
            {
                "symbol": row["symbol"],
                "timestamp": _timestamp_from_ms(row["timestamp"]),
                "long_short_ratio": ratio,
            }
        )
    return pd.DataFrame(rows, dtype=object).where(lambda frame: pd.notna(frame), None)
