from __future__ import annotations

from typing import Any

import pandas as pd
import requests


BASE_URL = "https://fapi.binance.com"


def _timestamp_from_ms(value: int | str) -> pd.Timestamp:
    return pd.to_datetime(int(value), unit="ms", utc=True)


def _get(session: requests.Session, path: str, *, params: dict[str, Any]) -> Any:
    response = session.get(f"{BASE_URL}{path}", params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_4h_klines(
    *,
    session: requests.Session,
    symbol: str,
    asset: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
) -> pd.DataFrame:
    """Fetch Binance 4h klines and return normalized rows."""

    payload: list[list[Any]] = []
    current_start = start_ms
    while current_start <= end_ms:
        page = _get(
            session,
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": "4h", "startTime": current_start, "endTime": end_ms, "limit": limit},
        )
        if not page:
            break
        payload.extend(page)
        last_open_ms = int(page[-1][0])
        if len(page) < limit or last_open_ms >= end_ms:
            break
        current_start = last_open_ms + 1
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
    limit: int = 500,
) -> pd.DataFrame:
    """Fetch Binance historical open interest statistics."""

    payload: list[dict[str, Any]] = []
    current_start = start_ms
    while current_start <= end_ms:
        page = _get(
            session,
            "/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "4h", "startTime": current_start, "endTime": end_ms, "limit": limit},
        )
        if not page:
            break
        payload.extend(page)
        last_ms = int(page[-1]["timestamp"])
        if len(page) < limit or last_ms >= end_ms:
            break
        current_start = last_ms + 1
    frame = parse_open_interest_history(payload)
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
    limit: int = 1000,
) -> pd.DataFrame:
    """Fetch Binance funding history."""

    payload: list[dict[str, Any]] = []
    current_start = start_ms
    while current_start <= end_ms:
        page = _get(
            session,
            "/fapi/v1/fundingRate",
            params={"symbol": symbol, "startTime": current_start, "endTime": end_ms, "limit": limit},
        )
        if not page:
            break
        payload.extend(page)
        last_ms = int(page[-1]["fundingTime"])
        if len(page) < limit or last_ms >= end_ms:
            break
        current_start = last_ms + 1
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
    """Fetch Binance global long/short account ratios."""

    payload: list[dict[str, Any]] = []
    current_start = start_ms
    while current_start <= end_ms:
        page = _get(
            session,
            "/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": "4h", "startTime": current_start, "endTime": end_ms, "limit": limit},
        )
        if not page:
            break
        payload.extend(page)
        last_ms = int(page[-1]["timestamp"])
        if len(page) < limit or last_ms >= end_ms:
            break
        current_start = last_ms + 1
    rows = [
        {
            "asset": asset,
            "symbol": symbol,
            "timestamp": _timestamp_from_ms(row["timestamp"]),
            "long_short_ratio": float(row["longShortRatio"]),
        }
        for row in payload
    ]
    return pd.DataFrame(rows)


def parse_4h_klines(symbol: str, payload: list[list[Any]]) -> pd.DataFrame:
    """Normalize Binance 4h kline rows into a compact feature-ready frame."""

    rows: list[dict[str, Any]] = []
    previous_close: float | None = None
    for item in payload:
        close = float(item[4])
        rows.append(
            {
                "symbol": symbol,
                "timestamp": _timestamp_from_ms(item[0]),
                "close": close,
                "return_4h": None if previous_close is None else (close / previous_close) - 1.0,
                "volume_quote": float(item[7]),
            }
        )
        previous_close = close
    frame = pd.DataFrame(rows, dtype=object)
    return frame.where(pd.notna(frame), None)


def parse_open_interest_history(payload: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize Binance open interest history rows to contracts and USD notional."""

    rows = [
        {
            "symbol": row["symbol"],
            "timestamp": _timestamp_from_ms(row["timestamp"]),
            "oi_contracts": float(row["sumOpenInterest"]),
            "oi_usd": float(row["sumOpenInterestValue"]),
        }
        for row in payload
    ]
    return pd.DataFrame(rows)


def parse_funding_history(payload: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize Binance funding history rows."""

    rows = [
        {
            "symbol": row["symbol"],
            "timestamp": _timestamp_from_ms(row["fundingTime"]),
            "funding_rate": float(row["fundingRate"]),
        }
        for row in payload
    ]
    return pd.DataFrame(rows)
