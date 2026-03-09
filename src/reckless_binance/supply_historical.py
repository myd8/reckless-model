from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import requests


BASE_URL = "https://api.coingecko.com/api/v3"


def parse_supply_snapshot(asset: str, timestamp: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Extract supply and market-cap fields from a point-in-time payload."""

    market_data = payload.get("market_data", {})
    circ_supply = market_data.get("circulating_supply")
    max_supply = market_data.get("max_supply")
    market_cap = market_data.get("market_cap", {}).get("usd")

    float_ratio = None
    if circ_supply is not None and max_supply not in (None, 0):
        float_ratio = float(circ_supply) / float(max_supply)

    return {
        "asset": asset,
        "timestamp": pd.Timestamp(timestamp, tz="UTC"),
        "supply_id": payload.get("id"),
        "name": payload.get("name"),
        "circ_supply": None if circ_supply is None else float(circ_supply),
        "max_supply": None if max_supply is None else float(max_supply),
        "float_ratio": float_ratio,
        "mcap_est": None if market_cap is None else float(market_cap),
    }


def fetch_coin_list(session: requests.Session) -> list[dict[str, Any]]:
    """Fetch the CoinGecko coin list for supply ID mapping."""

    response = session.get(f"{BASE_URL}/coins/list", params={"include_platform": "false"}, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise TypeError("Expected CoinGecko coin list to be a list")
    return payload


def fetch_coin_history(
    *,
    session: requests.Session,
    asset: str,
    coin_id: str,
    snapshot_date: date,
) -> dict[str, Any]:
    """Fetch a historical CoinGecko supply snapshot for one day."""

    response = session.get(
        f"{BASE_URL}/coins/{coin_id}/history",
        params={"date": snapshot_date.strftime("%d-%m-%Y"), "localization": "false"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise TypeError("Expected CoinGecko history payload to be a dict")
    return parse_supply_snapshot(asset=asset, timestamp=snapshot_date.isoformat(), payload=payload)


def fetch_coin_metadata(session: requests.Session, coin_id: str) -> dict[str, Any]:
    """Fetch CoinGecko metadata for one asset."""

    response = session.get(
        f"{BASE_URL}/coins/{coin_id}",
        params={
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise TypeError("Expected CoinGecko coin payload to be a dict")
    return payload


def fetch_market_chart_range(
    session: requests.Session,
    coin_id: str,
    *,
    start_ms: int,
    end_ms: int,
) -> dict[str, Any]:
    """Fetch CoinGecko price and market-cap history over a range."""

    response = session.get(
        f"{BASE_URL}/coins/{coin_id}/market_chart/range",
        params={"vs_currency": "usd", "from": start_ms // 1000, "to": end_ms // 1000},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise TypeError("Expected CoinGecko market chart payload to be a dict")
    return payload


def build_supply_history_frame(
    *,
    asset: str,
    coin_id: str,
    name: str,
    max_supply: float | None,
    prices: list[list[float]],
    market_caps: list[list[float]],
) -> pd.DataFrame:
    """Build a daily supply history frame from price and market-cap ranges."""

    price_frame = pd.DataFrame(prices, columns=["timestamp_ms", "price_usd"])
    cap_frame = pd.DataFrame(market_caps, columns=["timestamp_ms", "mcap_est"])

    if price_frame.empty or cap_frame.empty:
        return pd.DataFrame(columns=["asset", "timestamp", "supply_id", "name", "circ_supply", "max_supply", "float_ratio", "mcap_est"])

    price_frame["timestamp"] = pd.to_datetime(price_frame["timestamp_ms"], unit="ms", utc=True).dt.floor("D")
    cap_frame["timestamp"] = pd.to_datetime(cap_frame["timestamp_ms"], unit="ms", utc=True).dt.floor("D")

    merged = (
        price_frame.groupby("timestamp", as_index=False)["price_usd"].last()
        .merge(cap_frame.groupby("timestamp", as_index=False)["mcap_est"].last(), on="timestamp", how="inner")
        .sort_values("timestamp", kind="stable")
        .reset_index(drop=True)
    )
    merged["circ_supply"] = (merged["mcap_est"] / merged["price_usd"].replace(0, pd.NA)).round(10)
    merged["max_supply"] = max_supply
    merged["float_ratio"] = (
        (merged["circ_supply"] / merged["max_supply"]).round(10) if max_supply not in (None, 0) else None
    )
    merged["asset"] = asset
    merged["supply_id"] = coin_id
    merged["name"] = name
    return merged[["asset", "timestamp", "supply_id", "name", "circ_supply", "max_supply", "float_ratio", "mcap_est"]]
