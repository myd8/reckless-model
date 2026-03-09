from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from reckless_binance.asset_map import build_asset_map
from reckless_binance.binance_client import fetch_exchange_info, parse_active_usdt_perpetuals
from reckless_binance.binance_historical import (
    fetch_4h_klines as fetch_binance_4h_klines,
    fetch_funding_history as fetch_binance_funding_history,
    fetch_long_short_ratio_history as fetch_binance_long_short_ratio_history,
    fetch_open_interest_history as fetch_binance_open_interest_history,
)
from reckless_binance.bybit_historical import (
    fetch_4h_klines as fetch_bybit_4h_klines,
    fetch_funding_history as fetch_bybit_funding_history,
    fetch_instruments as fetch_bybit_instruments,
    fetch_long_short_ratio_history as fetch_bybit_long_short_ratio_history,
    fetch_open_interest_history as fetch_bybit_open_interest_history,
)
from reckless_binance.parquet_io import write_parquet
from reckless_binance.supply_historical import (
    build_supply_history_frame,
    fetch_coin_list,
    fetch_coin_metadata,
    fetch_market_chart_range,
)


def canonical_asset_symbol(base_asset: str) -> str:
    """Normalize exchange base assets to a canonical asset symbol."""

    return re.sub(r"^\d+", "", base_asset.upper())


def fetch_all_to_parquet(
    *,
    start_date: str,
    end_date: str,
    output_root: Path,
    limit_assets: int | None = None,
    session: requests.Session | None = None,
) -> dict[str, Path]:
    """Fetch raw historical 4h data and write raw parquet artifacts locally."""

    session = session or requests.Session()
    root = Path(output_root)
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    binance_catalog = _build_binance_catalog(session)
    bybit_catalog = _build_bybit_catalog(session)
    all_assets = sorted(set(binance_catalog["asset"]).union(bybit_catalog["asset"]))
    if limit_assets is not None:
        all_assets = all_assets[:limit_assets]
        binance_catalog = binance_catalog.loc[binance_catalog["asset"].isin(all_assets)].reset_index(drop=True)
        bybit_catalog = bybit_catalog.loc[bybit_catalog["asset"].isin(all_assets)].reset_index(drop=True)

    coin_list = fetch_coin_list(session)
    supply_catalog = _build_supply_catalog(all_assets, coin_list)
    asset_map = build_asset_map(binance=binance_catalog, bybit=bybit_catalog, supply=supply_catalog)

    raw_paths = {
        "asset_map": root / "asset_map.parquet",
        "binance_klines": root / "raw" / "binance" / "klines_4h" / "data.parquet",
        "binance_oi": root / "raw" / "binance" / "open_interest_4h" / "data.parquet",
        "binance_funding": root / "raw" / "binance" / "funding" / "data.parquet",
        "binance_sentiment": root / "raw" / "binance" / "sentiment" / "data.parquet",
        "bybit_klines": root / "raw" / "bybit" / "klines_4h" / "data.parquet",
        "bybit_oi": root / "raw" / "bybit" / "open_interest_4h" / "data.parquet",
        "bybit_funding": root / "raw" / "bybit" / "funding" / "data.parquet",
        "bybit_sentiment": root / "raw" / "bybit" / "sentiment" / "data.parquet",
        "supply": root / "raw" / "supply" / "history" / "data.parquet",
    }

    write_parquet(asset_map, raw_paths["asset_map"])
    write_parquet(_fetch_binance_dataset(binance_catalog, start_dt, end_dt, session, "klines"), raw_paths["binance_klines"])
    write_parquet(_fetch_binance_dataset(binance_catalog, start_dt, end_dt, session, "oi"), raw_paths["binance_oi"])
    write_parquet(_fetch_binance_dataset(binance_catalog, start_dt, end_dt, session, "funding"), raw_paths["binance_funding"])
    write_parquet(_fetch_binance_dataset(binance_catalog, start_dt, end_dt, session, "sentiment"), raw_paths["binance_sentiment"])
    write_parquet(_fetch_bybit_dataset(bybit_catalog, start_dt, end_dt, session, "klines"), raw_paths["bybit_klines"])
    write_parquet(_fetch_bybit_dataset(bybit_catalog, start_dt, end_dt, session, "oi"), raw_paths["bybit_oi"])
    write_parquet(_fetch_bybit_dataset(bybit_catalog, start_dt, end_dt, session, "funding"), raw_paths["bybit_funding"])
    write_parquet(_fetch_bybit_dataset(bybit_catalog, start_dt, end_dt, session, "sentiment"), raw_paths["bybit_sentiment"])
    write_parquet(_fetch_supply_history(supply_catalog, start_dt.date(), end_dt.date(), session), raw_paths["supply"])
    return raw_paths


def _build_binance_catalog(session: requests.Session) -> pd.DataFrame:
    exchange_info = fetch_exchange_info(session)
    active = parse_active_usdt_perpetuals(exchange_info)
    rows = [
        {
            "asset": canonical_asset_symbol(symbol["baseAsset"]),
            "binance_symbol": symbol["symbol"],
            "base_asset": symbol["baseAsset"],
        }
        for symbol in active
    ]
    return pd.DataFrame(rows).drop_duplicates(subset=["asset"], keep="first").reset_index(drop=True)


def _build_bybit_catalog(session: requests.Session) -> pd.DataFrame:
    instruments = fetch_bybit_instruments(session)
    rows = [
        {
            "asset": canonical_asset_symbol(row["baseCoin"]),
            "bybit_symbol": row["symbol"],
            "base_asset": row["baseCoin"],
        }
        for row in instruments
        if row.get("quoteCoin") == "USDT"
        and row.get("contractType") == "LinearPerpetual"
        and row.get("status") == "Trading"
    ]
    return pd.DataFrame(rows).drop_duplicates(subset=["asset"], keep="first").reset_index(drop=True)


def _build_supply_catalog(all_assets: list[str], coin_list: list[dict[str, Any]]) -> pd.DataFrame:
    symbol_map: dict[str, dict[str, Any]] = {}
    for coin in coin_list:
        symbol = str(coin.get("symbol", "")).upper()
        symbol_map.setdefault(symbol, coin)
    rows = []
    for asset in all_assets:
        coin = symbol_map.get(asset)
        rows.append(
            {
                "asset": asset,
                "supply_id": None if coin is None else coin.get("id"),
                "name": None if coin is None else coin.get("name"),
            }
        )
    return pd.DataFrame(rows)


def _fetch_binance_dataset(
    catalog: pd.DataFrame,
    start_dt: datetime,
    end_dt: datetime,
    session: requests.Session,
    dataset: str,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for row in catalog.itertuples(index=False):
        if dataset == "klines":
            frame = fetch_binance_4h_klines(session=session, symbol=row.binance_symbol, asset=row.asset, start_ms=_ms(start_dt), end_ms=_ms(end_dt))
        elif dataset == "oi":
            frame = fetch_binance_open_interest_history(session=session, symbol=row.binance_symbol, asset=row.asset, start_ms=_ms(start_dt), end_ms=_ms(end_dt))
        elif dataset == "funding":
            frame = fetch_binance_funding_history(session=session, symbol=row.binance_symbol, asset=row.asset, start_ms=_ms(start_dt), end_ms=_ms(end_dt))
        else:
            frame = fetch_binance_long_short_ratio_history(session=session, symbol=row.binance_symbol, asset=row.asset, start_ms=_ms(start_dt), end_ms=_ms(end_dt))
        if not frame.empty:
            rows.append(frame)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _fetch_bybit_dataset(
    catalog: pd.DataFrame,
    start_dt: datetime,
    end_dt: datetime,
    session: requests.Session,
    dataset: str,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for row in catalog.itertuples(index=False):
        if dataset == "klines":
            frame = fetch_bybit_4h_klines(session=session, symbol=row.bybit_symbol, asset=row.asset, start_ms=_ms(start_dt), end_ms=_ms(end_dt))
        elif dataset == "oi":
            frame = fetch_bybit_open_interest_history(session=session, symbol=row.bybit_symbol, asset=row.asset, start_ms=_ms(start_dt), end_ms=_ms(end_dt))
        elif dataset == "funding":
            frame = fetch_bybit_funding_history(session=session, symbol=row.bybit_symbol, asset=row.asset, start_ms=_ms(start_dt), end_ms=_ms(end_dt))
        else:
            frame = fetch_bybit_long_short_ratio_history(session=session, symbol=row.bybit_symbol, asset=row.asset, start_ms=_ms(start_dt), end_ms=_ms(end_dt))
        if not frame.empty:
            rows.append(frame)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _fetch_supply_history(
    supply_catalog: pd.DataFrame,
    start_date: date,
    end_date: date,
    session: requests.Session,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    start_dt = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    start_ms = _ms(start_dt)
    end_ms = _ms(end_dt)

    for row in supply_catalog.itertuples(index=False):
        if not row.supply_id:
            continue
        metadata = fetch_coin_metadata(session, row.supply_id)
        max_supply = metadata.get("market_data", {}).get("max_supply")
        payload = fetch_market_chart_range(session, row.supply_id, start_ms=start_ms, end_ms=end_ms)
        frame = build_supply_history_frame(
            asset=row.asset,
            coin_id=row.supply_id,
            name=metadata.get("name", row.name),
            max_supply=None if max_supply is None else float(max_supply),
            prices=payload.get("prices", []),
            market_caps=payload.get("market_caps", []),
        )
        if not frame.empty:
            rows.append(frame)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)
