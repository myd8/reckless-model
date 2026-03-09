import pandas as pd

from reckless_binance.asset_map import build_asset_map
from reckless_binance.schemas import ASSET_MAP_COLUMNS


def test_build_asset_map_merges_binance_bybit_and_supply_ids():
    binance = pd.DataFrame(
        [
            {"asset": "DOGE", "binance_symbol": "DOGEUSDT", "base_asset": "DOGE"},
            {"asset": "WIF", "binance_symbol": "1000WIFUSDT", "base_asset": "1000WIF"},
        ]
    )
    bybit = pd.DataFrame(
        [
            {"asset": "DOGE", "bybit_symbol": "DOGEUSDT", "base_asset": "DOGE"},
            {"asset": "WIF", "bybit_symbol": "WIFUSDT", "base_asset": "WIF"},
        ]
    )
    supply = pd.DataFrame(
        [
            {"asset": "DOGE", "supply_id": "dogecoin", "name": "Dogecoin"},
            {"asset": "WIF", "supply_id": "dogwifcoin", "name": "dogwifhat"},
        ]
    )

    asset_map = build_asset_map(binance=binance, bybit=bybit, supply=supply)

    assert list(asset_map.columns) == ASSET_MAP_COLUMNS
    assert asset_map.to_dict("records") == [
        {
            "asset": "DOGE",
            "name": "Dogecoin",
            "binance_symbol": "DOGEUSDT",
            "bybit_symbol": "DOGEUSDT",
            "supply_id": "dogecoin",
        },
        {
            "asset": "WIF",
            "name": "dogwifhat",
            "binance_symbol": "1000WIFUSDT",
            "bybit_symbol": "WIFUSDT",
            "supply_id": "dogwifcoin",
        },
    ]


def test_build_asset_map_keeps_union_of_assets():
    binance = pd.DataFrame([{"asset": "DOGE", "binance_symbol": "DOGEUSDT"}])
    bybit = pd.DataFrame([{"asset": "PEPE", "bybit_symbol": "PEPEUSDT"}])
    supply = pd.DataFrame([{"asset": "DOGE", "supply_id": "dogecoin"}])

    asset_map = build_asset_map(binance=binance, bybit=bybit, supply=supply)

    assert asset_map["asset"].tolist() == ["DOGE", "PEPE"]
    assert asset_map.loc[0, "bybit_symbol"] is None
    assert asset_map.loc[1, "binance_symbol"] is None
