import pandas as pd

from reckless_binance.schemas import ASSET_MAP_COLUMNS


def build_asset_map(
    *,
    binance: pd.DataFrame,
    bybit: pd.DataFrame,
    supply: pd.DataFrame,
) -> pd.DataFrame:
    """Build a canonical asset map across Binance, Bybit, and supply data."""

    merged = (
        binance.reindex(columns=["asset", "binance_symbol"])
        .merge(
            bybit.reindex(columns=["asset", "bybit_symbol"]),
            on="asset",
            how="outer",
        )
        .merge(
            supply.reindex(columns=["asset", "supply_id", "name"]),
            on="asset",
            how="outer",
        )
        .sort_values("asset", kind="stable")
        .reset_index(drop=True)
    )

    asset_map = merged.reindex(columns=ASSET_MAP_COLUMNS).astype(object)
    return asset_map.where(pd.notna(asset_map), None)
