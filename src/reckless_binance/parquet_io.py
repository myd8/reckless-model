from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_parquet(path: str | Path) -> pd.DataFrame:
    """Read a parquet artifact into a DataFrame."""

    return pd.read_parquet(Path(path))


def write_parquet(frame: pd.DataFrame, path: str | Path) -> Path:
    """Write a DataFrame to parquet, creating parent directories when needed."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(target, index=False)
    return target


def write_csv(frame: pd.DataFrame, path: str | Path) -> Path:
    """Write a DataFrame to CSV, creating parent directories when needed."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target, index=False)
    return target
