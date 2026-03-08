from __future__ import annotations

from typing import Any

import pandas as pd


def eligible_on_date(row: dict[str, Any], as_of_date: pd.Timestamp) -> bool:
    onboard_date = pd.Timestamp(row["onboard_date"])
    current_date = pd.Timestamp(as_of_date)
    return current_date >= onboard_date


def with_days_since_listing(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    enriched["date"] = pd.to_datetime(enriched["date"])
    enriched["onboard_date"] = pd.to_datetime(enriched["onboard_date"])
    enriched["days_since_listing"] = (
        enriched["date"] - enriched["onboard_date"]
    ).dt.days
    return enriched
