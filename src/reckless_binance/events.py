from __future__ import annotations

import pandas as pd


def build_top20_entry_events(frame: pd.DataFrame, top_n: int) -> pd.DataFrame:
    ordered = frame.sort_values(["symbol", "date"]).reset_index(drop=True).copy()
    ordered["date"] = pd.to_datetime(ordered["date"])
    ordered["in_top"] = ordered["rank_7d"] <= top_n
    previous = ordered.groupby("symbol")["in_top"].shift(1)
    ordered["prev_in_top"] = previous.where(previous.notna(), False).astype(bool)
    ordered["top20_streak_length"] = _compute_streak_lengths(ordered)

    events = ordered.loc[ordered["in_top"] & ordered["prev_in_top"].eq(False)].copy()
    events["prior_top20_entries_30d"] = _compute_prior_entries_30d(events)

    events = events.drop(columns=["in_top", "prev_in_top"]).reset_index(drop=True)
    events["date"] = events["date"].dt.strftime("%Y-%m-%d")
    return events


def attach_forward_returns(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    lookback_days: int,
    lookforward_days: int,
) -> pd.DataFrame:
    price_frame = prices.copy()
    price_frame["date"] = pd.to_datetime(price_frame["date"])
    event_frame = events.copy()
    event_frame["date"] = pd.to_datetime(event_frame["date"])

    price_lookup = {
        (row.symbol, row.date): row.close
        for row in price_frame.itertuples(index=False)
    }

    enriched_rows: list[dict] = []
    for row in event_frame.to_dict("records"):
        event_price = price_lookup.get((row["symbol"], row["date"]))
        if event_price is None:
            continue

        enriched = dict(row)
        for offset in range(-lookback_days, lookforward_days + 1):
            if offset == 0:
                continue
            target_date = row["date"] + pd.Timedelta(days=offset)
            target_price = price_lookup.get((row["symbol"], target_date))
            column = f"ret_{offset:+d}"
            enriched[column] = None if target_price is None else (target_price / event_price) - 1

        enriched["forward_14d_return"] = enriched.get(f"ret_{lookforward_days:+d}")
        value = enriched["forward_14d_return"]
        enriched["reversal_14d"] = None if value is None else int(value < 0)
        enriched_rows.append(enriched)

    return pd.DataFrame(enriched_rows)


def _compute_streak_lengths(frame: pd.DataFrame) -> pd.Series:
    streaks: list[int] = []
    streak_by_symbol: dict[str, int] = {}
    for row in frame.itertuples(index=False):
        current = streak_by_symbol.get(row.symbol, 0)
        if row.in_top:
            current += 1
        else:
            current = 0
        streak_by_symbol[row.symbol] = current
        streaks.append(current)
    return pd.Series(streaks, index=frame.index, dtype="int64")


def _compute_prior_entries_30d(events: pd.DataFrame) -> pd.Series:
    counts: list[int] = []
    prior_dates_by_symbol: dict[str, list[pd.Timestamp]] = {}
    for row in events.itertuples(index=False):
        history = prior_dates_by_symbol.get(row.symbol, [])
        window_start = row.date - pd.Timedelta(days=30)
        recent_count = sum(event_date >= window_start for event_date in history)
        counts.append(recent_count)
        history.append(row.date)
        prior_dates_by_symbol[row.symbol] = history
    return pd.Series(counts, index=events.index, dtype="int64")
