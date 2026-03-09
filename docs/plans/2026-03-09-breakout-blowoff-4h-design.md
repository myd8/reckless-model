# Breakout vs Blowoff 4H Historical Pipeline Design

**Date:** 2026-03-09

## Objective

Build a historical-only top-gainer event feature pipeline for breakout-vs-blowoff classification across Binance and Bybit perpetual markets on the 4h timeframe.

The pipeline should:

- ingest historical exchange data from Binance and Bybit APIs
- save raw market data locally as parquet
- build historical top-gainer events only
- generate a symmetric 4h feature panel around each event
- compute path-stat labels for breakout vs blowoff behavior
- write a label summary CSV for downstream research

## Scope

Artifacts:

1. `top_gainer_events.parquet`
2. `asset_map.parquet`
3. `event_features_4h.parquet`
4. `event_labels.parquet`
5. `outputs/breakout_blowoff_label_summary.csv`

Constraints:

- historical only
- top-gainer events only
- exchanges: Binance + Bybit
- timeframe: 4h
- raw data fetched from exchange APIs
- raw data cached locally as parquet
- modular code under `src/`
- stage scripts under `scripts/`

## Data Sources

### Exchange Market Data

Use Binance and Bybit APIs for:

- 4h market bars
- open interest
- funding
- sentiment / long-short ratio when available

### Supply / Float Data

Use a third data source for:

- `circ_supply`
- `max_supply`
- `float_ratio`
- `mcap_est`

This source should be joined only at the feature stage and remain replaceable.

## Architecture

The system should be divided into five stages.

### 1. Asset Map

Build a canonical asset map:

- canonical `asset`
- Binance perp symbol
- Bybit perp symbol
- supply-data identifier

Write:

- `asset_map.parquet`

### 2. Raw Ingestion

Download and persist raw exchange data separately by source and dataset.

Example raw layout:

- `raw/binance/klines_4h/*.parquet`
- `raw/binance/open_interest_4h/*.parquet`
- `raw/binance/funding/*.parquet`
- `raw/binance/sentiment/*.parquet`
- `raw/bybit/klines_4h/*.parquet`
- `raw/bybit/open_interest_4h/*.parquet`
- `raw/bybit/funding/*.parquet`
- `raw/bybit/sentiment/*.parquet`
- `raw/supply/*.parquet`

### 3. Top-Gainer Events

Use the existing Reckless top-gainer entry logic, adapted to `4h`.

- one row per event
- event time is the first 4h bar where the asset enters the top-gainer set

Write:

- `top_gainer_events.parquet`

### 4. Event Feature Panel

For each event, build a symmetric `-14d` to `+14d` 4h panel.

At 4h resolution:

- `84` bars before the event
- `84` bars after the event

Required columns:

- `event_id`, `asset`, `event_time`, `timestamp`, `rel_bar`
- `close`, `return_4h`, `volume_quote`
- `oi_usd_binance`, `oi_usd_bybit`, `oi_usd_total`
- `oi_share_binance`, `oi_share_bybit`
- `oi_change_4h_total`, `oi_change_z_30`
- `funding_binance`, `funding_bybit`, `funding_mean_1d`
- `long_short_ratio` if available
- `circ_supply`, `max_supply`, `float_ratio`, `mcap_est`

Normalization rule:

- normalize OI to USD notional before combining across exchanges

Missing data rule:

- retain the panel row and keep source-specific fields null when coverage is missing

Write:

- `event_features_4h.parquet`

### 5. Labels and Analysis

Compute event path metrics:

- `ret_1d`, `ret_3d`, `ret_7d`, `ret_14d`
- `peak_return_first_7d`
- `peak_day_first_7d`
- `trough_return_first_7d`
- `drawdown_from_peak_to_day7`
- `drawdown_from_peak_to_day14`

Apply labels by priority:

1. `immediate_reversal`
2. `delayed_blowoff`
3. `breakout`
4. `unclassified`

Write:

- `event_labels.parquet`
- `outputs/breakout_blowoff_label_summary.csv`

## Label Definitions

### A. immediate_reversal

- `ret_1d < 0`

### B. delayed_blowoff

- `ret_1d > 0.05`
- and either:
  - `ret_3d <= 0`
  - or:
    - `peak_day_first_7d in [2,3,4,5]`
    - `peak_return_first_7d > 0.10`
    - `drawdown_from_peak_to_day14 <= -0.15`

### C. breakout

- `ret_1d > 0.05`
- `ret_3d > 0`
- `ret_7d > 0`
- `drawdown_from_peak_to_day7 > -0.10`

Else:

- `unclassified`

## Risks and Caveats

- Binance historical OI availability may be incomplete through official API endpoints, so the pipeline must tolerate sparse Binance OI history.
- Exchange coverage must be nullable rather than dropping event rows.
- Supply / float fields are point-in-time inputs from a third source and should remain separately versioned.
- This phase is a feature-and-label research pipeline, not a live execution system.
