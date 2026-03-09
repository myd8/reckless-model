# Reckless Event Study

This project reconstructs weekly top gainers from Binance USDT-margined perpetual futures and studies their forward returns over the next 14 days. It also includes a historical 4h breakout-vs-blowoff feature pipeline for Binance + Bybit top-gainer events, where each 4h event is defined as a first entry into the fixed-count top `N` assets by 4h return after a liquidity filter.

## Scope

- Universe: Binance USDT-margined perpetuals
- Frequency: daily
- Event: first day a contract enters the daily top 20 by trailing 7-day return
- Output: observational research artifacts only

## Local setup

```bash
python3 -m pip install --user --break-system-packages pytest pandas numpy matplotlib requests
```

## Run tests

```bash
PYTHONPATH=src ~/.local/bin/pytest tests -v
```

## Run the pipeline

```bash
PYTHONPATH=src python3 -m reckless_binance.cli --lookback-years 3 --top-n 20
```

## Run the 4h breakout-vs-blowoff pipeline

Fetch raw historical data:

```bash
PYTHONPATH=src python3 scripts/fetch_historical_4h.py \
  --start-date 2025-01-01 \
  --end-date 2025-03-01 \
  --output-root outputs_4h
```

Build canonical market bars:

```bash
PYTHONPATH=src python3 scripts/build_market_bars_4h.py \
  --binance-klines-path outputs_4h/raw/binance/klines_4h/data.parquet \
  --bybit-klines-path outputs_4h/raw/bybit/klines_4h/data.parquet \
  --output-path outputs_4h/raw/market_bars_4h.parquet
```

Build event, feature, and label artifacts:

```bash
PYTHONPATH=src python3 scripts/build_top_gainer_events_4h.py \
  --asset-map-path outputs_4h/asset_map.parquet \
  --binance-klines-path outputs_4h/raw/binance/klines_4h/data.parquet \
  --bybit-klines-path outputs_4h/raw/bybit/klines_4h/data.parquet \
  --output-path outputs_4h/top_gainer_events.parquet \
  --top-n 10 \
  --min-volume-quote 10000

PYTHONPATH=src python3 scripts/build_event_features_4h.py \
  --events-path outputs_4h/top_gainer_events.parquet \
  --market-bars-path outputs_4h/raw/market_bars_4h.parquet \
  --binance-oi-path outputs_4h/raw/binance/open_interest_4h/data.parquet \
  --bybit-oi-path outputs_4h/raw/bybit/open_interest_4h/data.parquet \
  --binance-funding-path outputs_4h/raw/binance/funding/data.parquet \
  --bybit-funding-path outputs_4h/raw/bybit/funding/data.parquet \
  --sentiment-path outputs_4h/raw/bybit/sentiment/data.parquet \
  --supply-path outputs_4h/raw/supply/history/data.parquet \
  --output-path outputs_4h/event_features_4h.parquet

PYTHONPATH=src python3 scripts/build_event_labels.py \
  --feature-path outputs_4h/event_features_4h.parquet \
  --labels-path outputs_4h/event_labels.parquet \
  --summary-path outputs_4h/breakout_blowoff_label_summary.csv
```

## Output files

- `outputs/events.csv`
- `outputs/forward_returns_by_day.csv`
- `outputs/bucket_paths.csv`
- `outputs/feature_comparison.csv`
- `outputs/filter_oos_results.csv`
- `outputs/walk_forward_results.csv`
- `outputs/walk_forward_top_filters.csv`
- `outputs/walk_forward_summary.csv`
- `outputs/secondary_filter_walk_forward_results.csv`
- `outputs/secondary_filter_walk_forward_summary.csv`
- `outputs/tertiary_filter_walk_forward_results.csv`
- `outputs/tertiary_filter_walk_forward_summary.csv`
- `outputs/signal_table.csv`
- `outputs/signal_candidates.csv`
- `outputs/forward_return_buckets.svg`
- `outputs/summary.json`

The 4h pipeline writes:

- `asset_map.parquet`
- `top_gainer_events.parquet`
- `event_features_4h.parquet`
- `event_labels.parquet`
- `breakout_blowoff_label_summary.csv`
