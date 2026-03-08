# Reckless Binance Event Study

This project reconstructs weekly top gainers from Binance USDT-margined perpetual futures and studies their forward returns over the next 14 days.

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

## Output files

- `outputs/events.csv`
- `outputs/forward_returns_by_day.csv`
- `outputs/bucket_paths.csv`
- `outputs/feature_comparison.csv`
- `outputs/forward_return_buckets.svg`
- `outputs/summary.json`
