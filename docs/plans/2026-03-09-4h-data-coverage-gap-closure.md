# 4h Data Coverage Gap Closure Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve the 4h event dataset so price, volume, open interest, funding, long/short ratio, and float/supply features are sufficiently complete to evaluate predictive value at the event timestamp.

**Architecture:** Keep the existing `outputs_4h` pipeline shape, but add explicit coverage auditing, point-in-time backfill stages, and source-specific fallbacks. Prioritize backfilling raw data first, then regenerate `top_gainer_events.parquet`, `event_features_4h.parquet`, and `event_labels.parquet`, and finally write a coverage report that quantifies what is complete, partial, and missing.

**Tech Stack:** Python 3.12, pandas, requests, pyarrow, pytest, parquet artifacts under `outputs_4h`

---

### Task 1: Add a dataset coverage audit module

**Files:**
- Create: `src/reckless_binance/coverage_audit.py`
- Create: `tests/test_coverage_audit.py`

**Step 1: Write the failing test**

```python
def test_summarize_feature_coverage_reports_event_level_non_null_rates():
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_coverage_audit.py -v`
Expected: FAIL with `ModuleNotFoundError` or missing function

**Step 3: Write minimal implementation**

Implement:
- `summarize_event_feature_coverage(event_features: pd.DataFrame) -> pd.DataFrame`
- `summarize_raw_dataset_coverage(frames: dict[str, pd.DataFrame]) -> pd.DataFrame`

Coverage output should include:
- dataset name
- row count
- asset count
- timestamp min/max
- event-level non-null coverage for target columns

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_coverage_audit.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_coverage_audit.py src/reckless_binance/coverage_audit.py
git commit -m "feat: add 4h coverage audit helpers"
```

### Task 2: Make raw ingestion coverage-aware and source-aware

**Files:**
- Modify: `src/reckless_binance/ingest_4h.py`
- Modify: `src/reckless_binance/binance_historical.py`
- Modify: `src/reckless_binance/bybit_historical.py`
- Modify: `src/reckless_binance/supply_historical.py`
- Create: `tests/test_ingest_4h_coverage.py`

**Step 1: Write the failing test**

```python
def test_ingestion_records_empty_or_partial_source_outputs_without_crashing():
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_ingest_4h_coverage.py -v`
Expected: FAIL because coverage metadata is not written

**Step 3: Write minimal implementation**

Add:
- source-specific coverage metadata output under `outputs_4h/raw/coverage/`
- explicit labels for `complete`, `partial`, `empty`
- handling for endpoints known to be sparse or unsupported

Required behavior:
- Binance OI empty output must be persisted with schema and marked `empty`
- Bybit OI, funding, and sentiment outputs must be marked `partial` or `complete` based on non-null row counts
- supply raw output must be marked `missing` until backfilled

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_ingest_4h_coverage.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_ingest_4h_coverage.py src/reckless_binance/ingest_4h.py src/reckless_binance/binance_historical.py src/reckless_binance/bybit_historical.py src/reckless_binance/supply_historical.py
git commit -m "feat: track 4h raw data source coverage"
```

### Task 3: Backfill Bybit historical 4h data deeper than current cache

**Files:**
- Modify: `src/reckless_binance/bybit_historical.py`
- Modify: `src/reckless_binance/ingest_4h.py`
- Create: `tests/test_bybit_backfill_depth.py`

**Step 1: Write the failing test**

```python
def test_bybit_backfill_retries_older_windows_until_exhausted():
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_bybit_backfill_depth.py -v`
Expected: FAIL because only one range walk is attempted

**Step 3: Write minimal implementation**

Implement:
- backwards window walking for Bybit klines, OI, funding, and sentiment
- per-symbol earliest timestamp capture
- stop condition when oldest page no longer moves backward

Target result:
- maximize Bybit coverage before regenerating features

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_bybit_backfill_depth.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_bybit_backfill_depth.py src/reckless_binance/bybit_historical.py src/reckless_binance/ingest_4h.py
git commit -m "feat: deepen bybit 4h historical backfill"
```

### Task 4: Add point-in-time float/supply backfill

**Files:**
- Modify: `src/reckless_binance/supply_historical.py`
- Modify: `src/reckless_binance/ingest_4h.py`
- Create: `tests/test_supply_backfill.py`

**Step 1: Write the failing test**

```python
def test_supply_backfill_builds_daily_float_history_for_assets():
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_supply_backfill.py -v`
Expected: FAIL because supply raw parquet is empty

**Step 3: Write minimal implementation**

Implement:
- supply metadata fetch
- market-cap and price history range fetch
- derived daily `circ_supply`, `max_supply`, `float_ratio`, `mcap_est`
- cached raw parquet with schema and timestamps

Required outputs:
- non-empty `outputs_4h/raw/supply/history/data.parquet`
- coverage metadata for supply

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_supply_backfill.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_supply_backfill.py src/reckless_binance/supply_historical.py src/reckless_binance/ingest_4h.py
git commit -m "feat: backfill 4h supply and float history"
```

### Task 5: Make event feature generation expose explicit quality flags

**Files:**
- Modify: `src/reckless_binance/features_4h.py`
- Create: `tests/test_feature_quality_flags.py`

**Step 1: Write the failing test**

```python
def test_event_feature_panel_marks_missing_oi_and_supply_sources():
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_feature_quality_flags.py -v`
Expected: FAIL because no source-quality flags exist

**Step 3: Write minimal implementation**

Add event panel flags:
- `has_binance_oi`
- `has_bybit_oi`
- `has_any_oi`
- `has_binance_funding`
- `has_bybit_funding`
- `has_any_funding`
- `has_long_short_ratio`
- `has_supply`

These flags should be available for both per-bar rows and event rows.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_feature_quality_flags.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_feature_quality_flags.py src/reckless_binance/features_4h.py
git commit -m "feat: add 4h event feature quality flags"
```

### Task 6: Regenerate the 4h artifacts after raw backfills

**Files:**
- Modify: `scripts/fetch_historical_4h.py`
- Modify: `scripts/build_market_bars_4h.py`
- Modify: `scripts/build_top_gainer_events_4h.py`
- Modify: `scripts/build_event_features_4h.py`
- Modify: `scripts/build_event_labels.py`
- Create: `tests/test_4h_regeneration_flow.py`

**Step 1: Write the failing test**

```python
def test_regeneration_flow_writes_all_artifacts_after_backfill():
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_4h_regeneration_flow.py -v`
Expected: FAIL because regeneration summary is not written

**Step 3: Write minimal implementation**

Add:
- regeneration summary output
- artifact row counts
- artifact timestamp ranges
- feature coverage summary write-out

Required outputs:
- `outputs_4h/top_gainer_events.parquet`
- `outputs_4h/event_features_4h.parquet`
- `outputs_4h/event_labels.parquet`
- `outputs_4h/breakout_blowoff_label_summary.csv`
- `outputs_4h/coverage_summary.csv`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_4h_regeneration_flow.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_4h_regeneration_flow.py scripts/fetch_historical_4h.py scripts/build_market_bars_4h.py scripts/build_top_gainer_events_4h.py scripts/build_event_features_4h.py scripts/build_event_labels.py
git commit -m "feat: add 4h regeneration summary outputs"
```

### Task 7: Produce a metric-readiness report for model work

**Files:**
- Create: `analysis/metric_readiness_report.py`
- Create: `tests/test_metric_readiness_report.py`

**Step 1: Write the failing test**

```python
def test_metric_readiness_report_groups_metrics_by_complete_partial_missing():
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_metric_readiness_report.py -v`
Expected: FAIL because no readiness report exists

**Step 3: Write minimal implementation**

Generate:
- `complete`
- `partial`
- `missing`

For target metrics:
- 4h candle data
- volume
- OI total / OI changes / OI z-score
- funding rate / rolling mean / z-score
- long/short ratio
- float / circulating supply / market cap estimate

Output:
- `outputs_4h/metric_readiness_report.csv`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_metric_readiness_report.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_metric_readiness_report.py analysis/metric_readiness_report.py
git commit -m "feat: add 4h metric readiness report"
```

### Task 8: Full verification

**Files:**
- Modify: `README.md`

**Step 1: Update documentation**

Document:
- what is complete
- what is partial
- what is missing
- which sources feed each metric
- expected caveats around Binance OI and late-start Bybit history

**Step 2: Run the full test suite**

Run: `pytest tests -q`
Expected: PASS

**Step 3: Run compile check**

Run: `python3 -m py_compile src/reckless_binance/*.py scripts/*.py analysis/*.py`
Expected: PASS

**Step 4: Run regeneration commands**

Run: `PYTHONPATH=src python3 scripts/fetch_historical_4h.py --start-date 2023-05-01 --end-date 2026-03-09 --output-root outputs_4h`

Run: `PYTHONPATH=src python3 scripts/build_market_bars_4h.py --binance-klines-path outputs_4h/raw/binance/klines_4h/data.parquet --bybit-klines-path outputs_4h/raw/bybit/klines_4h/data.parquet --output-path outputs_4h/raw/market_bars_4h.parquet`

Run: `PYTHONPATH=src python3 scripts/build_top_gainer_events_4h.py --asset-map-path outputs_4h/asset_map.parquet --binance-klines-path outputs_4h/raw/binance/klines_4h/data.parquet --bybit-klines-path outputs_4h/raw/bybit/klines_4h/data.parquet --output-path outputs_4h/top_gainer_events.parquet --top-n 10 --min-volume-quote 10000`

Run: `PYTHONPATH=src python3 scripts/build_event_features_4h.py --events-path outputs_4h/top_gainer_events.parquet --market-bars-path outputs_4h/raw/market_bars_4h.parquet --binance-oi-path outputs_4h/raw/binance/open_interest_4h/data.parquet --bybit-oi-path outputs_4h/raw/bybit/open_interest_4h/data.parquet --binance-funding-path outputs_4h/raw/binance/funding/data.parquet --bybit-funding-path outputs_4h/raw/bybit/funding/data.parquet --sentiment-path outputs_4h/raw/sentiment_4h.parquet --supply-path outputs_4h/raw/supply/history/data.parquet --output-path outputs_4h/event_features_4h.parquet`

Run: `PYTHONPATH=src python3 scripts/build_event_labels.py --feature-path outputs_4h/event_features_4h.parquet --labels-path outputs_4h/event_labels.parquet --summary-path outputs_4h/breakout_blowoff_label_summary.csv`

**Step 5: Commit**

```bash
git add README.md
git commit -m "docs: document 4h metric coverage and backfill workflow"
```
