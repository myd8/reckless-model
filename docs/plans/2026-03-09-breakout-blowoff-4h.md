# Breakout vs Blowoff 4H Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a historical 4h top-gainer event pipeline across Binance and Bybit that ingests raw exchange data, caches parquet artifacts, constructs event feature panels, and labels events as breakout, delayed_blowoff, immediate_reversal, or unclassified.

**Architecture:** Use modular source adapters for Binance, Bybit, and supply data, persist raw parquet first, then build derived artifacts in separate pipeline stages. Keep missing source fields nullable and compute final features and labels from local parquet caches rather than from direct live joins.

**Tech Stack:** Python 3.11+, `pandas`, `numpy`, `requests`, `pyarrow`, `pytest`

---

### Task 1: Create Artifact Paths and Dataset Contracts

**Files:**
- Create: `src/reckless_binance/artifacts.py`
- Create: `tests/test_artifacts.py`

**Step 1: Write the failing test**

```python
from reckless_binance.artifacts import derived_artifact_paths


def test_derived_artifact_paths_include_required_outputs():
    paths = derived_artifact_paths()
    assert "top_gainer_events" in paths
    assert paths["event_labels"].name == "event_labels.parquet"
```

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_artifacts.py -v`
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write minimal implementation**

```python
from pathlib import Path

from reckless_binance.paths import output_dir, project_root


def derived_artifact_paths() -> dict[str, Path]:
    data_dir = project_root() / "data"
    return {
        "asset_map": data_dir / "asset_map.parquet",
        "top_gainer_events": data_dir / "top_gainer_events.parquet",
        "event_features_4h": data_dir / "event_features_4h.parquet",
        "event_labels": data_dir / "event_labels.parquet",
        "label_summary": output_dir() / "breakout_blowoff_label_summary.csv",
    }
```

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_artifacts.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/artifacts.py tests/test_artifacts.py
git commit -m "feat: add breakout-blowoff artifact contracts"
```

### Task 2: Add Asset Mapping and Raw Source Schemas

**Files:**
- Create: `src/reckless_binance/asset_map.py`
- Create: `src/reckless_binance/schemas.py`
- Create: `tests/test_asset_map.py`

**Step 1: Write the failing test**

```python
import pandas as pd

from reckless_binance.asset_map import build_asset_map


def test_build_asset_map_aligns_binance_bybit_and_supply_ids():
    binance = pd.DataFrame([{"asset": "BTC", "binance_symbol": "BTCUSDT"}])
    bybit = pd.DataFrame([{"asset": "BTC", "bybit_symbol": "BTCUSDT"}])
    supply = pd.DataFrame([{"asset": "BTC", "supply_id": "bitcoin"}])

    asset_map = build_asset_map(binance, bybit, supply)

    assert asset_map.loc[0, "asset"] == "BTC"
    assert asset_map.loc[0, "supply_id"] == "bitcoin"
```

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_asset_map.py -v`
Expected: FAIL with missing module or function

**Step 3: Write minimal implementation**

```python
def build_asset_map(binance, bybit, supply):
    merged = binance.merge(bybit, on="asset", how="outer")
    return merged.merge(supply, on="asset", how="left")
```

Then expand to:

- canonical asset naming rules
- symbol normalization
- parquet writer for `asset_map.parquet`
- schema constants for raw and derived datasets

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_asset_map.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/asset_map.py src/reckless_binance/schemas.py tests/test_asset_map.py
git commit -m "feat: add canonical asset mapping"
```

### Task 3: Implement Raw Historical Ingestion Clients

**Files:**
- Create: `src/reckless_binance/binance_historical.py`
- Create: `src/reckless_binance/bybit_historical.py`
- Create: `src/reckless_binance/supply_historical.py`
- Create: `tests/test_historical_clients.py`

**Step 1: Write the failing test**

```python
from reckless_binance.binance_historical import kline_interval
from reckless_binance.bybit_historical import bybit_interval


def test_historical_clients_use_4h_intervals():
    assert kline_interval() == "4h"
    assert bybit_interval() == "240"
```

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_historical_clients.py -v`
Expected: FAIL with missing imports

**Step 3: Write minimal implementation**

```python
def kline_interval() -> str:
    return "4h"


def bybit_interval() -> str:
    return "240"
```

Then expand to include:

- paginated historical 4h kline download
- historical OI download
- historical funding download
- historical sentiment / long-short ratio when available
- raw parquet persistence under source-specific folders
- source coverage metadata

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_historical_clients.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/binance_historical.py src/reckless_binance/bybit_historical.py src/reckless_binance/supply_historical.py tests/test_historical_clients.py
git commit -m "feat: add historical source ingestion clients"
```

### Task 4: Build 4H Top-Gainer Event Dataset

**Files:**
- Create: `src/reckless_binance/events_4h.py`
- Create: `tests/test_events_4h.py`

**Step 1: Write the failing test**

```python
import pandas as pd

from reckless_binance.events_4h import build_top_gainer_events_4h


def test_build_top_gainer_events_4h_marks_first_entry_bar():
    frame = pd.DataFrame(
        [
            {"timestamp": "2026-01-01 00:00:00", "asset": "AAA", "ret_7d": 0.20, "rank_7d": 25},
            {"timestamp": "2026-01-01 04:00:00", "asset": "AAA", "ret_7d": 0.35, "rank_7d": 10},
            {"timestamp": "2026-01-01 08:00:00", "asset": "AAA", "ret_7d": 0.36, "rank_7d": 9},
        ]
    )

    events = build_top_gainer_events_4h(frame, top_n=20)
    assert len(events) == 1
```

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_events_4h.py -v`
Expected: FAIL

**Step 3: Write minimal implementation**

```python
def build_top_gainer_events_4h(frame, top_n):
    ...
```

Implementation requirements:

- reuse the existing Reckless first-entry top-gainer logic
- operate on `4h` bars instead of daily
- write `top_gainer_events.parquet`

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_events_4h.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/events_4h.py tests/test_events_4h.py
git commit -m "feat: add 4h top gainer event builder"
```

### Task 5: Build Event Feature Panels

**Files:**
- Create: `src/reckless_binance/features_4h.py`
- Create: `tests/test_features_4h.py`

**Step 1: Write the failing test**

```python
import pandas as pd

from reckless_binance.features_4h import normalize_oi_usd


def test_normalize_oi_usd_uses_close_notional():
    frame = pd.DataFrame([{"open_interest": 10, "close": 1000.0}])
    out = normalize_oi_usd(frame, oi_column="open_interest", close_column="close")
    assert out.iloc[0] == 10000.0
```

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_features_4h.py -v`
Expected: FAIL

**Step 3: Write minimal implementation**

```python
def normalize_oi_usd(frame, oi_column, close_column):
    return frame[oi_column] * frame[close_column]
```

Then expand to include:

- symmetric `-84` to `+84` bar panel builder
- exchange joins by canonical asset and timestamp
- total and share OI features
- OI delta and z-score features
- funding and funding mean features
- supply / float joins
- parquet writer for `event_features_4h.parquet`

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_features_4h.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/features_4h.py tests/test_features_4h.py
git commit -m "feat: add 4h event feature panel builder"
```

### Task 6: Compute Labels and Breakout/Blowoff Summary

**Files:**
- Create: `src/reckless_binance/labels.py`
- Create: `tests/test_labels.py`

**Step 1: Write the failing test**

```python
from reckless_binance.labels import classify_event


def test_classify_event_breakout_priority():
    row = {
        "ret_1d": 0.06,
        "ret_3d": 0.02,
        "ret_7d": 0.03,
        "drawdown_from_peak_to_day7": -0.05,
    }
    assert classify_event(row) == "breakout"
```

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_labels.py -v`
Expected: FAIL

**Step 3: Write minimal implementation**

```python
def classify_event(row):
    ...
```

Implementation requirements:

- compute path metrics at 1d, 3d, 7d, 14d
- apply label priority exactly as specified
- write `event_labels.parquet`
- write `outputs/breakout_blowoff_label_summary.csv`

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_labels.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/labels.py tests/test_labels.py
git commit -m "feat: add breakout blowoff labels"
```

### Task 7: Add Stage Scripts and Reproducible Pipeline Entrypoints

**Files:**
- Create: `scripts/fetch_historical_4h.py`
- Create: `scripts/build_top_gainer_events_4h.py`
- Create: `scripts/build_event_features_4h.py`
- Create: `scripts/build_event_labels.py`
- Modify: `README.md`
- Create: `tests/test_stage_scripts.py`

**Step 1: Write the failing test**

```python
from pathlib import Path


def test_stage_scripts_exist():
    assert Path("scripts/fetch_historical_4h.py").exists()
    assert Path("scripts/build_event_labels.py").exists()
```

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_stage_scripts.py -v`
Expected: FAIL

**Step 3: Write minimal implementation**

Create the stage scripts with:

- argument parsing
- raw parquet write stage
- event parquet write stage
- feature parquet write stage
- label parquet and CSV write stage

Update `README.md` with:

- required APIs
- raw cache layout
- stage commands
- output artifact descriptions

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests/test_stage_scripts.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add scripts README.md tests/test_stage_scripts.py
git commit -m "feat: add stage scripts for breakout blowoff pipeline"
```

### Task 8: Verify End-to-End Artifact Generation

**Files:**
- Modify: `README.md`
- Modify: `scripts/*.py`

**Step 1: Run the full test suite**

Run: `PYTHONPATH=src ~/.local/bin/pytest tests -v`
Expected: PASS

**Step 2: Run ingestion stage**

Run: `PYTHONPATH=src python3 scripts/fetch_historical_4h.py`
Expected:

- raw parquet files written under source-specific directories

**Step 3: Run event and feature stages**

Run: `PYTHONPATH=src python3 scripts/build_top_gainer_events_4h.py`

Run: `PYTHONPATH=src python3 scripts/build_event_features_4h.py`

Run: `PYTHONPATH=src python3 scripts/build_event_labels.py`

Expected:

- `top_gainer_events.parquet`
- `asset_map.parquet`
- `event_features_4h.parquet`
- `event_labels.parquet`
- `outputs/breakout_blowoff_label_summary.csv`

**Step 4: Inspect coverage and nullability**

Verify:

- missing Binance OI history does not drop rows
- exchange share columns sum sensibly when both sources exist
- label distribution is non-empty

**Step 5: Commit**

```bash
git add README.md scripts src tests data outputs
git commit -m "chore: verify breakout blowoff historical pipeline"
```
