# Binance Perp Top Gainers Event Study Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Python research pipeline that reconstructs weekly top gainers from Binance USDT-margined perpetual futures over the last 3 years and measures their 14-day forward-return and reversal behavior.

**Architecture:** A small package downloads Binance futures metadata and daily klines, builds a point-in-time active universe, computes first-entry top-20 events based on trailing 7-day returns, and writes analysis artifacts to disk. The first version is observational only and produces CSV and chart outputs instead of simulated trades.

**Tech Stack:** Python 3.11+, `requests`, `pandas`, `numpy`, `matplotlib`, `pytest`

---

### Task 1: Bootstrap the Research Project

**Files:**
- Create: `pyproject.toml`
- Create: `src/reckless_binance/__init__.py`
- Create: `src/reckless_binance/paths.py`
- Create: `tests/test_paths.py`

**Step 1: Write the failing test**

```python
from reckless_binance.paths import project_root, output_dir


def test_output_dir_is_inside_project_root():
    root = project_root()
    out = output_dir()

    assert out.parent == root
    assert out.name == "outputs"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_paths.py -v`
Expected: FAIL with `ModuleNotFoundError` for `reckless_binance`

**Step 3: Write minimal implementation**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "reckless-binance"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "matplotlib>=3.8",
  "numpy>=1.26",
  "pandas>=2.2",
  "requests>=2.32",
]

[project.optional-dependencies]
dev = ["pytest>=8.0"]

[tool.pytest.ini_options]
pythonpath = ["src"]
```

```python
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def output_dir() -> Path:
    return project_root() / "outputs"
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_paths.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pyproject.toml src/reckless_binance/__init__.py src/reckless_binance/paths.py tests/test_paths.py
git commit -m "chore: bootstrap binance research package"
```

### Task 2: Add Binance Futures Metadata and Daily Klines Client

**Files:**
- Create: `src/reckless_binance/binance_client.py`
- Create: `tests/test_binance_client.py`

**Step 1: Write the failing test**

```python
from reckless_binance.binance_client import active_usdt_perps_url, klines_url


def test_binance_urls_match_futures_endpoints():
    assert active_usdt_perps_url().endswith("/fapi/v1/exchangeInfo")
    assert klines_url().endswith("/fapi/v1/klines")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_binance_client.py -v`
Expected: FAIL with `ModuleNotFoundError` or missing functions

**Step 3: Write minimal implementation**

```python
BASE_URL = "https://fapi.binance.com"


def active_usdt_perps_url() -> str:
    return f"{BASE_URL}/fapi/v1/exchangeInfo"


def klines_url() -> str:
    return f"{BASE_URL}/fapi/v1/klines"
```

Then expand `binance_client.py` to include:

- `fetch_exchange_info(session)`
- `parse_active_usdt_perpetuals(payload)`
- `fetch_daily_klines(session, symbol, start_ms, end_ms)`
- retry and rate-limit handling

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_binance_client.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/binance_client.py tests/test_binance_client.py
git commit -m "feat: add binance futures market data client"
```

### Task 3: Build the Point-in-Time Universe and Event Dataset

**Files:**
- Create: `src/reckless_binance/universe.py`
- Create: `src/reckless_binance/events.py`
- Create: `tests/test_universe.py`
- Create: `tests/test_events.py`

**Step 1: Write the failing tests**

```python
import pandas as pd

from reckless_binance.events import build_top20_entry_events


def test_event_only_triggers_on_first_day_of_top20_entry():
    frame = pd.DataFrame(
        [
            {"date": "2026-01-08", "symbol": "AAAUSDT", "ret_7d": 0.50, "rank_7d": 5},
            {"date": "2026-01-09", "symbol": "AAAUSDT", "ret_7d": 0.48, "rank_7d": 7},
        ]
    )

    events = build_top20_entry_events(frame, top_n=20)
    assert list(events["date"]) == ["2026-01-08"]
```

```python
import pandas as pd

from reckless_binance.universe import eligible_on_date


def test_symbol_is_ineligible_before_onboard_date():
    row = {"symbol": "AAAUSDT", "onboard_date": "2026-01-10"}
    assert eligible_on_date(row, "2026-01-09") is False
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_universe.py tests/test_events.py -v`
Expected: FAIL because the functions do not exist

**Step 3: Write minimal implementation**

```python
def eligible_on_date(row, as_of_date):
    return as_of_date >= row["onboard_date"]
```

```python
def build_top20_entry_events(frame, top_n):
    top = frame[frame["rank_7d"] <= top_n].copy()
    top["prev_in_top"] = top.groupby("symbol")["rank_7d"].shift(1).le(top_n).fillna(False)
    return top.loc[~top["prev_in_top"]]
```

Then expand the modules to include:

- point-in-time active universe construction from exchange metadata
- rolling 7-day returns from daily close data
- top-20 ranking within each date
- repeat features:
  - `top20_streak_length`
  - `prior_top20_entries_30d`
  - `days_since_listing`

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_universe.py tests/test_events.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/universe.py src/reckless_binance/events.py tests/test_universe.py tests/test_events.py
git commit -m "feat: build top gainer event dataset"
```

### Task 4: Compute Pre-Event and Forward Return Paths

**Files:**
- Modify: `src/reckless_binance/events.py`
- Create: `tests/test_forward_returns.py`

**Step 1: Write the failing test**

```python
import pandas as pd

from reckless_binance.events import attach_forward_returns


def test_forward_returns_cover_day_1_through_day_14():
    prices = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=20, freq="D"),
            "symbol": ["AAAUSDT"] * 20,
            "close": [100 + i for i in range(20)],
        }
    )
    events = pd.DataFrame([{"date": pd.Timestamp("2026-01-06"), "symbol": "AAAUSDT"}])

    result = attach_forward_returns(events, prices, lookback_days=14, lookforward_days=14)

    assert "ret_+14" in result.columns
    assert result.loc[0, "ret_+1"] == (106 / 105) - 1
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_forward_returns.py -v`
Expected: FAIL because `attach_forward_returns` does not exist

**Step 3: Write minimal implementation**

```python
def attach_forward_returns(events, prices, lookback_days, lookforward_days):
    # Join each event to symbol-matched prices and compute returns relative to event-day close.
    ...
```

Implementation requirements:

- create `ret_-14` through `ret_-1`
- create `ret_+1` through `ret_+14`
- add `forward_14d_return`
- add `reversal_14d`
- handle missing post-event data without crashing

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_forward_returns.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/events.py tests/test_forward_returns.py
git commit -m "feat: add pre and forward return paths"
```

### Task 5: Build Aggregations and Visual Outputs

**Files:**
- Create: `src/reckless_binance/reporting.py`
- Create: `tests/test_reporting.py`

**Step 1: Write the failing test**

```python
import pandas as pd

from reckless_binance.reporting import summarize_forward_returns


def test_reversal_probability_is_share_of_negative_forward_returns():
    events = pd.DataFrame(
        [
            {"ret_+1": -0.01, "ret_+2": 0.02},
            {"ret_+1": 0.03, "ret_+2": -0.04},
        ]
    )

    summary = summarize_forward_returns(events, max_horizon=2)

    assert summary.loc[summary["horizon_day"] == 1, "reversal_probability"].item() == 0.5
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_reporting.py -v`
Expected: FAIL because `summarize_forward_returns` does not exist

**Step 3: Write minimal implementation**

```python
def summarize_forward_returns(events, max_horizon):
    rows = []
    for day in range(1, max_horizon + 1):
        column = f"ret_+{day}"
        values = events[column].dropna()
        rows.append(
            {
                "horizon_day": day,
                "reversal_probability": (values < 0).mean(),
            }
        )
    return pd.DataFrame(rows)
```

Then expand `reporting.py` to include:

- quartile bucket assignment by event-day `ret_7d`
- bucketed mean and median paths
- feature comparison table for `reversal_14d = 1` vs `0`
- chart rendering to `outputs/forward_return_buckets.png` or `.svg`
- CSV writers for all outputs

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_reporting.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/reporting.py tests/test_reporting.py
git commit -m "feat: add event study summaries and charts"
```

### Task 6: Add a Reproducible CLI Pipeline

**Files:**
- Create: `src/reckless_binance/cli.py`
- Create: `README.md`
- Create: `tests/test_cli.py`

**Step 1: Write the failing test**

```python
from reckless_binance.cli import build_parser


def test_cli_defaults_to_three_years_and_top20():
    args = build_parser().parse_args([])
    assert args.lookback_years == 3
    assert args.top_n == 20
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py -v`
Expected: FAIL because `build_parser` does not exist

**Step 3: Write minimal implementation**

```python
import argparse


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-years", type=int, default=3)
    parser.add_argument("--top-n", type=int, default=20)
    return parser
```

Then expand `cli.py` so `python -m reckless_binance.cli` does the full run:

- fetch and cache exchange metadata
- download daily klines for active symbols
- build the event dataset
- compute summaries and charts
- write artifacts under `outputs/`

Document in `README.md`:

- environment setup
- install commands
- example run command
- list of generated output files

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cli.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reckless_binance/cli.py README.md tests/test_cli.py
git commit -m "feat: add reproducible event study pipeline"
```

### Task 7: Verify the Full Pipeline End-to-End

**Files:**
- Modify: `README.md`
- Modify: `src/reckless_binance/cli.py`

**Step 1: Run the targeted test suite**

Run: `pytest tests -v`
Expected: PASS

**Step 2: Run the pipeline against live Binance data**

Run: `python -m reckless_binance.cli --lookback-years 3 --top-n 20`
Expected:

- raw market data cached locally
- event dataset written to `outputs/`
- summary CSV files written to `outputs/`
- chart file written to `outputs/`

**Step 3: Check the outputs exist**

Run: `ls outputs`
Expected to include:

- `events.csv`
- `forward_returns_by_day.csv`
- `bucket_paths.csv`
- `forward_return_buckets.svg`
- `summary.json`

**Step 4: Tighten any edge-case handling discovered in the live run**

Focus on:

- delisted or inactive contracts
- symbols with insufficient history
- missing candle days
- duplicate timestamps

**Step 5: Commit**

```bash
git add README.md src/reckless_binance/cli.py outputs
git commit -m "chore: verify binance top gainer event study pipeline"
```
