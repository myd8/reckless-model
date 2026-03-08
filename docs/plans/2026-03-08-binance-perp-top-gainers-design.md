# Binance Perp Top Gainers Event Study Design

**Date:** 2026-03-08

## Objective

Study whether Binance USDT-margined perpetual futures that become weekly top gainers tend to reverse over the next two weeks.

This is an observational research pipeline, not a trading system. The goal is to measure forward returns and reversal probability after a contract first enters the daily top-20 ranked by trailing 7-day return.

## Scope

- Universe: Binance USDT-margined perpetual futures only
- Frequency: daily candles only
- Lookback window: last 3 years
- Event rule: first day a contract enters the daily top-20 by trailing 7-day return
- Repeat behavior: treated as features, not duplicate events
- Forward window: day `+1` through day `+14`
- Pre-event context: day `-14` through day `-1`

## Approach Options

### Option 1: Fast Baseline

Use the current perp symbol list, download daily candles, reconstruct 7-day returns, and run the event study.

Pros:
- Fastest to build
- Lowest complexity

Cons:
- Survivorship bias
- Ignores listing and delisting timing

### Option 2: Point-in-Time Research Baseline

Build a point-in-time universe using Binance futures exchange metadata and onboard dates, then run the same event study only on contracts active on each date.

Pros:
- Better research quality
- More defensible event counts and rankings

Cons:
- More engineering work

### Option 3: Feature-Heavy First Version

Build the point-in-time study and immediately add regime and predictive feature work.

Pros:
- Richest output

Cons:
- Too much complexity before validating the core behavior

### Recommendation

Use Option 2. It preserves research quality without overbuilding the first version.

## Architecture

The pipeline should have four stages:

1. `universe builder`
   - Pull Binance USDT-margined perpetual metadata
   - Determine when each contract is active during the 3-year window

2. `market data loader`
   - Download daily klines for each active contract
   - Persist raw data locally for repeatable analysis

3. `event study engine`
   - Compute trailing 7-day returns
   - Rank the active universe each day
   - Mark first-entry top-20 events
   - Compute pre-event and forward return paths

4. `analysis layer`
   - Compute daily reversal probabilities
   - Build quartile bucket paths
   - Compare feature distributions for events that decline after the event vs those that do not

## Data Model

Use one row per event.

Each event row should include:

- `symbol`
- `event_date`
- `rank_7d`
- `ret_7d`
- `top20_streak_length`
- `prior_top20_entries_30d`
- `days_since_listing`
- `event_close`
- `event_volume`
- `event_quote_volume`
- `event_trade_count`
- `forward_14d_return`
- `reversal_14d`
- `ret_-14` through `ret_-1`
- `ret_+1` through `ret_+14`

Optional later fields:

- realized volatility over the prior week
- open interest or funding if added in a later phase

## Analysis Logic

The study should answer four questions.

### 1. What happens before and after the event?

For every event:

- compute pre-event return path from day `-14` to day `-1`
- compute forward return path from day `+1` to day `+14`

### 2. How often do weekly top gainers reverse?

For each forward day from `+1` to `+14`, compute:

- mean return
- median return
- share of events with return below `0`

The share below zero is the day-by-day reversal probability.

### 3. Does event strength matter?

Bucket events by event-day `ret_7d`:

- `75th percentile & up`
- `50th percentile - 75th percentile`
- `25th percentile - 50th percentile`
- `25th percentile and below`

For each bucket, plot:

- mean forward return
- median forward return

### 4. What is common among names that go down after the event?

Compare `reversal_14d = 1` vs `0` on:

- event-day `ret_7d`
- `top20_streak_length`
- `prior_top20_entries_30d`
- `days_since_listing`
- event-day volume, quote volume, and trade count
- prior-week realized volatility if added

## Outputs

The first version should generate:

- `events.csv`
- `forward_returns_by_day.csv`
- `bucket_paths.csv`
- a bucketed forward-return chart
- a compact summary report with reversal rates and feature comparisons

## Validation Rules

- Include only Binance USDT-margined perpetual futures
- Do not create an event before a contract has 7 full prior daily bars
- Track repeat presence in the top 20 as features, not duplicate events
- Stop forward-return windows cleanly at missing data or delisting boundaries

## Known Limits

- Results describe Binance USDT-margined perpetuals, not the full crypto market
- Listing and delisting handling is a major source of bias if implemented poorly
- No funding, fees, slippage, borrow, or execution assumptions are included in this phase
- This phase measures behavior after the event; it does not prove tradability
