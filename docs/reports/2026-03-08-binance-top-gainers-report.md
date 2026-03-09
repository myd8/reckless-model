# Binance Top Gainers Reversal Report

**Date:** 2026-03-08

**Universe:** Binance USDT-margined perpetual futures  
**Event definition:** First day a contract enters the daily top 20 by trailing 7-day return  
**Final structural signal:** `trade_count_top_half & quote_vol_top_half & ret_7d_top_half`

## Overall Signal Results

| Metric | Value |
| --- | ---: |
| Signal events | 1,891 |
| Unique symbols | 459 |
| Down by +1d | 1,154 |
| Down by +7d | 1,191 |
| Down by +14d | 1,211 |
| +14d down rate | 64.0% |
| Median +14d return | -10.40% |
| Mean +14d return | 1.07% |
| Median +1d return | -2.20% |
| Median +7d return | -6.12% |
| Median +14d return | -10.40% |

## By Event-Strength Tier

Tiers are quartiles of `ret_7d` inside the final signal subset.

| Tier | Events | Down by +14d | Down Rate | Median +14d | Mean +14d |
| --- | ---: | ---: | ---: | ---: | ---: |
| 75th percentile & up | 473 | 328 | 69.3% | -17.76% | -8.19% |
| 50th percentile - 75th percentile | 472 | 292 | 61.9% | -9.81% | 14.21% |
| 25th percentile - 50th percentile | 473 | 286 | 60.5% | -6.96% | -0.39% |
| 25th percentile and below | 473 | 305 | 64.5% | -8.66% | -1.24% |

## By Year

| Year | Events | Down by +14d | Down Rate | Median +14d | Mean +14d |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2023 | 277 | 140 | 50.5% | -0.68% | 6.44% |
| 2024 | 701 | 405 | 57.8% | -6.76% | 2.44% |
| 2025 | 785 | 580 | 73.9% | -16.58% | -0.34% |
| 2026 | 128 | 86 | 67.2% | -24.52% | -11.14% |

## Interpretation

- The idea is viable as a reversal study: the final signal set was down after 14 days in 64.0% of cases.
- The strongest subgroup is the highest event-strength tier, which had a 69.3% down rate and a -17.76% median 14-day return.
- The distribution is still skewed: the full-sample mean remains positive because some continuation winners are very large.
- The effect was much stronger in 2025 and has remained strong in 2026 so far.

## Caveats

- This is an observational event study, not a trade PnL backtest.
- There is no entry/exit execution model, fee model, funding model, or slippage model in this report.
- Results depend on Binance perp history and the reconstructed event universe used in the pipeline.
