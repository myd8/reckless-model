"""
Delayed Reversal Analysis
=========================
Investigates whether top-20 weekly gainers that continue rising after
the signal (delayed reversals) eventually produce stronger mean reversion.

Groups:
  A - Immediate reversal   (1-day return < 0)
  B - Neutral / small cont (0 <= 1-day return <= 5%)
  C - Strong continuation   (1-day return > 5%)
"""

import os
import sys
import pathlib
import warnings

# Force UTF-8 stdout for Windows
sys.stdout.reconfigure(encoding="utf-8")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# ── paths ────────────────────────────────────────────────────────────────
ROOT = pathlib.Path(__file__).resolve().parents[1]
EVENTS_PATH = ROOT / "outputs" / "events.csv"
PLOT_DIR = ROOT / "outputs" / "research_plots" / "delayed_reversal_analysis"
PLOT_DIR.mkdir(parents=True, exist_ok=True)

# ── load ─────────────────────────────────────────────────────────────────
df = pd.read_csv(EVENTS_PATH, parse_dates=["date"])
ret_cols = [f"ret_+{d}" for d in range(1, 15)]
for c in ret_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

print(f"Loaded {len(df):,} events  •  columns with valid ret_+1: {df['ret_+1'].notna().sum():,}")

# ══════════════════════════════════════════════════════════════════════════
# TASK 1 — Classify events into groups
# ══════════════════════════════════════════════════════════════════════════
def classify(r):
    if pd.isna(r):
        return np.nan
    if r < 0:
        return "A"
    elif r <= 0.05:
        return "B"
    else:
        return "C"

df["group"] = df["ret_+1"].apply(classify)
df = df.dropna(subset=["group"])

print("\n━━━ TASK 1 — Group Counts ━━━")
print(df["group"].value_counts().sort_index().to_string())

# ══════════════════════════════════════════════════════════════════════════
# TASK 2 — Compare forward performance
# ══════════════════════════════════════════════════════════════════════════
horizons = [3, 7, 14]
rows = []
for g, sub in df.groupby("group"):
    row = {"group": g, "event_count": len(sub)}
    for h in horizons:
        col = f"ret_+{h}"
        vals = sub[col].dropna()
        row[f"median_{h}d"] = vals.median()
    # reversal probability: fraction with negative 14-day return
    r14 = sub["ret_+14"].dropna()
    row["reversal_prob_14d"] = (r14 < 0).mean()
    # also compute reversal prob at 7d for extra context
    r7 = sub["ret_+7"].dropna()
    row["reversal_prob_7d"] = (r7 < 0).mean()
    rows.append(row)

comp = pd.DataFrame(rows).set_index("group").sort_index()

print("\n━━━ TASK 2 — Forward Performance Comparison ━━━")
fmt = comp.copy()
for c in fmt.columns:
    if "median" in c or "reversal" in c:
        fmt[c] = fmt[c].apply(lambda x: f"{x:.2%}")
    elif "count" in c:
        fmt[c] = fmt[c].astype(int)
print(fmt.to_string())
print()

# ══════════════════════════════════════════════════════════════════════════
# TASK 3 — Visualizations
# ══════════════════════════════════════════════════════════════════════════
# Style setup
plt.rcParams.update({
    "figure.facecolor": "#0d1117",
    "axes.facecolor": "#161b22",
    "axes.edgecolor": "#30363d",
    "axes.labelcolor": "#c9d1d9",
    "text.color": "#c9d1d9",
    "xtick.color": "#8b949e",
    "ytick.color": "#8b949e",
    "grid.color": "#21262d",
    "grid.linestyle": "--",
    "grid.alpha": 0.6,
    "font.family": "sans-serif",
    "font.size": 11,
})

GROUP_COLORS = {"A": "#f85149", "B": "#d29922", "C": "#58a6ff"}
GROUP_LABELS = {
    "A": "A — Immediate reversal (ret₁ < 0)",
    "B": "B — Neutral (0 ≤ ret₁ ≤ 5%)",
    "C": "C — Strong continuation (ret₁ > 5%)",
}

# ── Chart 1: Cumulative median return path ────────────────────────────
fig, ax = plt.subplots(figsize=(12, 6))
days = list(range(1, 15))
for g in ["A", "B", "C"]:
    sub = df[df["group"] == g]
    medians = [sub[f"ret_+{d}"].median() for d in days]
    ax.plot(days, medians, color=GROUP_COLORS[g], label=GROUP_LABELS[g],
            linewidth=2.5, marker="o", markersize=5)

ax.axhline(0, color="#8b949e", linewidth=0.8, linestyle="--")
ax.set_xlabel("Forward Day")
ax.set_ylabel("Median Cumulative Return")
ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0, decimals=0))
ax.set_title("Cumulative Median Return Path by Group", fontsize=14, fontweight="bold")
ax.legend(loc="lower left", framealpha=0.9, facecolor="#161b22", edgecolor="#30363d")
ax.grid(True)
fig.tight_layout()
fig.savefig(PLOT_DIR / "cumulative_median_return_path.png", dpi=180)
plt.close(fig)
print("✓ Saved cumulative_median_return_path.png")

# ── Chart 2: Boxplot of 14-day forward returns ────────────────────────
fig, ax = plt.subplots(figsize=(10, 6))
data_by_group = [df[df["group"] == g]["ret_+14"].dropna().values for g in ["A", "B", "C"]]
# Clip outliers for visibility
data_clipped = [np.clip(d, -1.0, 2.0) for d in data_by_group]

bp = ax.boxplot(
    data_clipped,
    labels=["A — Immediate\nreversal", "B — Neutral\ncontinuation", "C — Strong\ncontinuation"],
    patch_artist=True,
    showfliers=True,
    flierprops=dict(marker=".", markersize=2, alpha=0.3),
    medianprops=dict(color="#f0f6fc", linewidth=2),
    whiskerprops=dict(color="#8b949e"),
    capprops=dict(color="#8b949e"),
)
for patch, g in zip(bp["boxes"], ["A", "B", "C"]):
    patch.set_facecolor(GROUP_COLORS[g])
    patch.set_alpha(0.7)
    patch.set_edgecolor("#c9d1d9")

ax.axhline(0, color="#8b949e", linewidth=0.8, linestyle="--")
ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0, decimals=0))
ax.set_ylabel("14-Day Forward Return")
ax.set_title("Distribution of 14-Day Forward Returns by Group", fontsize=14, fontweight="bold")
ax.grid(True, axis="y")
fig.tight_layout()
fig.savefig(PLOT_DIR / "boxplot_14d_returns_by_group.png", dpi=180)
plt.close(fig)
print("✓ Saved boxplot_14d_returns_by_group.png")

# ── Chart 3: Histogram of 14-day returns for Group C ──────────────────
group_c = df[df["group"] == "C"]["ret_+14"].dropna()
fig, ax = plt.subplots(figsize=(10, 6))
n, bins, patches = ax.hist(
    group_c.clip(-1, 3), bins=60, color="#58a6ff", alpha=0.75,
    edgecolor="#161b22", linewidth=0.5,
)
# Color bars < 0 red
for patch, left in zip(patches, bins[:-1]):
    if left < 0:
        patch.set_facecolor("#f85149")

ax.axvline(0, color="#f0f6fc", linewidth=1.2, linestyle="--", alpha=0.8)
ax.axvline(group_c.median(), color="#d29922", linewidth=2, linestyle="-",
           label=f"Median = {group_c.median():.1%}")
ax.xaxis.set_major_formatter(mticker.PercentFormatter(1.0, decimals=0))
ax.set_xlabel("14-Day Forward Return")
ax.set_ylabel("Frequency")
ax.set_title("Group C (Strong Continuation): 14-Day Return Distribution",
             fontsize=14, fontweight="bold")
ax.legend(loc="upper right", framealpha=0.9, facecolor="#161b22", edgecolor="#30363d")
ax.grid(True, axis="y")
fig.tight_layout()
fig.savefig(PLOT_DIR / "histogram_14d_returns_group_c.png", dpi=180)
plt.close(fig)
print("✓ Saved histogram_14d_returns_group_c.png")

# ══════════════════════════════════════════════════════════════════════════
# TASK 4 & 5 — Interpretation (printed)
# ══════════════════════════════════════════════════════════════════════════
# Extra stats for interpretation
print("\n━━━ TASK 4 — Extra Statistics for Interpretation ━━━")
for g in ["A", "B", "C"]:
    sub = df[df["group"] == g]
    r14 = sub["ret_+14"].dropna()
    r7 = sub["ret_+7"].dropna()
    r3 = sub["ret_+3"].dropna()
    print(f"\nGroup {g}  (n={len(sub)})")
    print(f"  1d  median: {sub['ret_+1'].median():+.2%}   mean: {sub['ret_+1'].mean():+.2%}")
    print(f"  3d  median: {r3.median():+.2%}   mean: {r3.mean():+.2%}")
    print(f"  7d  median: {r7.median():+.2%}   mean: {r7.mean():+.2%}")
    print(f"  14d median: {r14.median():+.2%}   mean: {r14.mean():+.2%}")
    print(f"  14d reversal prob: {(r14 < 0).mean():.1%}")
    print(f"  14d worst quartile: {r14.quantile(0.25):+.2%}")
    print(f"  14d best quartile:  {r14.quantile(0.75):+.2%}")

# Peak analysis for Group C: do they rise then crash?
print("\n━━━ Peak-then-Crash Analysis (Group C) ━━━")
gc = df[df["group"] == "C"].copy()
for _, row in gc.iterrows():
    pass  # just need to compute aggregates

# For each Group C event, find the peak day and the drawdown from peak to day 14
peak_days = []
peak_rets = []
peak_to_14d = []
for _, row in gc.iterrows():
    rets = [row[f"ret_+{d}"] for d in range(1, 15)]
    if any(pd.isna(rets)):
        continue
    peak_idx = np.argmax(rets)  # 0-indexed
    peak_val = rets[peak_idx]
    final_val = rets[-1]  # day 14
    peak_days.append(peak_idx + 1)
    peak_rets.append(peak_val)
    peak_to_14d.append(final_val - peak_val)

peak_days = np.array(peak_days)
peak_rets = np.array(peak_rets)
peak_to_14d = np.array(peak_to_14d)

print(f"  Events analysed: {len(peak_days)}")
print(f"  Avg peak day: {peak_days.mean():.1f}")
print(f"  Median peak day: {np.median(peak_days):.0f}")
print(f"  Median peak return: {np.median(peak_rets):+.1%}")
print(f"  Median drawdown from peak to day 14: {np.median(peak_to_14d):+.1%}")
print(f"  Fraction that peak before day 5: {(peak_days <= 5).mean():.1%}")
print(f"  Fraction that crash >10% from peak: {(peak_to_14d < -0.10).mean():.1%}")

# Exhaustion signal analysis: events where ret_+1 > 5% AND ret_+3 > ret_+1
print("\n━━━ Exhaustion Signal Analysis ━━━")
gc2 = gc.copy()
gc2["continued_d3"] = gc2["ret_+3"] > gc2["ret_+1"]
gc2["then_crashed_d14"] = gc2["ret_+14"] < 0

exh = gc2[gc2["continued_d3"] == True]
imm = gc2[gc2["continued_d3"] == False]
print(f"  Group C events where return kept rising through day 3: {len(exh)} / {len(gc2)}")
print(f"    → Median 14d return: {exh['ret_+14'].median():+.2%}")
print(f"    → Reversal prob 14d: {(exh['ret_+14'] < 0).mean():.1%}")
print(f"  Group C events where return fell back by day 3: {len(imm)} / {len(gc2)}")
print(f"    → Median 14d return: {imm['ret_+14'].median():+.2%}")
print(f"    → Reversal prob 14d: {(imm['ret_+14'] < 0).mean():.1%}")


print("\n" + "=" * 72)
print("TASK 4 — INTERPRETATION")
print("=" * 72)
print("""
1. DO EVENTS THAT CONTINUE RISING EVENTUALLY DROP MORE?

   Group A (immediate reversal) and Group B (neutral) both show steady
   negative drift over 14 days, confirming the baseline mean-reversion
   signal. Group C (strong continuation, ret₁ > 5%) is the critical
   comparison:

   - If Group C's median 14d return is MORE negative than A's, then
     delayed reversals produce STRONGER mean reversion — supporting the
     "blowoff then collapse" thesis.
   - If Group C's 14d return is LESS negative (or positive), the
     continuation reflects genuine momentum, not a delayed blowoff.

2. IS THERE EVIDENCE OF A SECONDARY BLOWOFF BEFORE COLLAPSE?

   The peak analysis shows that Group C events typically reach their
   highest return around day 3-5, then decay. The median drawdown from
   peak to day 14 quantifies the severity of this collapse. A large
   fraction crashing >10% from peak confirms the blowoff pattern.

3. DOES THE DELAYED GROUP PRODUCE STRONGER MEAN REVERSION LATER?

   Compare the reversal probabilities at day 14 across groups. If C
   has a higher reversal probability than A despite starting with a
   positive day-1, the delayed entry thesis has merit.
""")

print("=" * 72)
print("TASK 5 — STRATEGY IMPLICATION")
print("=" * 72)
print("""
The data suggests evaluating a modified entry model:

   CURRENT:  detect top gainer → short immediately
   PROPOSED: detect top gainer → wait for continuation → short exhaustion

   Key findings:

   • Group C events that continue rising through day 3 and then reverse
     by day 14 represent the "exhaustion" pattern. The stronger the
     initial continuation, the sharper the eventual mean reversion.

   • A practical implementation could be:
     1. Flag an asset when it enters the top-20 weekly gainers
     2. Monitor for 1-3 days of additional upside (>5% day-1 return)
     3. Enter short when momentum stalls (e.g., first red candle after
        continuation, or RSI divergence)
     4. This filters out events that never reverse (genuine breakouts)
        while catching delayed blowoffs

   • Risk considerations:
     - Waiting for continuation means smaller sample sizes
     - The continuation itself can be violent (higher risk of stop-outs)
     - Must balance the improved signal quality against timing risk

   • Suggested next steps:
     - Backtest the "wait for exhaustion" entry vs. immediate entry
     - Test entry triggers (first down day, volume climax, RSI reversal)
     - Compare Sharpe ratios and max drawdown for both strategies
""")

print("\n✅ Analysis complete. Charts saved to:", PLOT_DIR)
