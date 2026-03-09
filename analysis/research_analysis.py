"""
Reckless Mean-Reversion Strategy — Research Analysis
=====================================================
Generates all visualizations and analysis for the top-20 weekly gainer
mean-reversion hypothesis in crypto perpetual futures.
"""

import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

# ── Paths ─────────────────────────────────────────────────────────────
BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT     = os.path.join(BASE, "outputs")
PLOTS   = os.path.join(OUT, "research_plots")
os.makedirs(PLOTS, exist_ok=True)

# ── Style ─────────────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor": "#0d1117",
    "axes.facecolor":   "#161b22",
    "axes.edgecolor":   "#30363d",
    "axes.labelcolor":  "#c9d1d9",
    "text.color":       "#c9d1d9",
    "xtick.color":      "#8b949e",
    "ytick.color":      "#8b949e",
    "grid.color":       "#21262d",
    "grid.linestyle":   "--",
    "grid.alpha":       0.6,
    "font.family":      "sans-serif",
    "font.size":        11,
    "figure.dpi":       150,
})
ACCENT   = "#58a6ff"
NEGATIVE = "#f85149"
POSITIVE = "#3fb950"
WARN     = "#d29922"

# ═══════════════════════════════════════════════════════════════════════
# TASK 2 — Forward Return Analysis
# ═══════════════════════════════════════════════════════════════════════
print("▸ Loading forward_returns_by_day.csv …")
fwd = pd.read_csv(os.path.join(OUT, "forward_returns_by_day.csv"))

# 2-1  Median forward return vs horizon
fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(fwd["horizon_day"], fwd["median_return"] * 100, color=NEGATIVE, alpha=0.85, edgecolor="#21262d")
ax.axhline(0, color="#8b949e", lw=0.8)
ax.set_xlabel("Horizon (days)")
ax.set_ylabel("Median Forward Return (%)")
ax.set_title("Median Forward Return vs Horizon Day", fontweight="bold", fontsize=14)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f%%"))
ax.grid(True)
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "median_return_vs_horizon.png"))
plt.close(fig)
print("  ✓ median_return_vs_horizon.png")

# 2-2  Mean forward return vs horizon
fig, ax = plt.subplots(figsize=(10, 5))
colors = [POSITIVE if v >= 0 else NEGATIVE for v in fwd["mean_return"]]
ax.bar(fwd["horizon_day"], fwd["mean_return"] * 100, color=colors, alpha=0.85, edgecolor="#21262d")
ax.axhline(0, color="#8b949e", lw=0.8)
ax.set_xlabel("Horizon (days)")
ax.set_ylabel("Mean Forward Return (%)")
ax.set_title("Mean Forward Return vs Horizon Day", fontweight="bold", fontsize=14)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f%%"))
ax.grid(True)
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "mean_return_vs_horizon.png"))
plt.close(fig)
print("  ✓ mean_return_vs_horizon.png")

# 2-3  Reversal probability vs horizon
fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(fwd["horizon_day"], fwd["reversal_probability"] * 100,
        marker="o", color=ACCENT, lw=2.5, markersize=7, zorder=3)
ax.axhline(50, color=WARN, ls="--", lw=1.2, label="50 % (coin flip)")
ax.fill_between(fwd["horizon_day"], 50, fwd["reversal_probability"] * 100,
                alpha=0.15, color=ACCENT)
ax.set_xlabel("Horizon (days)")
ax.set_ylabel("Reversal Probability (%)")
ax.set_title("Reversal Probability vs Horizon Day", fontweight="bold", fontsize=14)
ax.legend(facecolor="#161b22", edgecolor="#30363d")
ax.set_ylim(45, 70)
ax.grid(True)
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "reversal_probability_vs_horizon.png"))
plt.close(fig)
print("  ✓ reversal_probability_vs_horizon.png")

# 2-4  Mean − Median (tail skew)
fig, ax = plt.subplots(figsize=(10, 5))
diff = (fwd["mean_return"] - fwd["median_return"]) * 100
ax.bar(fwd["horizon_day"], diff, color=WARN, alpha=0.85, edgecolor="#21262d")
ax.axhline(0, color="#8b949e", lw=0.8)
ax.set_xlabel("Horizon (days)")
ax.set_ylabel("Mean − Median Return (%)")
ax.set_title("Mean − Median Return (Tail-Effect Indicator)", fontweight="bold", fontsize=14)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f%%"))
ax.grid(True)
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "mean_minus_median_vs_horizon.png"))
plt.close(fig)
print("  ✓ mean_minus_median_vs_horizon.png")

# 2-5  Cumulative median return path
fig, ax = plt.subplots(figsize=(10, 5))
cum_med = fwd["median_return"].values
ax.plot(fwd["horizon_day"], cum_med * 100, marker="s", color=NEGATIVE, lw=2.5, markersize=7)
ax.fill_between(fwd["horizon_day"], 0, cum_med * 100, alpha=0.15, color=NEGATIVE)
ax.axhline(0, color="#8b949e", lw=0.8)
ax.set_xlabel("Horizon (days)")
ax.set_ylabel("Median Return (%)")
ax.set_title("Median Return Path After Top-20 Gainer Event", fontweight="bold", fontsize=14)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f%%"))
ax.grid(True)
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "cumulative_median_return_path.png"))
plt.close(fig)
print("  ✓ cumulative_median_return_path.png")

# ═══════════════════════════════════════════════════════════════════════
# TASK 3 — Distribution Analysis
# ═══════════════════════════════════════════════════════════════════════
print("\n▸ Loading events.csv for distribution analysis …")
events = pd.read_csv(os.path.join(OUT, "events.csv"))

# 3-1  Histogram of 7-day forward returns
ret7 = events["ret_+7"].dropna()
fig, ax = plt.subplots(figsize=(10, 5))
ax.hist(ret7 * 100, bins=120, color=ACCENT, alpha=0.75, edgecolor="#21262d")
ax.axvline(ret7.median() * 100, color=NEGATIVE, ls="--", lw=1.5, label=f"Median = {ret7.median()*100:.2f}%")
ax.axvline(ret7.mean() * 100, color=POSITIVE, ls="--", lw=1.5, label=f"Mean = {ret7.mean()*100:.2f}%")
ax.axvline(0, color="#8b949e", lw=0.8)
ax.set_xlabel("7-Day Forward Return (%)")
ax.set_ylabel("Count")
ax.set_title("Distribution of 7-Day Forward Returns", fontweight="bold", fontsize=14)
ax.legend(facecolor="#161b22", edgecolor="#30363d")
ax.set_xlim(-80, 120)
ax.grid(True)
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "histogram_7d_forward_return.png"))
plt.close(fig)
print("  ✓ histogram_7d_forward_return.png")

# 3-2  Boxplot of forward returns by horizon
ret_cols = [c for c in events.columns if c.startswith("ret_+")]
ret_data = events[ret_cols].copy()
ret_data.columns = [c.replace("ret_+", "Day ") for c in ret_cols]
melted = ret_data.melt(var_name="Horizon", value_name="Return")
melted["Return"] *= 100
melted = melted.dropna()

fig, ax = plt.subplots(figsize=(12, 6))
sns.boxplot(data=melted, x="Horizon", y="Return", ax=ax,
            flierprops=dict(marker=".", markerfacecolor="#8b949e", markersize=2, alpha=0.3),
            boxprops=dict(facecolor="#21262d", edgecolor=ACCENT),
            medianprops=dict(color=NEGATIVE, lw=2),
            whiskerprops=dict(color="#8b949e"),
            capprops=dict(color="#8b949e"))
ax.axhline(0, color=WARN, ls="--", lw=1)
ax.set_xlabel("Horizon")
ax.set_ylabel("Forward Return (%)")
ax.set_title("Forward Return Distribution by Horizon", fontweight="bold", fontsize=14)
ax.set_ylim(-100, 150)
ax.grid(True, axis="y")
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "boxplot_forward_returns_by_horizon.png"))
plt.close(fig)
print("  ✓ boxplot_forward_returns_by_horizon.png")

# 3-3  Percentile table
pct_rows = []
for c in ret_cols:
    day = c.replace("ret_+", "")
    s = events[c].dropna()
    pct_rows.append({
        "horizon": f"Day {day}",
        "p5":  s.quantile(0.05),
        "p25": s.quantile(0.25),
        "p50": s.quantile(0.50),
        "p75": s.quantile(0.75),
        "p95": s.quantile(0.95),
        "mean": s.mean(),
        "count": len(s),
    })
pct_df = pd.DataFrame(pct_rows)
for col in ["p5","p25","p50","p75","p95","mean"]:
    pct_df[col] = (pct_df[col] * 100).round(2)
pct_df.to_csv(os.path.join(PLOTS, "percentile_table.csv"), index=False)
print("  ✓ percentile_table.csv")
print(pct_df.to_string(index=False))

# ═══════════════════════════════════════════════════════════════════════
# TASK 5 — Walk-Forward Evaluation
# ═══════════════════════════════════════════════════════════════════════
print("\n▸ Loading walk-forward data …")
wf  = pd.read_csv(os.path.join(OUT, "walk_forward_results.csv"))
wfs = pd.read_csv(os.path.join(OUT, "walk_forward_summary.csv"))

# Keep only the rows that were selected each window
sel = wf[wf["selected"] == True].copy()
sel["test_month_dt"] = pd.to_datetime(sel["test_month"])
sel = sel.sort_values("test_month_dt")

# 5-1  Equity curve (cumulative OOS median return)
sel["cum_median"] = sel["test_forward_14d_median"].cumsum()

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(sel["test_month_dt"], sel["cum_median"] * 100,
        marker="o", color=ACCENT, lw=2, markersize=5, zorder=3)
ax.fill_between(sel["test_month_dt"], 0, sel["cum_median"] * 100,
                alpha=0.12, color=ACCENT)
ax.axhline(0, color="#8b949e", lw=0.8)
ax.set_xlabel("OOS Month")
ax.set_ylabel("Cumulative Median 14d Return (%)")
ax.set_title("Walk-Forward Equity Curve (Cumulative OOS Median Return)",
             fontweight="bold", fontsize=14)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax.grid(True)
fig.autofmt_xdate()
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "walk_forward_equity_curve.png"))
plt.close(fig)
print("  ✓ walk_forward_equity_curve.png")

# 5-2  Rolling OOS performance by window
fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

# Median 14d return per window
c_med = [NEGATIVE if v < 0 else POSITIVE for v in sel["test_forward_14d_median"]]
axes[0].bar(sel["test_month_dt"], sel["test_forward_14d_median"] * 100,
            width=25, color=c_med, alpha=0.85, edgecolor="#21262d")
axes[0].axhline(0, color="#8b949e", lw=0.8)
axes[0].set_ylabel("OOS Median 14d Return (%)")
axes[0].set_title("Walk-Forward: OOS Performance by Window", fontweight="bold", fontsize=14)
axes[0].yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
axes[0].grid(True)

# Reversal rate per window
axes[1].bar(sel["test_month_dt"], sel["test_reversal_rate"] * 100,
            width=25, color=ACCENT, alpha=0.75, edgecolor="#21262d")
axes[1].axhline(50, color=WARN, ls="--", lw=1.2)
axes[1].set_ylabel("OOS Reversal Rate (%)")
axes[1].set_xlabel("OOS Month")
axes[1].yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
axes[1].grid(True)

fig.autofmt_xdate()
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "walk_forward_rolling_performance.png"))
plt.close(fig)
print("  ✓ walk_forward_rolling_performance.png")

# 5-3  OOS summary metrics
print("\n── Walk-Forward Summary ──")
print(wfs.to_string(index=False))

# Compute additional OOS stats
n_windows = len(sel)
win_rate = (sel["test_forward_14d_median"] < 0).mean()
avg_rev  = sel["test_reversal_rate"].mean()
avg_med  = sel["test_forward_14d_median"].mean()
avg_mean = sel["test_forward_14d_mean"].mean()

oos_summary_rows = []
for _, r in wfs.iterrows():
    oos_summary_rows.append({
        "Filter": r["filter_name"],
        "Times Selected": int(r["selected_count"]),
        "Avg OOS Reversal": f"{r['avg_test_reversal_rate']*100:.1f}%",
        "Median OOS 14d Ret": f"{r['median_test_forward_14d_median']*100:.2f}%",
        "Avg OOS 14d Mean": f"{r['avg_test_forward_14d_mean']*100:.2f}%",
        "Total OOS Events": int(r["total_test_count"]),
    })
oos_df = pd.DataFrame(oos_summary_rows)
oos_df.to_csv(os.path.join(PLOTS, "walk_forward_oos_summary.csv"), index=False)
print("  ✓ walk_forward_oos_summary.csv")

print(f"\n  OOS Windows:  {n_windows}")
print(f"  Win % (median < 0): {win_rate*100:.1f}%")
print(f"  Avg Reversal Rate:  {avg_rev*100:.1f}%")
print(f"  Avg OOS Median 14d: {avg_med*100:.2f}%")
print(f"  Avg OOS Mean 14d:   {avg_mean*100:.2f}%")

# ═══════════════════════════════════════════════════════════════════════
# Additional: Bucket path analysis
# ═══════════════════════════════════════════════════════════════════════
print("\n▸ Plotting bucket path analysis …")
bp = pd.read_csv(os.path.join(OUT, "bucket_paths.csv"))

fig, ax = plt.subplots(figsize=(10, 6))
palette = {"25th percentile and below": POSITIVE,
           "25th percentile - 50th percentile": ACCENT,
           "50th percentile - 75th percentile": WARN,
           "75th percentile & up": NEGATIVE}

for bucket, grp in bp.groupby("bucket"):
    grp = grp.sort_values("horizon_day")
    ax.plot(grp["horizon_day"], grp["median_return"] * 100,
            marker="o", lw=2, markersize=5,
            color=palette.get(bucket, "#8b949e"),
            label=bucket)

ax.axhline(0, color="#8b949e", lw=0.8)
ax.set_xlabel("Horizon (days)")
ax.set_ylabel("Median Forward Return (%)")
ax.set_title("Median Return Path by 7d-Gain Percentile Bucket", fontweight="bold", fontsize=14)
ax.legend(fontsize=9, facecolor="#161b22", edgecolor="#30363d", loc="lower left")
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f%%"))
ax.grid(True)
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "bucket_median_return_paths.png"))
plt.close(fig)
print("  ✓ bucket_median_return_paths.png")

# ═══════════════════════════════════════════════════════════════════════
# Additional: Filter OOS heatmap
# ═══════════════════════════════════════════════════════════════════════
print("\n▸ Plotting filter OOS comparison …")
filt = pd.read_csv(os.path.join(OUT, "filter_oos_results.csv"))

fig, ax = plt.subplots(figsize=(14, 7))
filt_sorted = filt.sort_values("test_reversal_rate", ascending=True)

y_pos = range(len(filt_sorted))
bars = ax.barh(y_pos, filt_sorted["test_reversal_rate"] * 100,
               color=ACCENT, alpha=0.8, edgecolor="#21262d")
ax.axvline(50, color=WARN, ls="--", lw=1.2, label="50% baseline")
ax.set_yticks(y_pos)
ax.set_yticklabels(filt_sorted["filter_name"], fontsize=8)
ax.set_xlabel("OOS Reversal Rate (%)")
ax.set_title("Filter Performance: OOS Reversal Rate", fontweight="bold", fontsize=14)
ax.legend(facecolor="#161b22", edgecolor="#30363d")
ax.grid(True, axis="x")
fig.tight_layout()
fig.savefig(os.path.join(PLOTS, "filter_oos_reversal_rates.png"))
plt.close(fig)
print("  ✓ filter_oos_reversal_rates.png")

# ═══════════════════════════════════════════════════════════════════════
print(f"\n✅  All plots saved to: {PLOTS}")
print(f"    Total files generated: {len(os.listdir(PLOTS))}")
