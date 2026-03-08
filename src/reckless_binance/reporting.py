from __future__ import annotations

from html import escape
from pathlib import Path

import pandas as pd


def summarize_forward_returns(events: pd.DataFrame, max_horizon: int) -> pd.DataFrame:
    rows = []
    for day in range(1, max_horizon + 1):
        column = f"ret_+{day}"
        values = events[column].dropna()
        rows.append(
            {
                "horizon_day": day,
                "count": int(values.shape[0]),
                "mean_return": values.mean() if not values.empty else None,
                "median_return": values.median() if not values.empty else None,
                "reversal_probability": (values < 0).mean() if not values.empty else None,
            }
        )
    return pd.DataFrame(rows)


def assign_performance_buckets(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    labels = [
        "25th percentile and below",
        "25th percentile - 50th percentile",
        "50th percentile - 75th percentile",
        "75th percentile & up",
    ]
    frame["performance_bucket"] = pd.qcut(
        frame["ret_7d"],
        q=4,
        labels=labels,
        duplicates="drop",
    )
    return frame


def bucket_forward_paths(events: pd.DataFrame, max_horizon: int) -> pd.DataFrame:
    bucketed = assign_performance_buckets(events)
    rows = []
    for bucket, group in bucketed.groupby("performance_bucket", observed=True):
        for day in range(1, max_horizon + 1):
            column = f"ret_+{day}"
            values = group[column].dropna()
            rows.append(
                {
                    "bucket": bucket,
                    "horizon_day": day,
                    "count": int(values.shape[0]),
                    "mean_return": values.mean() if not values.empty else None,
                    "median_return": values.median() if not values.empty else None,
                }
            )
    return pd.DataFrame(rows)


def compare_reversal_groups(events: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    rows = []
    for feature in feature_columns:
        reversed_values = events.loc[events["reversal_14d"] == 1, feature].dropna()
        continued_values = events.loc[events["reversal_14d"] == 0, feature].dropna()
        rows.append(
            {
                "feature": feature,
                "reversed_mean": reversed_values.mean() if not reversed_values.empty else None,
                "reversed_median": reversed_values.median() if not reversed_values.empty else None,
                "continued_mean": continued_values.mean() if not continued_values.empty else None,
                "continued_median": continued_values.median() if not continued_values.empty else None,
            }
        )
    return pd.DataFrame(rows)


def render_forward_return_chart(bucket_rows: pd.DataFrame, out_path: Path) -> None:
    if bucket_rows.empty:
        out_path.write_text(
            "<svg xmlns='http://www.w3.org/2000/svg' width='600' height='300'>"
            "<text x='20' y='40'>No bucketed forward-return data available.</text>"
            "</svg>",
            encoding="utf-8",
        )
        return

    width, height = 1100, 700
    left, right, top, bottom = 90, 250, 50, 90
    plot_width = width - left - right
    plot_height = height - top - bottom

    value_columns = ["mean_return", "median_return"]
    value_series = bucket_rows[value_columns].stack().dropna()
    y_min = float(value_series.min())
    y_max = float(value_series.max())
    if y_min == y_max:
        y_min -= 0.01
        y_max += 0.01

    max_horizon = int(bucket_rows["horizon_day"].max())

    def x_scale(day: int) -> float:
        if max_horizon == 1:
            return left + plot_width / 2
        return left + ((day - 1) / (max_horizon - 1)) * plot_width

    def y_scale(value: float) -> float:
        return top + ((y_max - value) / (y_max - y_min)) * plot_height

    bucket_order = [
        "75th percentile & up",
        "50th percentile - 75th percentile",
        "25th percentile - 50th percentile",
        "25th percentile and below",
    ]
    colors = {
        "75th percentile & up": "#c23b22",
        "50th percentile - 75th percentile": "#e08e0b",
        "25th percentile - 50th percentile": "#1f77b4",
        "25th percentile and below": "#2e8b57",
    }

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff" />',
        '<text x="90" y="28" font-size="22" font-family="Arial" fill="#111">Forward Returns After Top-20 Entry Events</text>',
        '<text x="90" y="50" font-size="13" font-family="Arial" fill="#555">Solid lines show means. Dashed lines show medians. Buckets are event-day 7-day return quartiles.</text>',
    ]

    for index in range(6):
        y_value = y_min + ((y_max - y_min) * index / 5)
        y = y_scale(y_value)
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_width}" y2="{y:.2f}" stroke="#e6e6e6" stroke-width="1" />')
        parts.append(f'<text x="{left - 12}" y="{y + 4:.2f}" text-anchor="end" font-size="12" font-family="Arial" fill="#444">{y_value * 100:.1f}%</text>')

    for day in range(1, max_horizon + 1):
        x = x_scale(day)
        parts.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_height}" stroke="#f3f3f3" stroke-width="1" />')
        parts.append(f'<text x="{x:.2f}" y="{top + plot_height + 28}" text-anchor="middle" font-size="12" font-family="Arial" fill="#444">+{day}</text>')

    zero_y = y_scale(0.0)
    parts.append(f'<line x1="{left}" y1="{zero_y:.2f}" x2="{left + plot_width}" y2="{zero_y:.2f}" stroke="#999" stroke-width="1.5" />')
    parts.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#444" stroke-width="1.5" />')
    parts.append(f'<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#444" stroke-width="1.5" />')

    legend_x = left + plot_width + 24
    legend_y = 120
    for index, bucket in enumerate(bucket_order):
        subset = bucket_rows.loc[bucket_rows["bucket"] == bucket].sort_values("horizon_day")
        if subset.empty:
            continue

        for style_name, dashed in [("mean_return", False), ("median_return", True)]:
            points = []
            for row in subset.itertuples(index=False):
                value = getattr(row, style_name)
                if pd.isna(value):
                    continue
                points.append(f"{x_scale(int(row.horizon_day)):.2f},{y_scale(float(value)):.2f}")
            if points:
                dash = ' stroke-dasharray="8 5"' if dashed else ""
                parts.append(
                    f'<polyline fill="none" stroke="{colors[bucket]}" stroke-width="2.5"{dash} points="{" ".join(points)}" />'
                )

        y = legend_y + index * 90
        parts.append(f'<line x1="{legend_x}" y1="{y}" x2="{legend_x + 32}" y2="{y}" stroke="{colors[bucket]}" stroke-width="3" />')
        parts.append(f'<line x1="{legend_x}" y1="{y + 18}" x2="{legend_x + 32}" y2="{y + 18}" stroke="{colors[bucket]}" stroke-width="3" stroke-dasharray="8 5" />')
        parts.append(f'<text x="{legend_x + 40}" y="{y + 4}" font-size="12" font-family="Arial" fill="#222">{escape(bucket)} mean</text>')
        parts.append(f'<text x="{legend_x + 40}" y="{y + 22}" font-size="12" font-family="Arial" fill="#222">{escape(bucket)} median</text>')

    parts.append("</svg>")
    out_path.write_text("\n".join(parts), encoding="utf-8")
