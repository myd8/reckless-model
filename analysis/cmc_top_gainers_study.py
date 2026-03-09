#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import getpass
import hashlib
import json
import math
import os
import pathlib
import statistics
import sys
import time
import urllib.parse
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests


BASE_URL = "https://pro-api.coinmarketcap.com"
CACHE_DIR = pathlib.Path("outputs/cache/cmc")
OUTPUT_DIR = pathlib.Path("outputs")
USD = "USD"


def iso_day(value: dt.date) -> str:
    return f"{value.isoformat()}T00:00:00Z"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CoinMarketCap top-gainer event study")
    parser.add_argument("--days", type=int, default=365, help="Analysis window length ending today.")
    parser.add_argument("--top-n", type=int, default=10, help="Daily top-gainer set size after filters.")
    parser.add_argument("--lookback", type=int, default=14, help="Days before event.")
    parser.add_argument("--lookforward", type=int, default=14, help="Days after event.")
    parser.add_argument("--limit", type=int, default=200, help="Listings pulled per day before filtering.")
    parser.add_argument("--min-market-cap", type=float, default=10_000_000, help="Minimum USD market cap.")
    parser.add_argument("--min-volume", type=float, default=1_000_000, help="Minimum 24h USD volume.")
    parser.add_argument("--sleep", type=float, default=0.25, help="Seconds to sleep between API calls.")
    return parser.parse_args()


def ensure_dirs() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_api_key() -> str:
    api_key = os.getenv("CMC_API_KEY")
    if api_key:
        return api_key.strip()
    prompt = "CoinMarketCap API key: "
    return getpass.getpass(prompt).strip()


def percentile(values: Sequence[float], p: float) -> float:
    if not values:
        raise ValueError("percentile requires non-empty values")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * p
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    lower_weight = upper - rank
    upper_weight = rank - lower
    return ordered[lower] * lower_weight + ordered[upper] * upper_weight


def median(values: Sequence[float]) -> Optional[float]:
    values = list(values)
    if not values:
        return None
    return statistics.median(values)


def mean(values: Sequence[float]) -> Optional[float]:
    values = list(values)
    if not values:
        return None
    return statistics.fmean(values)


class CMCClient:
    def __init__(self, api_key: str, sleep_seconds: float) -> None:
        self.api_key = api_key
        self.sleep_seconds = sleep_seconds

    def _cache_path(self, path: str, params: Dict[str, object]) -> pathlib.Path:
        raw = json.dumps({"path": path, "params": params}, sort_keys=True)
        name = hashlib.sha256(raw.encode("utf-8")).hexdigest() + ".json"
        return CACHE_DIR / name

    def get(self, path: str, params: Dict[str, object]) -> object:
        cache_path = self._cache_path(path, params)
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))

        url = urllib.parse.urljoin(BASE_URL, path)
        response = requests.get(
            url,
            params=params,
            headers={
                "Accepts": "application/json",
                "X-CMC_PRO_API_KEY": self.api_key,
            },
            timeout=60,
        )
        payload = response.json()
        if response.status_code != 200:
            status = payload.get("status", {})
            message = status.get("error_message") or response.text
            raise RuntimeError(f"{path} failed ({response.status_code}): {message}")
        status = payload.get("status", {})
        if status.get("error_code", 0) != 0:
            raise RuntimeError(f"{path} failed: {status.get('error_message')}")
        data = payload.get("data")
        cache_path.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(self.sleep_seconds)
        return data


def get_key_info(client: CMCClient) -> object:
    return client.get("/v1/key/info", {})


def historical_listings(
    client: CMCClient,
    date_value: dt.date,
    limit: int,
) -> List[dict]:
    data = client.get(
        "/v1/cryptocurrency/listings/historical",
        {
            "date": iso_day(date_value),
            "convert": USD,
            "sort": "percent_change_7d",
            "sort_dir": "desc",
            "limit": limit,
        },
    )
    if not isinstance(data, list):
        raise RuntimeError("Unexpected listings response format")
    return data


def historical_ohlcv(
    client: CMCClient,
    coin_id: int,
    start_date: dt.date,
    end_date: dt.date,
) -> Dict[dt.date, float]:
    data = client.get(
        "/v2/cryptocurrency/ohlcv/historical",
        {
            "id": coin_id,
            "convert": USD,
            "time_start": iso_day(start_date),
            "time_end": iso_day(end_date),
            "interval": "daily",
        },
    )
    coin_payload = None
    if isinstance(data, dict):
        coin_payload = data.get(str(coin_id)) or data.get(coin_id)
    if not coin_payload:
        raise RuntimeError(f"Unexpected OHLCV response for coin id {coin_id}")
    prices: Dict[dt.date, float] = {}
    for row in coin_payload.get("quotes", []):
        quote = row.get("quote", {}).get(USD, {})
        close_value = quote.get("close")
        time_close = row.get("time_close") or row.get("time_open")
        if close_value is None or not time_close:
            continue
        date_key = dt.datetime.fromisoformat(time_close.replace("Z", "+00:00")).date()
        prices[date_key] = float(close_value)
    return prices


def info_batch(client: CMCClient, ids: Sequence[int]) -> Dict[int, dict]:
    if not ids:
        return {}
    data = client.get(
        "/v2/cryptocurrency/info",
        {
            "id": ",".join(str(coin_id) for coin_id in ids),
            "aux": "date_added,platform,category,description,logo,urls",
        },
    )
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected info response format")
    result: Dict[int, dict] = {}
    for key, value in data.items():
        result[int(key)] = value
    return result


def safe_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_daily_top_sets(
    client: CMCClient,
    start_date: dt.date,
    end_date: dt.date,
    limit: int,
    top_n: int,
    min_market_cap: float,
    min_volume: float,
) -> Tuple[List[dict], Dict[dt.date, List[int]]]:
    events_source: List[dict] = []
    top_sets: Dict[dt.date, List[int]] = {}
    day_count = (end_date - start_date).days + 1
    for offset in range(day_count):
        current_date = start_date + dt.timedelta(days=offset)
        listings = historical_listings(client, current_date, limit)
        filtered: List[dict] = []
        for coin in listings:
            quote = coin.get("quote", {}).get(USD, {})
            pct_7d = safe_float(quote.get("percent_change_7d"))
            market_cap = safe_float(quote.get("market_cap"))
            volume_24h = safe_float(quote.get("volume_24h"))
            price = safe_float(quote.get("price"))
            if pct_7d is None or market_cap is None or volume_24h is None or price is None:
                continue
            if market_cap < min_market_cap or volume_24h < min_volume:
                continue
            filtered.append(
                {
                    "event_date": current_date.isoformat(),
                    "id": int(coin["id"]),
                    "name": coin.get("name"),
                    "symbol": coin.get("symbol"),
                    "cmc_rank": coin.get("cmc_rank"),
                    "price": price,
                    "percent_change_7d": pct_7d,
                    "market_cap": market_cap,
                    "volume_24h": volume_24h,
                    "volume_market_cap_ratio": volume_24h / market_cap if market_cap else None,
                }
            )
        top_slice = filtered[:top_n]
        events_source.extend(top_slice)
        top_sets[current_date] = [row["id"] for row in top_slice]
    return events_source, top_sets


def build_entry_events(events_source: Sequence[dict], top_sets: Dict[dt.date, List[int]]) -> List[dict]:
    rows_by_date: Dict[dt.date, Dict[int, dict]] = defaultdict(dict)
    for row in events_source:
        event_date = dt.date.fromisoformat(row["event_date"])
        rows_by_date[event_date][row["id"]] = dict(row)
    entry_events: List[dict] = []
    for current_date, ids in sorted(top_sets.items()):
        previous_ids = set(top_sets.get(current_date - dt.timedelta(days=1), []))
        for coin_id in ids:
            if coin_id in previous_ids:
                continue
            row = rows_by_date[current_date][coin_id]
            row["entry_type"] = "new_top_gainer"
            entry_events.append(row)
    return entry_events


def fetch_coin_info(client: CMCClient, coin_ids: Sequence[int]) -> Dict[int, dict]:
    result: Dict[int, dict] = {}
    batch: List[int] = []
    for coin_id in coin_ids:
        batch.append(coin_id)
        if len(batch) == 100:
            result.update(info_batch(client, batch))
            batch = []
    if batch:
        result.update(info_batch(client, batch))
    return result


def fetch_price_histories(
    client: CMCClient,
    events: Sequence[dict],
    lookback: int,
    lookforward: int,
) -> Dict[int, Dict[dt.date, float]]:
    windows: Dict[int, Tuple[dt.date, dt.date]] = {}
    for event in events:
        coin_id = event["id"]
        event_date = dt.date.fromisoformat(event["event_date"])
        start = event_date - dt.timedelta(days=lookback)
        end = event_date + dt.timedelta(days=lookforward)
        if coin_id not in windows:
            windows[coin_id] = (start, end)
            continue
        old_start, old_end = windows[coin_id]
        windows[coin_id] = (min(old_start, start), max(old_end, end))
    histories: Dict[int, Dict[dt.date, float]] = {}
    for coin_id, (start, end) in windows.items():
        histories[coin_id] = historical_ohlcv(client, coin_id, start, end)
    return histories


def annotate_events(
    events: Sequence[dict],
    histories: Dict[int, Dict[dt.date, float]],
    coin_info: Dict[int, dict],
    lookback: int,
    lookforward: int,
) -> List[dict]:
    enriched: List[dict] = []
    for event in events:
        event_date = dt.date.fromisoformat(event["event_date"])
        prices = histories.get(event["id"], {})
        event_price = prices.get(event_date)
        if not event_price:
            continue
        row = dict(event)
        row["event_price"] = event_price
        pre_price = prices.get(event_date - dt.timedelta(days=lookback))
        if pre_price:
            row[f"return_m{lookback}"] = (event_price / pre_price) - 1.0
        info = coin_info.get(event["id"], {})
        date_added = info.get("date_added")
        if date_added:
            added_date = dt.datetime.fromisoformat(date_added.replace("Z", "+00:00")).date()
            row["coin_age_days"] = (event_date - added_date).days
        row["category"] = info.get("category")
        row["platform_name"] = (info.get("platform") or {}).get("name")
        for horizon in range(-lookback, lookforward + 1):
            target_date = event_date + dt.timedelta(days=horizon)
            target_price = prices.get(target_date)
            key = f"ret_{horizon:+d}"
            row[key] = None if not target_price else (target_price / event_price) - 1.0
        row["forward_14d"] = row.get(f"ret_{lookforward:+d}")
        if row["forward_14d"] is not None:
            row["reversal_14d"] = int(row["forward_14d"] < 0)
        enriched.append(row)
    return enriched


def bucket_name(value: float, q25: float, q50: float, q75: float) -> str:
    if value >= q75:
        return "75th percentile & up"
    if value >= q50:
        return "50th percentile - 75th percentile"
    if value >= q25:
        return "25th percentile - 50th percentile"
    return "25th percentile and below"


def write_csv(path: pathlib.Path, rows: Iterable[dict], fieldnames: Sequence[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def summarize_forward_returns(events: Sequence[dict], lookforward: int) -> List[dict]:
    rows = []
    for day in range(1, lookforward + 1):
        values = [event.get(f"ret_{day:+d}") for event in events if event.get(f"ret_{day:+d}") is not None]
        negative = [value for value in values if value < 0]
        rows.append(
            {
                "horizon_day": day,
                "count": len(values),
                "mean_return": mean(values),
                "median_return": median(values),
                "reversal_probability": (len(negative) / len(values)) if values else None,
            }
        )
    return rows


def build_bucket_paths(events: Sequence[dict], lookforward: int) -> Tuple[List[dict], Dict[str, object]]:
    scores = [event["percent_change_7d"] for event in events]
    q25 = percentile(scores, 0.25)
    q50 = percentile(scores, 0.50)
    q75 = percentile(scores, 0.75)
    buckets = [
        "75th percentile & up",
        "50th percentile - 75th percentile",
        "25th percentile - 50th percentile",
        "25th percentile and below",
    ]
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for event in events:
        grouped[bucket_name(event["percent_change_7d"], q25, q50, q75)].append(event)
    rows: List[dict] = []
    bucket_sizes: Dict[str, int] = {}
    for bucket in buckets:
        bucket_events = grouped.get(bucket, [])
        bucket_sizes[bucket] = len(bucket_events)
        for day in range(1, lookforward + 1):
            values = [event.get(f"ret_{day:+d}") for event in bucket_events if event.get(f"ret_{day:+d}") is not None]
            rows.append(
                {
                    "bucket": bucket,
                    "horizon_day": day,
                    "count": len(values),
                    "mean_return": mean(values),
                    "median_return": median(values),
                }
            )
    meta = {"q25": q25, "q50": q50, "q75": q75, "bucket_sizes": bucket_sizes}
    return rows, meta


def compare_groups(events: Sequence[dict]) -> Dict[str, object]:
    reversed_events = [event for event in events if event.get("reversal_14d") == 1]
    continued_events = [event for event in events if event.get("reversal_14d") == 0]
    metrics = [
        "percent_change_7d",
        "return_m14",
        "market_cap",
        "volume_24h",
        "volume_market_cap_ratio",
        "coin_age_days",
    ]
    summary: Dict[str, object] = {
        "reversed_count": len(reversed_events),
        "continued_count": len(continued_events),
    }
    for metric in metrics:
        reverse_values = [event.get(metric) for event in reversed_events if event.get(metric) is not None]
        continue_values = [event.get(metric) for event in continued_events if event.get(metric) is not None]
        summary[metric] = {
            "reversed_median": median(reverse_values),
            "continued_median": median(continue_values),
            "reversed_mean": mean(reverse_values),
            "continued_mean": mean(continue_values),
        }
    reversal_by_category: Dict[str, List[int]] = defaultdict(list)
    for event in events:
        key = event.get("category") or "Unknown"
        reversal_by_category[key].append(event.get("reversal_14d", 0))
    summary["category_reversal_rates"] = {
        key: mean(values) for key, values in reversal_by_category.items() if len(values) >= 5
    }
    return summary


def svg_polyline(points: Sequence[Tuple[float, float]], color: str, dashed: bool) -> str:
    encoded = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
    dash_attr = ' stroke-dasharray="8 5"' if dashed else ""
    return f'<polyline fill="none" stroke="{color}" stroke-width="2"{dash_attr} points="{encoded}" />'


def render_svg(bucket_rows: Sequence[dict], out_path: pathlib.Path) -> None:
    width, height = 1100, 700
    left, right, top, bottom = 90, 220, 40, 90
    plot_width = width - left - right
    plot_height = height - top - bottom
    rows = [row for row in bucket_rows if row["mean_return"] is not None or row["median_return"] is not None]
    y_values = []
    for row in rows:
        if row["mean_return"] is not None:
            y_values.append(row["mean_return"])
        if row["median_return"] is not None:
            y_values.append(row["median_return"])
    if not y_values:
        raise RuntimeError("No data available to render SVG chart")
    y_min = min(y_values)
    y_max = max(y_values)
    if y_min == y_max:
        y_min -= 0.01
        y_max += 0.01

    def x_scale(day: int) -> float:
        return left + ((day - 1) / 13.0) * plot_width

    def y_scale(value: float) -> float:
        return top + ((y_max - value) / (y_max - y_min)) * plot_height

    bucket_order = [
        "75th percentile & up",
        "50th percentile - 75th percentile",
        "25th percentile - 50th percentile",
        "25th percentile and below",
    ]
    colors = {
        "75th percentile & up": "#d94841",
        "50th percentile - 75th percentile": "#f28e2b",
        "25th percentile - 50th percentile": "#4e79a7",
        "25th percentile and below": "#59a14f",
    }
    bucket_points: Dict[str, Dict[str, List[Tuple[float, float]]]] = defaultdict(lambda: {"mean": [], "median": []})
    for row in rows:
        bucket = row["bucket"]
        day = int(row["horizon_day"])
        if row["mean_return"] is not None:
            bucket_points[bucket]["mean"].append((x_scale(day), y_scale(float(row["mean_return"]))))
        if row["median_return"] is not None:
            bucket_points[bucket]["median"].append((x_scale(day), y_scale(float(row["median_return"]))))

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff" />',
        '<text x="90" y="25" font-size="22" font-family="Arial" fill="#111">Forward Returns After Top-Gainer Entry Events</text>',
        '<text x="90" y="48" font-size="13" font-family="Arial" fill="#555">Solid = mean, dashed = median. Buckets are based on event-day 7-day performance quartiles.</text>',
    ]
    for i in range(6):
        y_value = y_min + ((y_max - y_min) * i / 5)
        y = y_scale(y_value)
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_width}" y2="{y:.2f}" stroke="#e0e0e0" stroke-width="1" />')
        parts.append(f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="12" font-family="Arial" fill="#444">{y_value * 100:.1f}%</text>')
    for day in range(1, 15):
        x = x_scale(day)
        parts.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_height}" stroke="#f2f2f2" stroke-width="1" />')
        parts.append(f'<text x="{x:.2f}" y="{top + plot_height + 25}" text-anchor="middle" font-size="12" font-family="Arial" fill="#444">+{day}</text>')
    zero_y = y_scale(0.0)
    parts.append(f'<line x1="{left}" y1="{zero_y:.2f}" x2="{left + plot_width}" y2="{zero_y:.2f}" stroke="#999" stroke-width="1.5" />')
    parts.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#444" stroke-width="1.5" />')
    parts.append(f'<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#444" stroke-width="1.5" />')
    for bucket in bucket_order:
        parts.append(svg_polyline(bucket_points[bucket]["mean"], colors[bucket], dashed=False))
        parts.append(svg_polyline(bucket_points[bucket]["median"], colors[bucket], dashed=True))
    legend_x = left + plot_width + 25
    legend_y = 110
    for index, bucket in enumerate(bucket_order):
        y = legend_y + (index * 80)
        color = colors[bucket]
        parts.append(f'<line x1="{legend_x}" y1="{y}" x2="{legend_x + 30}" y2="{y}" stroke="{color}" stroke-width="3" />')
        parts.append(f'<line x1="{legend_x}" y1="{y + 18}" x2="{legend_x + 30}" y2="{y + 18}" stroke="{color}" stroke-width="3" stroke-dasharray="8 5" />')
        parts.append(f'<text x="{legend_x + 40}" y="{y + 4}" font-size="12" font-family="Arial" fill="#222">{bucket} mean</text>')
        parts.append(f'<text x="{legend_x + 40}" y="{y + 22}" font-size="12" font-family="Arial" fill="#222">{bucket} median</text>')
    parts.append('</svg>')
    out_path.write_text("\n".join(parts), encoding="utf-8")


def write_summary(
    key_info: object,
    entry_events: Sequence[dict],
    forward_summary: Sequence[dict],
    bucket_meta: Dict[str, object],
    group_summary: Dict[str, object],
    out_path: pathlib.Path,
    args: argparse.Namespace,
) -> None:
    valid_events = [event for event in entry_events if event.get("forward_14d") is not None]
    ret_m14 = [event.get("return_m14") for event in valid_events if event.get("return_m14") is not None]
    ret_p14 = [event.get("forward_14d") for event in valid_events if event.get("forward_14d") is not None]
    summary = {
        "run_timestamp_utc": dt.datetime.utcnow().isoformat() + "Z",
        "assumptions": {
            "analysis_days": args.days,
            "top_n": args.top_n,
            "lookback_days": args.lookback,
            "lookforward_days": args.lookforward,
            "min_market_cap": args.min_market_cap,
            "min_volume": args.min_volume,
            "entry_event_rule": "coin enters filtered top-gainer set today and was not in the set yesterday",
            "bucket_rule": "quartiles of event-day percent_change_7d",
        },
        "key_info": key_info,
        "sample": {
            "entry_events_total": len(entry_events),
            "entry_events_with_complete_forward_window": len(valid_events),
            "median_return_pre_14d": median(ret_m14),
            "mean_return_pre_14d": mean(ret_m14),
            "median_return_post_14d": median(ret_p14),
            "mean_return_post_14d": mean(ret_p14),
        },
        "forward_returns_by_day": list(forward_summary),
        "bucket_meta": bucket_meta,
        "reversal_group_comparison": group_summary,
    }
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    ensure_dirs()
    api_key = get_api_key()
    if not api_key:
        raise SystemExit("No API key provided")

    today = dt.date.today()
    start_date = today - dt.timedelta(days=args.days - 1)
    client = CMCClient(api_key=api_key, sleep_seconds=args.sleep)

    print("Checking API key permissions...", file=sys.stderr)
    key_info = get_key_info(client)

    print("Pulling historical top-gainer sets...", file=sys.stderr)
    try:
        events_source, top_sets = build_daily_top_sets(
            client=client,
            start_date=start_date,
            end_date=today,
            limit=args.limit,
            top_n=args.top_n,
            min_market_cap=args.min_market_cap,
            min_volume=args.min_volume,
        )
    except RuntimeError as exc:
        message = str(exc)
        if "doesn't support this endpoint" in message:
            print("This CoinMarketCap subscription tier does not include the required historical endpoint.", file=sys.stderr)
            print("Required endpoint: /v1/cryptocurrency/listings/historical", file=sys.stderr)
            print("The analysis cannot proceed with a CMC-only workflow until the plan is upgraded.", file=sys.stderr)
            return 2
        raise

    print("Deriving entry events...", file=sys.stderr)
    entry_events = build_entry_events(events_source, top_sets)
    if not entry_events:
        raise SystemExit("No entry events found under current filters")

    coin_ids = sorted({event["id"] for event in entry_events})
    print(f"Fetching metadata for {len(coin_ids)} coins...", file=sys.stderr)
    coin_info = fetch_coin_info(client, coin_ids)

    print("Fetching price histories...", file=sys.stderr)
    histories = fetch_price_histories(
        client=client,
        events=entry_events,
        lookback=args.lookback,
        lookforward=args.lookforward,
    )

    print("Computing event study outputs...", file=sys.stderr)
    enriched_events = annotate_events(
        events=entry_events,
        histories=histories,
        coin_info=coin_info,
        lookback=args.lookback,
        lookforward=args.lookforward,
    )
    if not enriched_events:
        raise SystemExit("No events had complete event-day prices")

    forward_summary = summarize_forward_returns(enriched_events, args.lookforward)
    bucket_rows, bucket_meta = build_bucket_paths(enriched_events, args.lookforward)
    group_summary = compare_groups(enriched_events)

    event_fields = [
        "event_date",
        "id",
        "symbol",
        "name",
        "cmc_rank",
        "percent_change_7d",
        "market_cap",
        "volume_24h",
        "volume_market_cap_ratio",
        "coin_age_days",
        "category",
        "platform_name",
        "return_m14",
        "forward_14d",
        "reversal_14d",
    ] + [f"ret_{h:+d}" for h in range(-args.lookback, args.lookforward + 1)]
    write_csv(OUTPUT_DIR / "cmc_top_gainer_events.csv", enriched_events, event_fields)
    write_csv(
        OUTPUT_DIR / "cmc_forward_returns_by_day.csv",
        forward_summary,
        ["horizon_day", "count", "mean_return", "median_return", "reversal_probability"],
    )
    write_csv(
        OUTPUT_DIR / "cmc_bucket_paths.csv",
        bucket_rows,
        ["bucket", "horizon_day", "count", "mean_return", "median_return"],
    )
    render_svg(bucket_rows, OUTPUT_DIR / "cmc_forward_return_buckets.svg")
    write_summary(
        key_info=key_info,
        entry_events=enriched_events,
        forward_summary=forward_summary,
        bucket_meta=bucket_meta,
        group_summary=group_summary,
        out_path=OUTPUT_DIR / "cmc_top_gainers_summary.json",
        args=args,
    )

    print("Analysis complete.", file=sys.stderr)
    print("Outputs:", file=sys.stderr)
    print("  outputs/cmc_top_gainer_events.csv", file=sys.stderr)
    print("  outputs/cmc_forward_returns_by_day.csv", file=sys.stderr)
    print("  outputs/cmc_bucket_paths.csv", file=sys.stderr)
    print("  outputs/cmc_forward_return_buckets.svg", file=sys.stderr)
    print("  outputs/cmc_top_gainers_summary.json", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
