from __future__ import annotations

import argparse

from reckless_binance.pipeline_4h import build_event_labels_artifact


def main(argv: list[str] | None = None) -> None:
    """Build event labels and the breakout-vs-blowoff summary from a feature panel."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-path", required=True)
    parser.add_argument("--labels-path", required=True)
    parser.add_argument("--summary-path", required=True)
    parser.add_argument("--bars-per-day", type=int, default=6)
    args = parser.parse_args(argv)

    build_event_labels_artifact(
        feature_path=args.feature_path,
        labels_path=args.labels_path,
        summary_path=args.summary_path,
        bars_per_day=args.bars_per_day,
    )


if __name__ == "__main__":
    main()
