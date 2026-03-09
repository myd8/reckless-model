from pathlib import Path

from reckless_binance.paths import output_dir


def artifact_path(filename: str) -> Path:
    """Return a top-level derived artifact path under outputs/."""

    path = Path(filename)
    if path.name != filename:
        raise ValueError("artifact_path expects a filename only")
    return output_dir() / path


def raw_dataset_dir(source: str, dataset: str) -> Path:
    """Return the raw parquet cache directory for a source dataset."""

    return output_dir() / "raw" / source / dataset
