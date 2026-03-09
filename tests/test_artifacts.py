from reckless_binance.artifacts import artifact_path, raw_dataset_dir
from reckless_binance.paths import output_dir


def test_artifact_path_targets_top_level_parquet_outputs():
    path = artifact_path("top_gainer_events.parquet")

    assert path == output_dir() / "top_gainer_events.parquet"


def test_raw_dataset_dir_nests_by_source_and_dataset():
    path = raw_dataset_dir("binance", "klines_4h")

    assert path == output_dir() / "raw" / "binance" / "klines_4h"


def test_artifact_path_rejects_directory_segments():
    try:
        artifact_path("raw/top_gainer_events.parquet")
    except ValueError as exc:
        assert "filename only" in str(exc)
    else:
        raise AssertionError("artifact_path should reject nested paths")
