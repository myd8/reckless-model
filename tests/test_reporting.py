import pandas as pd

from reckless_binance.reporting import bucket_forward_paths, summarize_forward_returns


def test_reversal_probability_is_share_of_negative_forward_returns():
    events = pd.DataFrame(
        [
            {"ret_+1": -0.01, "ret_+2": 0.02},
            {"ret_+1": 0.03, "ret_+2": -0.04},
        ]
    )

    summary = summarize_forward_returns(events, max_horizon=2)

    assert summary.loc[summary["horizon_day"] == 1, "reversal_probability"].item() == 0.5


def test_bucket_forward_paths_produces_bucketed_daily_rows():
    events = pd.DataFrame(
        [
            {"ret_7d": 0.10, "ret_+1": -0.01, "ret_+2": 0.01},
            {"ret_7d": 0.20, "ret_+1": -0.02, "ret_+2": 0.02},
            {"ret_7d": 0.30, "ret_+1": 0.03, "ret_+2": -0.03},
            {"ret_7d": 0.40, "ret_+1": 0.04, "ret_+2": -0.04},
        ]
    )

    bucketed = bucket_forward_paths(events, max_horizon=2)

    assert set(bucketed["horizon_day"]) == {1, 2}
    assert bucketed["bucket"].nunique() == 4
