from reckless_binance.cli import build_parser


def test_cli_defaults_to_three_years_and_top20():
    args = build_parser().parse_args([])

    assert args.lookback_years == 3
    assert args.top_n == 20
