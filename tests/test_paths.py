from reckless_binance.paths import output_dir, project_root


def test_output_dir_is_inside_project_root():
    root = project_root()
    out = output_dir()

    assert out.parent == root
    assert out.name == "outputs"
