"""Tests for generate_raw_data.py: the command-line entry point, end to end."""

import hashlib

import generate_raw_data

# MD5 of each CSV written with the default arguments. The dbt models, tests and
# README numbers all depend on this exact data, so any change to the
# generator's output must be deliberate: update these hashes only when you
# mean to change the data.
DEFAULT_OUTPUT_MD5 = {
    "customers.csv": "ad6a8b7ef3ae05a26b2bdfd78136c7bb",
    "orders.csv": "8cd81abcad7b206f96436fceafb94480",
    "products.csv": "0c864d0958d279912e9db2a5ae1e4e7b",
    "subscriptions.csv": "f0d49a1a0cb46b89c05baed943fbd160",
}


def md5(path):
    return hashlib.md5(path.read_bytes()).hexdigest()


def test_default_output_is_unchanged(tmp_path):
    generate_raw_data.main(["--out-dir", str(tmp_path)])
    assert {p.name: md5(p) for p in sorted(tmp_path.glob("*.csv"))} == (
        DEFAULT_OUTPUT_MD5
    )


def test_same_seed_same_files_different_seed_different_files(tmp_path):
    small = ["--customers", "100", "--products", "20", "--orders", "300"]
    for name, seed in (("a", "5"), ("b", "5"), ("c", "6")):
        generate_raw_data.main(
            [*small, "--seed", seed, "--out-dir", str(tmp_path / name)]
        )
    hashes = {
        name: [md5(p) for p in sorted((tmp_path / name).glob("*.csv"))]
        for name in "abc"
    }
    assert hashes["a"] == hashes["b"]
    assert hashes["a"] != hashes["c"]
