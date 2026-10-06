import os
import shutil
import numpy as np
import pytest
import soundfile as sf
import h5pack
from conftest import (
    FIXTURES_DIR,
    run
)


LEGACY_DIR = os.path.join(FIXTURES_DIR, "legacy")

# Files created with previous releases of h5pack from the same data
LEGACY_FILES = ["v1.1.0.h5", "v1.2.0.h5"]


@pytest.mark.parametrize("file", LEGACY_FILES)
def test_read_legacy_file(file):
    with h5pack.open(os.path.join(LEGACY_DIR, file)) as data:
        assert len(data) == 4
        assert data.sample_rate("audio") == 8000

        for idx, row in enumerate(data):
            folder = "a" if idx % 2 == 0 else "b"
            name = str(idx) if idx < 2 else f"x{idx}"
            expected, _ = sf.read(
                os.path.join(LEGACY_DIR, "data", folder, f"{name}.wav"),
                dtype="int16"
            )
            assert np.array_equal(row["audio"], expected)
            assert row["label"] == f"lab{idx % 2}"
            assert row["score"] == idx
            assert row["emb"].tolist() == [idx, idx + 1]


@pytest.mark.parametrize("file", LEGACY_FILES)
def test_inspect_legacy_file(file):
    run("info", os.path.join(LEGACY_DIR, file))
    run("show", os.path.join(LEGACY_DIR, file), "-r", "0:4")


@pytest.mark.parametrize("file", LEGACY_FILES)
def test_unpack_legacy_file(file, tmp_path):
    run("unpack", os.path.join(LEGACY_DIR, file), "-o", str(tmp_path / "un"))
    run(
        "pack", "-c", "h5pack.yaml", "-d", "un", "-o", "re.h5", "-u",
        cwd=str(tmp_path / "un")
    )

    with (
        h5pack.open(os.path.join(LEGACY_DIR, file)) as a,
        h5pack.open(str(tmp_path / "un" / "re.h5")) as b
    ):
        for row_a, row_b in zip(a, b, strict=True):
            for field in a.fields:
                assert np.array_equal(row_a[field], row_b[field])


def test_verify_legacy_file_requires_source(tmp_path):
    file = os.path.join(LEGACY_DIR, "v1.2.0.h5")
    result = run("verify", file, check=False)
    assert result.returncode != 0
    assert "--source" in result.stderr

    run("verify", file, "--source", os.path.join(LEGACY_DIR, "data"), "-a")


def test_legacy_config_still_packs(tmp_path):
    root = str(tmp_path / "legacy")
    shutil.copytree(LEGACY_DIR, root)
    run(
        "pack", "-c", "h5pack.yaml", "-d", "legacy", "-o", "new.h5", "-u",
        cwd=root
    )

    with (
        h5pack.open(os.path.join(root, "v1.2.0.h5")) as a,
        h5pack.open(os.path.join(root, "new.h5")) as b
    ):
        for row_a, row_b in zip(a, b, strict=True):
            for field in a.fields:
                assert np.array_equal(row_a[field], row_b[field])
