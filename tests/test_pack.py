import os
import h5py
import numpy as np
import pytest
import soundfile as sf
import h5pack
from conftest import (
    make_dataset,
    run,
    write_config
)


FIELDS = {
    "audio": ("file", "as_audioint16"),
    "label": ("label", "as_utf8str"),
    "score": ("score", "as_int16"),
    "emb": ("emb", "as_listint16"),
    "weight": ("weight", "as_float32")
}


def _read_all(file: str) -> list[dict]:
    with h5pack.open(file) as data:
        return [data[idx] for idx in range(len(data))]


@pytest.mark.parametrize("workers", [1, 3])
def test_pack_partitions(dataset, workers):
    root = dataset["root"]
    write_config(root, FIELDS)
    run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "out/ds.h5",
        "-p", "3", "-w", str(workers), "--create-virtual", "-u",
        cwd=root
    )

    for idx in range(3):
        assert os.path.isfile(os.path.join(root, "out", f"ds.pt{idx}.h5"))

    rows = _read_all(os.path.join(root, "out", "ds.h5"))
    assert len(rows) == dataset["num_rows"]

    for idx, row in enumerate(rows):
        expected, _ = sf.read(
            os.path.join(root, f"data/spk{idx % 2}/{str(idx // 2).zfill(3)}"
                         ".wav"),
            dtype="int16"
        )
        assert np.array_equal(row["audio"], expected)
        assert row["label"] == "abc"[idx % 3]
        assert row["score"] == idx - 3
        assert row["emb"].tolist() == [idx, idx + 1, idx + 2]
        assert row["weight"] == pytest.approx(idx / 4)

    # Checksum file is created and valid
    run("checksum", os.path.join(root, "out", "ds.sha256"))


def test_virtual_dataset_from_other_folder(dataset, tmp_path_factory):
    root = dataset["root"]
    write_config(root, FIELDS)
    run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "out/ds.h5", "-p",
        "2", "--create-virtual", "-u", cwd=root
    )
    other_dir = str(tmp_path_factory.mktemp("other"))

    # NOTE: Tests run from a folder other than the dataset folder
    rows = _read_all(os.path.join(root, "out", "ds.h5"))
    assert len(rows) == dataset["num_rows"]

    result = run("info", os.path.join(root, "out", "ds.h5"), cwd=other_dir)
    assert "not found" not in result.stdout + result.stderr


def test_dry_run_writes_nothing(dataset):
    root = dataset["root"]
    write_config(root, FIELDS)
    result = run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "out/ds.h5", "-p",
        "2", "--dry-run", cwd=root
    )
    assert "Dry run" in result.stdout
    assert not os.path.exists(os.path.join(root, "out"))


@pytest.mark.parametrize("compression", ["gzip", "lzf"])
def test_compression(dataset, compression):
    root = dataset["root"]
    write_config(root, FIELDS)
    run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "ds.h5",
        "--compression", compression, "-u", cwd=root
    )

    with h5py.File(os.path.join(root, "ds.h5")) as f:
        assert f["data"]["audio"].compression == compression
        assert f["data"]["audio"].shuffle

    assert len(_read_all(os.path.join(root, "ds.h5"))) == 8


def test_skip_filepaths(dataset):
    root = dataset["root"]
    write_config(root, FIELDS)
    run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "ds.h5",
        "--skip-filepaths", "-u", cwd=root
    )

    with h5py.File(os.path.join(root, "ds.h5")) as f:
        assert "audio__filepath" not in f["data"]

    # Unpacking generates file names
    run("unpack", "ds.h5", "-o", "un", cwd=root)
    assert len(os.listdir(os.path.join(root, "un", "data", "audio"))) == 8


def test_quiet_prints_nothing(dataset):
    root = dataset["root"]
    write_config(root, FIELDS)
    result = run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "ds.h5", "-q", "-u",
        cwd=root
    )
    assert result.stdout.strip() == ""


@pytest.mark.parametrize(
    "fields, message",
    [
        ({"audio": ("missing", "as_audioint16")}, "missing"),
        ({"label": ("label", "as_int16")}, "as_int16"),
        ({"score": ("score", "as_unknown")}, "Unknown parser"),
        ({"emb": ("emb", "as_listint8")}, None),
        ({"label": ("label", "as_audioint16")}, None)
    ]
)
def test_invalid_config(dataset, fields, message):
    root = dataset["root"]

    if "emb" in fields:
        # Values out of int8 range
        with open(os.path.join(root, "dataset.csv"), "a") as f:
            f.write('data/spk0/000.wav,a,0,"[1000]",0.0,x\n')

    write_config(root, fields)
    result = run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "ds.h5", "-u",
        cwd=root,
        check=False
    )
    assert result.returncode != 0
    assert "error:" in result.stderr

    if message is not None:
        assert message in result.stderr

    assert not os.path.exists(os.path.join(root, "ds.h5"))


def test_failing_worker_does_not_hang(dataset):
    root = dataset["root"]
    write_config(root, FIELDS)

    # Corrupt one audio file after validation would pass its metadata check
    with open(os.path.join(root, "data", "spk1", "003.wav"), "wb") as f:
        f.write(b"not audio")

    result = run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "out/ds.h5", "-p",
        "4", "-w", "4", "--skip-validation", "-u", cwd=root, check=False
    )
    assert result.returncode != 0


def test_float_parser_warning(tmp_path):
    root = str(tmp_path)
    make_dataset(root)
    write_config(root, {"audio": ("file", "as_audiofloat32")})
    result = run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "ds.h5", "-u",
        cwd=root
    )
    assert "warning:" in result.stderr
    assert "16-bit" in result.stderr
