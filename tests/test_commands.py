import os
import pickle
import shutil
import yaml
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
    "emb": ("emb", "as_listint16")
}


@pytest.fixture
def packed(dataset) -> str:
    root = dataset["root"]
    write_config(root, FIELDS)
    run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "out/ds.h5", "-p",
        "2", "--create-virtual", "-u", cwd=root
    )
    return os.path.join(root, "out", "ds.h5")


def test_init(dataset):
    root = dataset["root"]
    run("init", "dataset.csv", cwd=root)

    with open(os.path.join(root, "h5pack.yaml")) as f:
        config = yaml.safe_load(f)

    fields = config["datasets"]["dataset"]["data"]["fields"]
    assert fields["file"]["parser"] == "as_audioint16"
    assert fields["label"]["parser"] == "as_categorical"
    assert fields["score"]["parser"] == "as_int8"
    assert fields["emb"]["parser"] == "as_listint8"
    assert fields["weight"]["parser"] == "as_float32"
    assert fields["text"]["parser"] == "as_utf8str"

    # Existing files are not overwritten
    result = run("init", "dataset.csv", cwd=root, check=False)
    assert result.returncode != 0
    assert "--overwrite" in result.stderr

    # The created configuration can be packed as it is
    run(
        "pack", "-c", "h5pack.yaml", "-d", "dataset", "-o", "ds.h5", "-u",
        cwd=root
    )


def test_unpack_round_trip(packed, dataset):
    root = dataset["root"]
    run("unpack", packed, "-o", os.path.join(root, "un"))

    # Files with the same name in different folders are kept
    for idx in range(8):
        file = f"spk{idx % 2}/{str(idx // 2).zfill(3)}.wav"
        unpacked, _ = sf.read(
            os.path.join(root, "un", "data", "audio", file),
            dtype="int16"
        )
        original, _ = sf.read(os.path.join(root, "data", file), dtype="int16")
        assert np.array_equal(unpacked, original)

    # Unpacked datasets can be packed again
    run(
        "pack", "-c", "h5pack.yaml", "-d", "un", "-o", "re.h5", "-u",
        cwd=os.path.join(root, "un")
    )

    with (
        h5pack.open(packed) as a,
        h5pack.open(os.path.join(root, "un", "re.h5")) as b
    ):
        for row_a, row_b in zip(a, b, strict=True):
            for field in FIELDS:
                assert np.array_equal(row_a[field], row_b[field])


def test_unpack_keeps_text(tmp_path):
    root = str(tmp_path)
    make_dataset(root)

    # Text that looks like numbers must be kept as it is
    with open(os.path.join(root, "dataset.csv")) as f:
        lines = f.read().splitlines()

    lines = [lines[0] + ",code"] + [
        f"{line},{str(idx).zfill(3)}" for idx, line in enumerate(lines[1:])
    ]

    with open(os.path.join(root, "dataset.csv"), "w") as f:
        f.write("\n".join(lines) + "\n")

    write_config(
        root,
        {
            "code": ("code", "as_utf8str"),
            "score": ("score", "as_int16"),
            "audio": ("file", "as_audioint16")
        }
    )
    run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "ds.h5", "-u",
        cwd=root
    )
    run("unpack", "ds.h5", "-o", "un", cwd=root)

    with open(os.path.join(root, "un", "dataset.csv")) as f:
        rows = f.read().splitlines()

    assert rows[0] == "audio__filepath,code,score"
    assert rows[1] == "data/audio/spk0/000.wav,000,-3"


def test_show(packed, dataset):
    result = run("show", packed, "-r", "-1")
    assert "Row 7 of 8" in result.stdout
    assert "spk1/003.wav" in result.stdout

    save_dir = os.path.join(dataset["root"], "rows")
    run("show", packed, "-r", "2:4", "-f", "audio", "-s", save_dir)
    assert sorted(os.listdir(save_dir)) == [
        "row2_audio.wav",
        "row3_audio.wav"
    ]

    result = run("show", packed, "-r", "100", check=False)
    assert result.returncode != 0


def test_verify_detects_changes(packed, dataset):
    run("verify", packed, "-a")

    # Changed source file
    file = os.path.join(dataset["root"], "data", "spk1", "002.wav")
    audio, fs = sf.read(file, dtype="int16")
    audio[10] += 1
    sf.write(file, audio, fs, subtype="PCM_16")

    result = run("verify", packed, "-a", check=False)
    assert result.returncode == 1
    assert "Row 5 'audio' does not match" in result.stderr


def test_verify_with_source(packed, dataset, tmp_path_factory):
    # Original files moved to another folder
    new_dir = os.path.join(str(tmp_path_factory.mktemp("moved")), "audio")
    shutil.move(os.path.join(dataset["root"], "data"), new_dir)

    result = run("verify", packed, check=False)
    assert result.returncode != 0
    assert "--source" in result.stderr

    run("verify", packed, "--source", new_dir, "-a")


def test_checksum_mismatch(packed):
    checksum_file = packed.replace(".h5", ".sha256")
    run("checksum", checksum_file)

    with open(packed.replace(".h5", ".pt1.h5"), "ab") as f:
        f.write(b"\0")

    result = run("checksum", checksum_file, check=False)
    assert result.returncode != 0


def test_info(packed):
    result = run("info", packed)
    assert "ds.pt0.h5" in result.stdout
    assert "as_audioint16" in result.stdout


def test_reader(packed):
    data = h5pack.open(packed, fields=["audio", "label"])
    assert len(data) == 8
    assert data.fields == ["audio", "label"]
    assert data.sample_rate("audio") == 8000
    assert data.filepath("audio", 3) == "spk1/001.wav"
    assert data[-1]["label"] == data[7]["label"]

    with pytest.raises(IndexError):
        data[8]

    # Readers can be pickled (e.g. for DataLoader workers)
    clone = pickle.loads(pickle.dumps(data))
    assert np.array_equal(clone[3]["audio"], data[3]["audio"])
    data.close()
    clone.close()

    with pytest.raises(KeyError):
        h5pack.open(packed, fields=["missing"])


def test_reader_multiprocessing(packed):
    import multiprocessing as mp

    data = h5pack.open(packed)
    data[0]  # Open the file in the parent process

    with mp.get_context("spawn").Pool(2) as pool:
        lengths = pool.map(_audio_len, [(data, idx) for idx in range(8)])

    assert lengths == [800] * 8


def _audio_len(args) -> int:
    data, idx = args
    return len(data[idx]["audio"])


def test_dry_run_and_verbose(tmp_path):
    root = str(tmp_path)
    make_dataset(root)
    write_config(root, FIELDS)
    result = run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "ds.h5", "-p", "2",
        "--dry-run", "-v", cwd=root
    )
    assert "ds.pt0.h5" in result.stdout
    assert not os.path.exists(os.path.join(root, "ds.pt0.h5"))
