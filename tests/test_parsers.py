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


def _source(root: str, idx: int, dtype: str) -> np.ndarray:
    audio, _ = sf.read(
        os.path.join(root, f"data/spk{idx % 2}/{str(idx // 2).zfill(3)}.wav"),
        dtype=dtype,
        always_2d=True
    )
    audio = audio.T
    return audio[0] if audio.shape[0] == 1 else audio


def _pack(root: str, fields: dict, *args: str) -> str:
    write_config(root, fields)
    run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "ds.h5", "-u", *args,
        cwd=root
    )
    return os.path.join(root, "ds.h5")


@pytest.mark.parametrize("subtype", ["PCM_16", "PCM_24"])
@pytest.mark.parametrize("lengths", ["fixed", "vlen"])
def test_flac_is_lossless(tmp_path, subtype, lengths):
    root = str(tmp_path)
    make_dataset(root, subtype=subtype, lengths=lengths, num_channels=2)
    file = _pack(root, {"audio": ("file", "as_audioflac")})

    with h5py.File(file) as f:
        assert f["data"]["audio"].attrs["codec"] == "flac"

    with h5pack.open(file, audio_dtype="float64") as data:
        for idx in range(len(data)):
            assert np.array_equal(data[idx]["audio"], _source(root, idx,
                                                               "float64"))

    # Unpacking writes .flac files
    run("unpack", file, "-o", "un", cwd=root)
    assert os.path.isfile(
        os.path.join(root, "un", "data", "audio", "spk1", "000.flac")
    )


@pytest.mark.parametrize("lengths", ["fixed", "vlen"])
@pytest.mark.parametrize("parser", ["as_audioint16", "as_audiofloat32"])
def test_multichannel(tmp_path, lengths, parser):
    root = str(tmp_path)
    make_dataset(root, num_channels=3, lengths=lengths)
    file = _pack(root, {"audio": ("file", parser)})
    dtype = "int16" if parser == "as_audioint16" else "float32"

    with h5pack.open(file) as data:
        for idx in range(len(data)):
            audio = data[idx]["audio"]
            assert audio.shape[0] == 3
            assert np.array_equal(audio, _source(root, idx, dtype))

    # Round trip through unpack
    run("unpack", file, "-o", "un", cwd=root)
    unpacked, _ = sf.read(
        os.path.join(root, "un", "data", "audio", "spk1", "001.wav"),
        dtype=dtype
    )
    assert np.array_equal(unpacked.T, _source(root, 3, dtype))


def test_mono_layout_is_unchanged(dataset):
    file = _pack(dataset["root"], {"audio": ("file", "as_audioint16")})

    with h5py.File(file) as f:
        assert f["data"]["audio"].shape == (8, 800)
        assert f["data"]["audio"].dtype == np.int16
        assert f["data"]["audio"].attrs["sample_rate"] == "8000"


def test_categorical(dataset):
    root = dataset["root"]
    file = _pack(
        root,
        {
            "label": ("label", "as_categorical"),
            "score": ("score", "as_categorical")
        },
        "-p", "3"
    )

    for idx in range(3):
        with h5py.File(file.replace(".h5", f".pt{idx}.h5")) as f:
            assert f["data"]["label"].dtype == np.uint8

    run("virtual", root, "-o", os.path.join(root, "all.h5"), "-u")

    with h5pack.open(os.path.join(root, "all.h5")) as data:
        assert data.categories("label") == ["a", "b", "c"]
        assert [row["label"] for row in data] == list("abcabcab")
        assert [row["score"] for row in data] == list(range(-3, 5))

    run("unpack", os.path.join(root, "all.h5"), "-o", "un", cwd=root)

    with open(os.path.join(root, "un", "dataset.csv")) as f:
        assert f.readline().strip() == "label,score"
        assert f.readline().strip() == "a,-3"


def test_mixed_sample_rates_fail(tmp_path):
    root = str(tmp_path)
    make_dataset(root, fs=[8000, 16000])
    write_config(root, {"audio": ("file", "as_audioint16")})
    result = run(
        "pack", "-c", "h5pack.yaml", "-d", "ds", "-o", "ds.h5", "-u",
        cwd=root,
        check=False
    )
    assert result.returncode != 0
    assert "same sample rate" in result.stderr


@pytest.mark.parametrize(
    "parser, dtype",
    [
        ("as_int8", np.int8),
        ("as_int16", np.int16),
        ("as_float32", np.float32),
        ("as_float64", np.float64)
    ]
)
def test_scalar_parsers(dataset, parser, dtype):
    file = _pack(dataset["root"], {"score": ("score", parser)})

    with h5py.File(file) as f:
        assert f["data"]["score"].dtype == dtype
        assert f["data"]["score"][:].tolist() == list(range(-3, 5))


def test_list_and_text_parsers(dataset):
    file = _pack(
        dataset["root"],
        {
            "emb8": ("emb", "as_listint8"),
            "emb64": ("emb", "as_listfloat64"),
            "text": ("text", "as_utf8str")
        }
    )

    with h5pack.open(file) as data:
        row = data[-1]
        assert row["emb8"].dtype == np.int8
        assert row["emb8"].tolist() == [7, 8, 9]
        assert row["emb64"].dtype == np.float64
        assert row["text"] == "text 7"
