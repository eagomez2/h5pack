import os
import sys
import subprocess
import numpy as np
import pytest
import soundfile as sf


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def run(
        *args: str,
        cwd: str | None = None,
        check: bool = True
) -> subprocess.CompletedProcess:
    """Runs `h5pack` in a subprocess.

    Args:
        *args (str): Command line arguments.
        cwd (str | None): Working directory.
        check (bool): If `True`, the command must succeed.

    Returns:
        (subprocess.CompletedProcess): Finished process.
    """
    env = {**os.environ, "NO_COLOR": "1", "COLUMNS": "200"}
    result = subprocess.run(
        [sys.executable, "-m", "h5pack", *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8"
    )

    if check and result.returncode != 0:
        raise AssertionError(
            f"h5pack {' '.join(args)} failed with code {result.returncode}"
            f"\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

    return result


def write_config(
        root: str,
        fields: dict,
        dataset: str = "ds",
        csv_file: str = "dataset.csv",
        file: str = "h5pack.yaml"
) -> str:
    """Writes a `h5pack.yaml` configuration file.

    Args:
        root (str): Dataset folder.
        fields (dict): Mapping of field names to `(column, parser)` or
            `(column, parser, parser_args)`.
        dataset (str): Dataset name.
        csv_file (str): `.csv` file relative to `root`.
        file (str): Configuration file name.

    Returns:
        (str): Path of the configuration file.
    """
    lines = [
        "datasets:",
        f"  {dataset}:",
        "    attrs:",
        "      author: tests",
        "    data:",
        f"      file: {csv_file}",
        "      fields:"
    ]

    for name, spec in fields.items():
        lines += [
            f"        {name}:",
            f"          column: {spec[0]}",
            f"          parser: {spec[1]}"
        ]

        if len(spec) > 2:
            lines.append("          parser_args:")
            lines += [f"            {k}: {v}" for k, v in spec[2].items()]

    path = os.path.join(root, file)

    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")

    return path


def make_dataset(
        root: str,
        num_rows: int = 8,
        fs: int | list[int] = 8000,
        num_channels: int = 1,
        lengths: str = "fixed",
        subtype: str = "PCM_16",
        seed: int = 0
) -> dict:
    """Creates a synthetic dataset with audio files and a `.csv` file. Files
    of different rows share names in different subfolders (e.g.
    `spk0/000.wav` and `spk1/000.wav`).

    Args:
        root (str): Output folder.
        num_rows (int): Number of rows.
        fs (int | list[int]): Sample rate(s) used cyclically.
        num_channels (int): Number of channels.
        lengths (str): `fixed` or `vlen`.
        subtype (str): Audio subtype.
        seed (int): Random seed.

    Returns:
        (dict): Dataset information including the written audio.
    """
    rng = np.random.default_rng(seed)
    sample_rates = fs if isinstance(fs, list) else [fs]
    rows = []
    audios = []

    for idx in range(num_rows):
        file = f"data/spk{idx % 2}/{str(idx // 2).zfill(3)}.wav"
        row_fs = sample_rates[idx % len(sample_rates)]
        num_samples = (
            row_fs // 10 if lengths == "fixed"
            else row_fs // 10 + 7 * idx
        )
        audio = np.clip(
            rng.standard_normal((num_samples, num_channels)) * 0.2,
            -1.0,
            1.0
        )
        os.makedirs(os.path.join(root, os.path.dirname(file)), exist_ok=True)
        sf.write(
            os.path.join(root, file),
            audio,
            samplerate=row_fs,
            subtype=subtype
        )
        audios.append(audio)
        rows.append(
            f"{file},{'abc'[idx % 3]},{idx - 3},"
            f"\"[{idx}, {idx + 1}, {idx + 2}]\",{idx / 4},text {idx}"
        )

    with open(os.path.join(root, "dataset.csv"), "w") as f:
        f.write("file,label,score,emb,weight,text\n" + "\n".join(rows) + "\n")

    return {"root": root, "num_rows": num_rows, "audios": audios}


@pytest.fixture
def dataset(tmp_path) -> dict:
    """Default mono dataset with 8 fixed-length rows."""
    return make_dataset(str(tmp_path))


def has_module(name: str) -> bool:
    """Returns `True` if a module can be imported.

    Args:
        name (str): Module name.

    Returns:
        (bool): `True` if the module is available.
    """
    try:
        __import__(name)

    except (ModuleNotFoundError, OSError):
        return False

    return True
