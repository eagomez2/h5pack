import os
import h5py
import numpy as np
from argparse import Namespace
from time import perf_counter
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn
)
from ..core.guards import is_file_with_ext
from ..core.io import (
    read_audio,
    read_audio_metadata
)
from ..core.display import (
    exit_error,
    get_console,
    is_progress_enabled,
    print_error,
    print_step,
    print_warning
)
from ..reader import (
    H5PackFile,
    decode_audio
)


# Source subtypes that FLAC stores without any loss
_LOSSLESS_FLAC_SUBTYPES = ("PCM_S8", "PCM_U8", "PCM_16", "PCM_24")


def get_source_dir(input_file: str, attrs: dict) -> str | None:
    """Returns the folder of the original audio files of a field.

    Args:
        input_file (str): `.h5` file being verified.
        attrs (dict): Attributes of the audio field.

    Returns:
        (str | None): Folder of the original audio files, or `None` if it
            was not stored (files created with h5pack < 1.3.0).
    """
    source_dir = attrs.get("source_dir")

    if source_dir is None:
        return None

    source_dir = str(source_dir)

    if os.path.isabs(source_dir):
        return source_dir

    # NOTE: Relative folders are stored relative to the packed file
    candidates = [os.path.dirname(os.path.abspath(input_file))]

    # Virtual datasets copy the attributes of their partitions, so the folder
    # is relative to the partitions instead
    with h5py.File(input_file, "r") as h5_file:
        for dataset in h5_file["data"].values():
            if not dataset.is_virtual:
                continue

            for source in dataset.virtual_sources():
                candidates.append(
                    os.path.dirname(
                        os.path.join(candidates[0], source.file_name)
                    )
                )

            break

    for candidate in candidates:
        path = os.path.normpath(os.path.join(candidate, source_dir))

        if os.path.isdir(path):
            return path

    return os.path.normpath(os.path.join(candidates[0], source_dir))


def _compare_audio(
        packed: np.ndarray,
        source_file: str,
        attrs: dict
) -> str | None:
    """Compares packed audio with its original audio file.

    Args:
        packed (np.ndarray): Raw packed value of the row.
        source_file (str): Original audio file.
        attrs (dict): Attributes of the audio field.

    Returns:
        (str | None): Description of the mismatch, or `None` if the audio
            matches.
    """
    if attrs.get("codec") == "flac":
        packed = decode_audio(packed, attrs=attrs, dtype="float64")
        source, _ = read_audio(source_file, dtype="float64")
        subtype = read_audio_metadata(source_file)["subtype"]

        # NOTE: Float sources are quantized when stored as FLAC, so they can
        # only match within the resolution of the FLAC subtype
        bits = 24 if attrs.get("flac_subtype", "PCM_16") == "PCM_24" else 16
        atol = (
            0.0 if subtype in _LOSSLESS_FLAC_SUBTYPES
            else 1.0 / 2 ** (bits - 1)
        )

    else:
        packed = decode_audio(packed, attrs=attrs)
        source, _ = read_audio(source_file, dtype=str(packed.dtype))
        atol = 0.0

    if source.shape[0] == 1:
        source = source[0]

    if packed.shape != source.shape:
        return f"shape {packed.shape} != {source.shape}"

    if atol == 0.0:
        if not np.array_equal(packed, source):
            num_diff = int(np.sum(packed != source))
            return f"{num_diff} sample(s) differ"

    elif not np.allclose(packed, source, rtol=0.0, atol=atol):
        max_diff = float(np.max(np.abs(packed - source)))
        return f"max. difference {max_diff:.2e}"

    return None


def cmd_verify(args: Namespace) -> None:
    """Verifies packed audio against the original audio files.

    Args:
        args (Namespace): User input arguments provided through the console.
    """
    # Check input file
    if not is_file_with_ext(args.input, ext=".h5"):
        exit_error(f"Invalid input file '{args.input}'")

    try:
        data = H5PackFile(args.input)

    except (ValueError, OSError) as e:
        exit_error(f"Cannot read '{args.input}'", cause=str(e))

    start_time = perf_counter()
    audio_fields = [f for f in data.fields if data.is_audio(f)]

    if len(audio_fields) == 0:
        exit_error(f"'{args.input}' does not contain audio fields")

    # Collect fields that can be verified
    fields = {}

    for field in audio_fields:
        attrs = data.field_attrs(field)

        if attrs.get("resampled", False):
            print_warning(
                f"Skipping field '{field}' since it was resampled, so it "
                "cannot match its original audio files"
            )
            continue

        if data.filepath(field, 0) is None:
            exit_error(
                f"Field '{field}' does not contain the paths of its original "
                "audio files",
                hint=(
                    "Files packed with --skip-filepaths cannot be verified"
                )
            )

        if args.source is not None:
            source_dir = args.source

        else:
            source_dir = get_source_dir(args.input, attrs=attrs)

            if source_dir is None:
                exit_error(
                    f"Folder of the original audio files of field '{field}' "
                    "is unknown",
                    hint=(
                        "Use --source to set the folder (files created with "
                        "h5pack < 1.3.0 do not store it)"
                    )
                )

        if not os.path.isdir(source_dir):
            exit_error(
                f"Folder of the original audio files of field '{field}' not "
                f"found: '{source_dir}'",
                hint="Use --source to set the folder"
            )

        fields[field] = source_dir

    if len(fields) == 0:
        exit_error("No fields left to verify")

    # Select rows
    if args.all or args.num_rows >= len(data):
        rows = list(range(len(data)))

    else:
        rng = np.random.default_rng(args.seed)
        rows = sorted(
            rng.choice(len(data), size=args.num_rows, replace=False).tolist()
        )

    progress_bar = Progress(
        TextColumn("{task.description:>10}", style="bold cyan"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        console=get_console(),
        transient=True,
        disable=not is_progress_enabled()
    )
    mismatches = []

    with data, progress_bar:
        task = progress_bar.add_task(
            "Verifying",
            total=len(rows) * len(fields)
        )

        for field, source_dir in fields.items():
            attrs = data.field_attrs(field)

            for row_idx in rows:
                source_file = os.path.join(
                    source_dir,
                    data.filepath(field, row_idx)
                )

                if not os.path.isfile(source_file):
                    mismatches.append(
                        (row_idx, field, f"'{source_file}' not found")
                    )

                else:
                    error = _compare_audio(
                        data.read(field, row_idx, decode=False),
                        source_file=source_file,
                        attrs=attrs
                    )

                    if error is not None:
                        mismatches.append(
                            (row_idx, field, f"{source_file}: {error}")
                        )

                progress_bar.advance(task)

    for row_idx, field, error in mismatches:
        print_error(f"Row {row_idx} '{field}' does not match", cause=error)

    num_checks = len(rows) * len(fields)
    rows_repr = (
        f"all {len(rows)} row(s)" if len(rows) == len(data)
        else f"{len(rows)} of {len(data)} random row(s)"
    )

    if len(mismatches) > 0:
        exit_error(
            f"{len(mismatches)} of {num_checks} audio file(s) do not match "
            "their original audio files"
        )

    print_step(
        "Verified",
        f"{rows_repr}, {', '.join(repr(f) for f in fields)}",
        details="audio matches the original files",
        elapsed=perf_counter() - start_time
    )
