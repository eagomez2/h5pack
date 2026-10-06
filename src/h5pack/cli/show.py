import os
import numpy as np
from argparse import Namespace
from rich.table import Table
from rich.markup import escape
from ..core.guards import is_file_with_ext
from ..core.io import write_audio
from ..core.display import (
    exit_error,
    get_console,
    get_verbosity,
    print_output,
    print_step,
    NORMAL
)
from ..reader import H5PackFile


def parse_rows(rows: str, num_rows: int) -> list[int]:
    """Parses a row index or range of rows.

    Args:
        rows (str): Row index (e.g. `42` or `-1`) or range of rows (e.g.
            `10:20`, `:5` or `-3:`).
        num_rows (int): Total number of rows.

    Returns:
        (list[int]): Selected row indices.
    """
    try:
        if ":" in rows:
            start, stop = (
                int(s) if s.strip() != "" else None
                for s in rows.split(":", maxsplit=1)
            )
            return list(range(num_rows))[slice(start, stop)]

        idx = int(rows)

    except ValueError:
        raise ValueError(f"Invalid rows '{rows}'") from None

    if idx < 0:
        idx += num_rows

    if not 0 <= idx < num_rows:
        raise ValueError(
            f"Row {rows} out of range (file has {num_rows} rows)"
        )

    return [idx]


def describe_value(value, attrs: dict) -> tuple[str, str]:
    """Returns a short type and value description of a field value.

    Args:
        value: Decoded field value.
        attrs (dict): Attributes of the field.

    Returns:
        (tuple[str, str]): Type and value descriptions.
    """
    parser = str(attrs.get("parser", ""))

    if parser.startswith("as_audio"):
        fs = int(attrs["sample_rate"])
        num_channels = int(attrs.get("num_channels", 1))
        num_samples = value.shape[-1]
        channels_repr = (
            "mono" if num_channels == 1 else f"{num_channels} channels"
        )
        peak = float(np.max(np.abs(value))) if value.size > 0 else 0.0

        if np.issubdtype(value.dtype, np.integer):
            peak /= np.iinfo(value.dtype).max

        codec = (
            "flac" if attrs.get("codec") == "flac" else str(value.dtype)
        )
        return (
            f"audio ({codec})",
            f"{num_samples / fs:.2f}s, {fs / 1000:g} kHz, {channels_repr}, "
            f"peak {peak:.2f}"
        )

    if isinstance(value, np.ndarray):
        values_repr = np.array2string(
            value,
            threshold=8,
            edgeitems=3,
            max_line_width=60
        )
        return f"list[{value.dtype}] ({len(value)})", values_repr

    if parser == "as_categorical":
        return "categorical", value

    if isinstance(value, str):
        return "str", value

    return str(np.asarray(value).dtype), str(value)


def _play_audio(audio: np.ndarray, fs: int) -> None:
    """Plays audio using `sounddevice` and waits until playback ends.

    Args:
        audio (np.ndarray): Audio with shape `(num_samples,)` or
            `(num_channels, num_samples)`.
        fs (int): Sample rate.
    """
    import sounddevice as sd

    sd.play(audio.T, samplerate=fs)
    sd.wait()


def cmd_show(args: Namespace) -> None:
    """Shows, saves or plays the data of one or more rows of a `.h5` file.

    Args:
        args (Namespace): User input arguments provided through the console.
    """
    # Check input file
    if not is_file_with_ext(args.input, ext=".h5"):
        exit_error(f"Invalid input file '{args.input}'")

    if args.play:
        try:
            import sounddevice  # noqa: F401

        except (ModuleNotFoundError, OSError) as e:
            exit_error(
                "--play requires the 'sounddevice' package",
                cause=str(e),
                hint="Install it with 'pip install h5pack[play]'"
            )

    try:
        data = H5PackFile(args.input, fields=args.fields)

    except (KeyError, ValueError, OSError) as e:
        exit_error(f"Cannot read '{args.input}'", cause=str(e).strip("'\""))

    try:
        rows = parse_rows(args.rows, num_rows=len(data))

    except ValueError as e:
        exit_error(str(e), hint="Use e.g. '-r 42', '-r -1' or '-r 10:20'")

    if len(rows) == 0:
        exit_error(f"No rows selected with '{args.rows}'")

    console = get_console()

    with data:
        for row_idx in rows:
            row = data[row_idx]

            if get_verbosity() >= NORMAL:
                table = Table(
                    title=f"Row {row_idx} of {len(data)}",
                    title_justify="left",
                    title_style="bold",
                    header_style="bold",
                    show_edge=False
                )
                table.add_column("Field", style="cyan", no_wrap=True)
                table.add_column("Type", style="dim")
                table.add_column("Value")

                for field, value in row.items():
                    type_repr, value_repr = describe_value(
                        value,
                        attrs=data.field_attrs(field)
                    )

                    value_cell = escape(str(value_repr))

                    if data.is_audio(field):
                        filepath = data.filepath(field, row_idx)

                        if filepath is not None:
                            value_cell += f"\n[dim]{escape(filepath)}[/dim]"

                    table.add_row(
                        escape(field),
                        escape(type_repr),
                        value_cell
                    )

                console.print(table)
                console.print()

            for field, value in row.items():
                if not data.is_audio(field):
                    continue

                fs = data.sample_rate(field)

                if args.save is not None:
                    os.makedirs(args.save, exist_ok=True)
                    file = os.path.join(
                        args.save,
                        f"row{str(row_idx).zfill(len(str(len(data))))}_"
                        f"{field}.wav"
                    )
                    write_audio(value, file=file, fs=fs)
                    print_output(file)

                if args.play:
                    print_step(
                        "Playing",
                        f"row {row_idx} '{field}'",
                        details=f"{value.shape[-1] / fs:.2f}s"
                    )

                    try:
                        _play_audio(value, fs=fs)

                    except KeyboardInterrupt:
                        import sounddevice as sd

                        sd.stop()
                        return
