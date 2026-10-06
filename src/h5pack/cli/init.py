import os
import re
import ast
import polars as pl
from argparse import Namespace
from ..core.config import get_allowed_audio_extensions
from ..core.guards import is_file_with_ext
from ..core.io import read_audio_metadata
from ..core.display import (
    exit_error,
    print_output,
    print_step,
    print_warning
)


# Number of values inspected to guess the parser of audio and list columns
_NUM_SAMPLES = 100

# Maximum number of unique values of a column packed as categorical
_MAX_CATEGORIES = 256


def _fits(values: list, dtype_min: int, dtype_max: int) -> bool:
    """Returns `True` if all integer values fit in a data type range.

    Args:
        values (list): Integer values.
        dtype_min (int): Minimum value of the data type.
        dtype_max (int): Maximum value of the data type.

    Returns:
        (bool): `True` if all values fit.
    """
    return all(dtype_min <= v <= dtype_max for v in values)


def _guess_list_parser(values: list[str]) -> str | None:
    """Guesses the parser of a column containing lists such as `[1, 2, 3]`.

    Args:
        values (list[str]): Values of the column.

    Returns:
        (str | None): Parser name or `None` if values are not lists.
    """
    items = []

    for value in values:
        if not value.strip().startswith("["):
            return None

        try:
            parsed = ast.literal_eval(value)

        except (ValueError, SyntaxError):
            return None

        if not isinstance(parsed, list):
            return None

        items += parsed

    if not all(isinstance(i, int | float) for i in items):
        return None

    if all(isinstance(i, int) for i in items):
        if _fits(items, -2 ** 7, 2 ** 7 - 1):
            return "as_listint8"

        if _fits(items, -2 ** 15, 2 ** 15 - 1):
            return "as_listint16"

    return "as_listfloat32"


def _guess_audio_parser(
        values: list[str],
        root_dir: str
) -> tuple[str, str] | None:
    """Guesses the parser of a column containing audio file paths.

    Args:
        values (list[str]): Values of the column.
        root_dir (str): Folder used to resolve relative paths.

    Returns:
        (tuple[str, str] | None): Parser name and description of the audio
            files, or `None` if values are not existing audio files.
    """
    extensions = get_allowed_audio_extensions()
    metas = []

    for value in values:
        if os.path.splitext(value)[1].lower() not in extensions:
            return None

        file = value if os.path.isabs(value) else os.path.join(root_dir, value)

        if not os.path.isfile(file):
            return None

        metas.append(read_audio_metadata(file))

    sample_rates = sorted({m["fs"] for m in metas})
    channels = sorted({m["num_channels"] for m in metas})
    subtypes = {m["subtype"] for m in metas}
    parser = (
        "as_audiofloat32" if subtypes & {"FLOAT", "DOUBLE", "PCM_24", "PCM_32"}
        else "as_audioint16"
    )
    fs_repr = ", ".join(f"{fs / 1000:g}" for fs in sample_rates)
    channels_repr = (
        "mono" if channels == [1]
        else f"{', '.join(str(c) for c in channels)} channel(s)"
    )
    description = f"{fs_repr} kHz, {channels_repr}, {', '.join(subtypes)}"

    if len(sample_rates) > 1:
        description += " (sample rates must be equal to pack)"

    return parser, description


def guess_parser(
        series: pl.Series,
        root_dir: str
) -> tuple[str | None, str]:
    """Guesses the parser of a column.

    Args:
        series (pl.Series): Column data.
        root_dir (str): Folder used to resolve relative audio paths.

    Returns:
        (tuple[str | None, str]): Parser name (or `None` if no parser is
            available) and a short comment describing the choice.
    """
    values = series.drop_nulls()
    sample = values.head(_NUM_SAMPLES).to_list()

    if len(sample) == 0:
        return None, "empty column"

    if series.dtype == pl.String:
        audio = _guess_audio_parser(sample, root_dir=root_dir)

        if audio is not None:
            return audio

        # NOTE: All values are used, since a single value may not fit in
        # the data type guessed from a sample
        list_parser = (
            _guess_list_parser(values.to_list())
            if _guess_list_parser(sample) is not None else None
        )

        if list_parser is not None:
            return list_parser, "list of numbers"

        num_unique = values.n_unique()

        if num_unique <= _MAX_CATEGORIES and num_unique < 0.5 * len(values):
            return "as_categorical", f"{num_unique} unique value(s)"

        return "as_utf8str", "text"

    if series.dtype.is_integer():
        min_value, max_value = values.min(), values.max()

        if _fits([min_value, max_value], -2 ** 7, 2 ** 7 - 1):
            return "as_int8", f"range [{min_value}, {max_value}]"

        if _fits([min_value, max_value], -2 ** 15, 2 ** 15 - 1):
            return "as_int16", f"range [{min_value}, {max_value}]"

        return "as_float64", f"range [{min_value}, {max_value}]"

    if series.dtype.is_float():
        return "as_float32", "use as_float64 for full precision"

    return None, f"type '{series.dtype}' is not supported"


def _quote(s: str) -> str:
    """Quotes a `str` for a `.yaml` file if needed.

    Args:
        s (str): Input `str`.

    Returns:
        (str): Quoted `str`, or `s` itself if it does not need quotes.
    """
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_./-]*", s) and s.lower() not in (
        "true", "false", "yes", "no", "on", "off", "null"
    ):
        return s

    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def cmd_init(args: Namespace) -> None:
    """Creates a `h5pack.yaml` configuration file from a `.csv` file.

    Args:
        args (Namespace): User input arguments provided through the console.
    """
    # Check input file
    if not is_file_with_ext(args.input, ext=".csv"):
        exit_error(f"Invalid input file '{args.input}'")

    if not args.output.endswith((".yaml", ".yml")):
        args.output += ".yaml"

    if os.path.isfile(args.output) and not args.overwrite:
        exit_error(
            f"File '{args.output}' already exists",
            hint="Use --overwrite to replace it"
        )

    # NOTE: Paths in h5pack.yaml are relative to the folder of h5pack.yaml
    root_dir = os.path.dirname(os.path.abspath(args.output))
    csv_file = os.path.relpath(os.path.abspath(args.input), root_dir)
    dataset_name = (
        args.dataset if args.dataset is not None
        else os.path.splitext(os.path.basename(args.input))[0]
    )

    try:
        df = pl.read_csv(args.input, has_header=True)

    except Exception as e:
        exit_error(f"Cannot read '{args.input}'", cause=str(e))

    lines = [
        "# h5pack configuration file created with 'h5pack init'. Please",
        "# review the parsers before running:",
        f"#   h5pack pack -c {os.path.basename(args.output)} -d "
        f"{_quote(dataset_name)} -o {dataset_name}.h5",
        "# All parsers: https://eagomez2.github.io/h5pack/parsers/",
        "datasets:",
        f"  {_quote(dataset_name)}:",
        "    attrs:",
        '      description: ""',
        "    data:",
        f"      file: {_quote(csv_file.replace(os.sep, '/'))}",
        "      fields:"
    ]
    num_fields = 0
    has_audio = False

    for col in df.columns:
        parser, comment = guess_parser(df[col], root_dir=root_dir)

        if parser is None:
            print_warning(f"Skipping column '{col}' ({comment})")
            continue

        # NOTE: '/' would create nested groups inside the .h5 file
        field_name = col.replace("/", "_")
        lines += [
            f"        {_quote(field_name)}:",
            f"          column: {_quote(col)}",
            f"          parser: {parser}  # {comment}"
        ]
        num_fields += 1
        has_audio = has_audio or parser.startswith("as_audio")

    if num_fields == 0:
        exit_error(f"No columns of '{args.input}' can be packed")

    if not has_audio:
        print_warning(
            "No audio columns found. Audio paths must be relative to the "
            f"folder of '{args.output}' or absolute"
        )

    if os.path.dirname(args.output) != "":
        os.makedirs(os.path.dirname(args.output), exist_ok=True)

    with open(args.output, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print_step(
        "Created",
        args.output,
        details=f"dataset '{dataset_name}', {num_fields} field(s)"
    )
    print_output(args.output)
