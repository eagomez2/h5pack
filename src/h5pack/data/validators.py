import os
import ast
import yaml
import numpy as np
import polars as pl
from collections.abc import Callable
from ..core.config import get_allowed_audio_extensions
from ..core.display import exit_error
from ..core.guards import (
    has_ext,
    is_file_with_ext_or_error
)
from ..core.io import read_audio_metadata
from ..core.exceptions import (
    ChannelCountError,
    SampleRateError
)


def validate_config_file(file: str, ctx: dict) -> dict:
    """Validate configuration file in `.yaml format.
    
    Args:
        file (str): Configuration `.yaml` file.
        ctx (dict): Context.
    
    Returns:
        (dict): Validate specs data.
    """
    # Open .yaml file
    try:
        with open(file, encoding="utf-8") as f:
            specs = yaml.safe_load(f)
    
    except Exception as e:
        exit_error(f"Configuration file could not be parsed: {e}")
    
    # NOTE: Imported here to avoid a circular import
    from . import get_parsers_map

    parser_names = sorted(
        {name for parsers in get_parsers_map().values() for name in parsers}
    )

    # Check 'datasets' key exists (mandatory)
    if not isinstance(specs, dict) or "datasets" not in specs:
        exit_error(f"Missing 'datasets' key in '{file}'")
    
    # Validate individual datasets
    datasets = specs["datasets"]

    for dataset_name, dataset_config in datasets.items():
        # Validate attrs if any (attrs are optional)
        for attr_name, attr_value in dataset_config.get("attrs", {}).items():
            if not isinstance(attr_value, str):
                exit_error(
                    "Attributes can only be of type str. Found key "
                    f"'{attr_name}' of 'attrs' of dataset '{dataset_name}'"
                    f" of type '{attr_value.__class__.__name__}'"
                )
        
        if "data" not in dataset_config:
            exit_error(f"Missing 'data' key in dataset '{dataset_name}'")

        if "file" not in dataset_config["data"]:
            exit_error(f"Missing 'file' key in dataset '{dataset_name}'")
        
        # Use context root dir to validate data file
        data_file = os.path.join(
            ctx["root_dir"],
            dataset_config["data"]["file"]
        )

        # NOTE: Only the extension is validated since existance of file should
        # be validated at runtime
        if not has_ext(data_file, ext=".csv"):
            exit_error(
                f"Invalid data file '{data_file}' in dataset "
                f"'{dataset_name}'"
            )
        
        if "fields" not in dataset_config["data"]:
            exit_error(f"Missing 'fields' key in dataset '{dataset_name}'")
        
        if len(dataset_config["data"]["fields"]) == 0:
            exit_error(f"0 fields found in dataset '{dataset_name}'")
        
        for field_name, field_data in dataset_config["data"]["fields"].items():
            if "column" not in field_data:
                exit_error(
                    f"Missing 'column' key for field '{field_name}' in "
                    f"dataset '{dataset_name}'"
                )
            
            if "parser" not in field_data:
                exit_error(
                    f"Missing 'parser' key for field '{field_name}' in dataset"
                    f" '{dataset_name}'"
                )

            if field_data["parser"] not in parser_names:
                exit_error(
                    f"Unknown parser '{field_data['parser']}' for field "
                    f"'{field_name}' in dataset '{dataset_name}'",
                    hint=f"Available parsers: {', '.join(parser_names)}"
                )

            # NOTE: No parser accepts arguments at the moment
            for arg_name in field_data.get("parser_args", {}) or {}:
                exit_error(
                    f"Unknown parser argument '{arg_name}' for field "
                    f"'{field_name}' in dataset '{dataset_name}'",
                    hint="Parsers do not accept arguments"
                )
    
    return specs


def _validate_not_null(df: pl.DataFrame, col: str) -> None:
    """Raises an exception if a column contains empty values.

    Args:
        df (pl.DataFrame): `DataFrame` containing the data.
        col (str): Column name.

    Raises:
        ValueError: If the column contains empty values.
    """
    null_rows = df.with_row_index().filter(pl.col(col).is_null())["index"]

    if len(null_rows) > 0:
        rows_repr = ", ".join(str(r) for r in null_rows.to_list()[:5])
        raise ValueError(
            f"Column '{col}' has {len(null_rows)} empty value(s) (row(s) "
            f"{rows_repr}{', ...' if len(null_rows) > 5 else ''})"
        )


def _validate_range(
        df: pl.DataFrame,
        col: str,
        dtype: np.dtype
) -> None:
    """Raises an exception if the values of a column do not fit in an integer
    data type.

    Args:
        df (pl.DataFrame): `DataFrame` containing the data.
        col (str): Column name.
        dtype (np.dtype): Integer data type.

    Raises:
        ValueError: If any value is out of the range of `dtype`.
    """
    info = np.iinfo(dtype)
    col_min, col_max = df[col].min(), df[col].max()

    if col_min < info.min or col_max > info.max:
        raise ValueError(
            f"Values of column '{col}' range from {col_min} to {col_max}, "
            f"which does not fit in '{np.dtype(dtype).name}' ({info.min} to "
            f"{info.max})"
        )


def _validate_scalar(
        df: pl.DataFrame,
        col: str,
        ctx: dict,
        dtype: np.dtype | None = None,
        **kwargs
) -> None:
    """Generic validator of single value columns.

    Args:
        df (pl.DataFrame): `DataFrame` containing the data.
        col (str): Column name.
        ctx (dict): Validation context.
        dtype (np.dtype | None): Integer data type used to check the range of
            the values. If `None`, the range is not checked.
    """
    _validate_not_null(df, col)

    if dtype is not None and np.issubdtype(dtype, np.integer):
        _validate_range(df, col, dtype)


def _validate_list(
        df: pl.DataFrame,
        col: str,
        ctx: dict,
        dtype: np.dtype,
        **kwargs
) -> None:
    """Generic validator of columns containing lists of numbers.

    Args:
        df (pl.DataFrame): `DataFrame` containing the data.
        col (str): Column name.
        ctx (dict): Validation context.
        dtype (np.dtype): Data type used to store the values.
    """
    _validate_not_null(df, col)

    for row_idx, value in enumerate(df[col].to_list()):
        try:
            values = ast.literal_eval(value)

        except (ValueError, SyntaxError):
            raise ValueError(
                f"Value '{value}' of column '{col}' (row {row_idx}) is not a "
                "valid list"
            ) from None

        if not isinstance(values, (list, tuple)):
            raise ValueError(
                f"Value '{value}' of column '{col}' (row {row_idx}) is not a "
                "list"
            )

        try:
            array = np.array(values, dtype=np.float64)

        except (ValueError, TypeError):
            raise ValueError(
                f"List '{value}' of column '{col}' (row {row_idx}) contains "
                "non-numeric values"
            ) from None

        if array.ndim != 1:
            raise ValueError(
                f"List '{value}' of column '{col}' (row {row_idx}) must be a "
                "flat list of numbers"
            )

        if np.issubdtype(dtype, np.integer) and len(array) > 0:
            info = np.iinfo(dtype)

            if array.min() < info.min or array.max() > info.max:
                raise ValueError(
                    f"List '{value}' of column '{col}' (row {row_idx}) has "
                    f"values that do not fit in '{np.dtype(dtype).name}'"
                )


def _validate_file_as_audiodtype(
        df: pl.DataFrame,
        col: str,
        ctx: dict,
        max_channels: int | None = None,
        **kwargs
) -> None:
    """Generic validator of audio types. A summary of the audio files (sample
    rates, channels and subtypes) is stored in `ctx["audio_info"][col]`.
    
    Args:
        df (pl.DataFrame): `DataFrame` containing the data with the column with
            audio file paths.
        col (str): Column name.
        ctx (dict): Validation context.
        max_channels (int | None): Maximum number of channels allowed.
    """
    _validate_not_null(df, col)

    # Get all files
    files = df[col].to_list()
    observed_fs = []
    observed_channels = []
    observed_subtypes = []
    progress_bar = ctx["progress_bar"]

    with progress_bar:
        # Add task
        task = progress_bar.add_task(f"Validating '{col}'", total=len(files))

        for file in files:
            # Make step
            progress_bar.advance(task)
            
            # Solve path
            file = (
                os.path.join(ctx["root_dir"], file) if not os.path.isabs(file)
                else file
            )

            is_file_with_ext_or_error(file, ext=get_allowed_audio_extensions())
            meta = read_audio_metadata(file)

            if meta["num_channels"] not in observed_channels:
                observed_channels.append(meta["num_channels"])

            if len(observed_channels) > 1:
                raise ChannelCountError(
                    "All files should have the same number of channels. "
                    f"Previous files had {observed_channels[0]} channel(s) "
                    f"but current file '{file}' has {meta['num_channels']} "
                    "channel(s)"
                )

            if (
                max_channels is not None
                and meta["num_channels"] > max_channels
            ):
                raise ChannelCountError(
                    f"At most {max_channels} channels are supported but "
                    f"'{file}' has {meta['num_channels']} channels"
                )

            if meta["fs"] not in observed_fs:
                observed_fs.append(meta["fs"])

            if len(observed_fs) > 1:
                raise SampleRateError(
                    "All files should have the same sample rate. Previous "
                    f"files had sample rate {observed_fs[0]} but current file "
                    f"'{file}' has sample rate {observed_fs[-1]}"
                )

            if meta["subtype"] not in observed_subtypes:
                observed_subtypes.append(meta["subtype"])

    ctx.setdefault("audio_info", {})[col] = {
        "sample_rates": observed_fs,
        "num_channels": observed_channels[0] if observed_channels else 1,
        "subtypes": observed_subtypes
    }


def validate_file_as_audioint16(
        df: pl.DataFrame,
        col: str,
        ctx: dict,
        **kwargs
) -> None:
    """Alias of generic method to validate audio as `int16`."""
    return _validate_file_as_audiodtype(df=df, col=col, ctx=ctx, **kwargs)


def validate_file_as_audiofloat32(
        df: pl.DataFrame,
        col: str,
        ctx: dict,
        **kwargs
) -> None:
    """Alias of generic method to validate audio as `float32`."""
    return _validate_file_as_audiodtype(df=df, col=col, ctx=ctx, **kwargs)


def validate_file_as_audiofloat64(
        df: pl.DataFrame,
        col: str,
        ctx: dict,
        **kwargs
) -> None:
    """Alias of generic method to validate audio as `float64`."""
    return _validate_file_as_audiodtype(df=df, col=col, ctx=ctx, **kwargs)


def validate_file_as_audioflac(
        df: pl.DataFrame,
        col: str,
        ctx: dict,
        **kwargs
) -> None:
    """Alias of generic method to validate audio stored as FLAC."""
    # NOTE: FLAC supports up to 8 channels
    return _validate_file_as_audiodtype(
        df=df,
        col=col,
        ctx=ctx,
        max_channels=8,
        **kwargs
    )


def validate_as_scalar(dtype: np.dtype | None = None) -> Callable:
    """Returns a validator of single value columns.

    Args:
        dtype (np.dtype | None): Data type used to store the values.

    Returns:
        (Callable): Validator method.
    """
    def validator(df: pl.DataFrame, col: str, ctx: dict, **kwargs) -> None:
        _validate_scalar(df=df, col=col, ctx=ctx, dtype=dtype, **kwargs)

    return validator


def validate_as_list(dtype: np.dtype) -> Callable:
    """Returns a validator of columns containing lists of numbers.

    Args:
        dtype (np.dtype): Data type used to store the values.

    Returns:
        (Callable): Validator method.
    """
    def validator(df: pl.DataFrame, col: str, ctx: dict, **kwargs) -> None:
        _validate_list(df=df, col=col, ctx=ctx, dtype=dtype, **kwargs)

    return validator
