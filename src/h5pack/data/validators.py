import os
import yaml
import polars as pl
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn
)
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
        with open(file, "r") as f:
            specs = yaml.safe_load(f)
    
    except Exception as e:
        exit_error(f"Configuration file could not be parsed: {e}")
    
    # Check 'datasets' key exists (mandatory)
    if "datasets" not in specs:
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
                    f"'{dataset_name}'"
                )
    
    return specs


def _validate_file_as_audiodtype(
        df: pl.DataFrame,
        col: str,
        ctx: dict
) -> None:
    """Generic validator of audio types.
    
    Args:
        df (pl.DataFrame): `DataFrame` containing the data with the column with
            audio file paths.
        col (str): Column name.
        ctx (dict): Validation context.
    """
    # Get all files
    files = df[col].to_list()
    observed_fs = []
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

            if meta["num_channels"] != 1:
                raise ChannelCountError(
                    f"Currently only mono files are supported but '{file}' has"
                    f" {meta['num_channels']} channels"
                )

            if meta["fs"] not in observed_fs:
                observed_fs.append(meta["fs"])

            if len(observed_fs) > 1:
                raise SampleRateError(
                    "All files should have the same sample rate. Previous "
                    f"files had sample rate {observed_fs[0]} but current file "
                    f"'{file}' has sample rate {observed_fs[-1]}"
                )


def validate_file_as_audioint16(
        df: pl.DataFrame,
        col: str,
        ctx: dict,
) -> None:
    """Alias of generic method to validate audio as `int16`."""
    return _validate_file_as_audiodtype(
        df=df,
        col=col,
        ctx=ctx
    )


def validate_file_as_audiofloat32(
        df: pl.DataFrame,
        col: str,
        ctx: dict
) -> None:
    """Alias of generic method to validate audio as `float32`."""
    return _validate_file_as_audiodtype(
        df=df,
        col=col,
        ctx=ctx
    )


def validate_file_as_audiofloat64(
        df: pl.DataFrame,
        col:str,
        ctx: dict
) -> None:
    """Alias of generic method to validate audio as `float64`."""
    return _validate_file_as_audiodtype(
        df=df,
        col=col,
        ctx=ctx
    )
