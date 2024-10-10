import polars as pl
from tqdm import tqdm
from ..core.guards import is_file_with_ext_or_error
from ..core.io import read_audio_metadata
from ..core.exceptions import (
    ChannelCountError,
    SampleRateError
)


def validate_attrs(data: dict) -> None:
    for k, v in data.items():
        if not isinstance(v, str):
            raise TypeError(
                f"Only str keys are supported. Found key '{k}' of type "
                f"'{v.__class__.__name__}'"
            )

def validate_file_as_audiofloat32(
        df: pl.DataFrame,
        col: str,
        verbose: bool = False
) -> None:
    # Get all files
    files = df[col].to_list()
    observed_fs = []

    for file in tqdm(
        files,
        desc=f"Validating '{col}'",
        leave=False,
        unit="row",
        disable=not verbose
    ):
        is_file_with_ext_or_error(file, ext=".wav")
        meta = read_audio_metadata(file)

        if meta["num_channels"] != 1:
            raise ChannelCountError(
                f"Currently only mono files are supported but '{file}' has "
                f"{meta['num_channels']} channels"
            )

        if meta["fs"] not in observed_fs:
            observed_fs.append(meta["fs"])

        if len(observed_fs) > 1:
            raise SampleRateError(
                "All files should have the same sample rate. Previous files "
                f"had sample rate {observed_fs[0]} but current file '{file}' "
                f"has sample rate {observed_fs[-1]}"
            )


def validate_file_as_audiofloat64() -> bool:
    ...

