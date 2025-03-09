import h5py
import polars as pl
import numpy as np
import multiprocessing as mp
from typing import (
    List,
    Optional
)
from tqdm import tqdm
from ..core.io import (
    read_audio,
    read_audio_metadata
)


def _as_audiodtype(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        dtype: np.dtype,
        parser_name: str,
        verbose: bool = False
) -> None:
    """Parses audio file paths to extract audio data that will be written to
    a `.h5`file.
    
    Args:
        partition_idx (int): Partition index.
        partition_data_group (h5py.Group): Data group where the audio data will
            be written.
        partition_field_name (str): Field name where the data will be stored.
        data_frame (pl.DataFrame): `DataFrame` containing the list of audio
            file paths.
        data_column_name (str): Column name where the audio data is stored.
        data_start_idx (int): Index of first row to parse.
        data_end_idx (int): Index of last row to parse.
        dtype (np.dtype): Data type used to read the audio data.
        parser_name (str): Name of parser method.
        verbose (bool): Enable verbose mode if `True`.
    """
    # NOTE: Files are already validated at this point
    files = data_frame[data_column_name].to_list()[data_start_idx:data_end_idx]

    # Check if files are fixed length or vlen
    observed_lens = []
    vlen = False

    for file in files:
        num_samples = read_audio_metadata(file)["num_samples_per_channel"]

        if num_samples not in observed_lens:
            observed_lens.append(num_samples)
        
        if len(observed_lens) > 1:
            vlen = True
            break
    
    # NOTE: All audios have the same sample rate
    fs = read_audio_metadata(files[0])["fs"]

    # Add group data
    if vlen is False:
        dataset = partition_data_group.create_dataset(
            name=partition_field_name,
            shape=(len(files), num_samples),
            dtype=dtype
        )
    
    else:
        dataset = partition_data_group.create_dataset(
            name=partition_field_name,
            shape=(len(files),),
            dtype=h5py.vlen_dtype(np.dtype(dtype))
        )
    
    dataset.attrs["parser"] = parser_name
    dataset.attrs["sample_rate"] = str(fs)

    filenames_dataset = partition_data_group.create_dataset(
        name=f"{partition_field_name}_filepaths",
        shape=(len(files),),
        dtype=h5py.string_dtype()
    )

    for idx, file in enumerate(
        tqdm(
            files,
            desc=(
                f"Writing '{partition_field_name}' in partition "
                f"#{partition_idx}"
            ),
            colour="green",
            leave=False,
            position=(
                0 if mp.current_process().name == "MainProcess"
                else partition_idx
            ),
            disable=not verbose
        )
    ):
        data, _ = read_audio(file, dtype=dtype)

        if vlen:
            dataset[idx] = data
        
        else:
            dataset[idx, :] = data

        filenames_dataset[idx] = file


def as_audioint16(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        verbose: bool = False
) -> None:
    """Alias of generic parser for audio data as `int16`."""
    return _as_audiodtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        dtype=np.int16,
        parser_name="as_audioint16",
        verbose=verbose
    )


def as_audiofloat32(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        verbose: bool = False
) -> None:
    """Alias of generic parser for audio data as `float32`."""
    return _as_audiodtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        dtype=np.float32,
        parser_name="as_audiofloat32",
        verbose=verbose
    )

    
def as_audiofloat64(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        verbose: bool = False
) -> List[np.ndarray]:
    """Alias of generic parser for audio data as `float64`."""
    return _as_audiodtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        dtype=np.float64,
        parser_name="as_audiofloat64",
        verbose=verbose
    )


def _as_dtype(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    dtype: np.dtype,
    parser_name: str,
    data_start_idx: Optional[int] = None,
    data_end_idx: Optional[int] = None,
    verbose: bool = False
) -> None:
    """Parses columns having single objects data types such as a single `int16`
    value or `str`.
    
    Args:
        partition_idx (int): Partition index.
        partition_data_group (h5py.Group): Data group where the audio data will
            be written.
        partition_field_name (str): Field name where the data will be stored.
        data_frame (pl.DataFrame): `DataFrame` containing the list of audio
            file paths.
        data_column_name (str): Column name where the audio data is stored.
        data_start_idx (int): Index of first row to parse.
        data_end_idx (int): Index of last row to parse.
        dtype (np.dtype): Data type used to read the audio data.
        parser_name (str): Name of parser method.
        verbose (bool): Enable verbose mode it `True`.
    """
    metrics = (
        data_frame[data_column_name].to_list()[data_start_idx:data_end_idx]
    )

    # Add group data
    dataset = partition_data_group.create_dataset(
        name=partition_field_name,
        shape=(len(metrics),),
        dtype=dtype
    )
    dataset.attrs["parser"] = parser_name

    for idx, metric in enumerate(
        tqdm(
            metrics,
            desc=(
                f"Writing '{partition_field_name}' in partition "
                f"#{partition_idx}"
            ),
            colour="green",
            leave=False,
            disable=not verbose
        )
    ):
        dataset[idx] = metric


def as_int16(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: Optional[int] = None,
        data_end_idx: Optional[int] = None,
        verbose: bool = False
) -> None:
    """Alias of generic parser for single value data as `int16`."""
    _as_dtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        dtype=np.int16,
        parser_name="as_int16",
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        verbose=verbose
    )


def as_float32(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: Optional[int] = None,
        data_end_idx: Optional[int] = None,
        verbose: bool = False
) -> None:
    """Alias of generic parser for single value data as `float32`."""
    _as_dtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        dtype=np.float32,
        parser_name="as_float32",
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        verbose=verbose
    )


def as_float64(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: Optional[int] = None,
    data_end_idx: Optional[int] = None,
    verbose: bool = False
) -> None:
    """Alias of generic parser for single value data as `float64`."""
    _as_dtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        dtype=np.float64,
        parser_name="as_float64",
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        verbose=verbose
    )


def as_utf8_str(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: Optional[int] = None,
    data_end_idx: Optional[int] = None,
    verbose: bool = False
) -> None:
    """Alias of generic parser for single value data as `str`."""
    values = (
        data_frame[data_column_name].to_list()[data_start_idx:data_end_idx]
    )

    # Add group data
    dataset = partition_data_group.create_dataset(
        name=partition_field_name,
        shape=((len(values),)),
        dtype=h5py.string_dtype(encoding="utf-8")
    )
    dataset.attrs["parser"] = "as_utf8_str"

    for idx, value in enumerate(
        tqdm(
            values, 
            desc=(
                f"Writing '{partition_field_name}' in partition "
                f"#{partition_idx}"
            ),
            colour="green",
            leave=False,
            disable=not verbose
        )
    ):
        dataset[idx] = value
