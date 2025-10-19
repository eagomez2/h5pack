import os
import ast
import h5py
import polars as pl
import numpy as np
from typing import (
    List,
    Optional
)
from ..core.io import (
    read_audio,
    read_audio_metadata
)
from ..core.guards import are_lists_equal_len


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
        ctx: dict = {}
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
        ctx (dict): Dictionary containing context variables.
        verbose (bool): Enable verbose mode if `True`.
    """
    # NOTE: Files are already validated at this point
    files = data_frame[data_column_name].to_list()[data_start_idx:data_end_idx]
    
    # Prepend root from context if path is relative
    files = [
        f if os.path.isabs(f) else os.path.join(ctx["root_dir"], f)
        for f in files
    ]

    # Check if files are fixed length or vlen
    observed_lens = []
    vlen = False

    for idx, file in enumerate(files):
        # Get number of samples
        num_samples = read_audio_metadata(file)["num_samples_per_channel"]

        if num_samples not in observed_lens:
            observed_lens.append(num_samples)
        
        if len(observed_lens) > 1:
            vlen = True
            break
    
    # NOTE: All audios have the same sample rate
    fs = read_audio_metadata(files[0])["fs"]

    # Add group data
    if not vlen:
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
    
    # Add auxiliary meta data for audio filemeta data for audio files
    dataset.attrs["parser"] = parser_name
    dataset.attrs["sample_rate"] = str(fs)

    filenames_dataset = partition_data_group.create_dataset(
        name=f"{partition_field_name}__filepath",
        shape=(len(files),),
        dtype=h5py.string_dtype()
    )

    for idx, file in enumerate(files):
        data, _ = read_audio(file, dtype=dtype)

        if vlen:
            dataset[idx] = data
        
        else:
            dataset[idx, :] = data

        # Store filename only
        filenames_dataset[idx] = os.path.basename(file)
        
        # Update progress bar
        ctx["queue"].put((partition_idx, 1))


def as_audioint16(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        ctx: dict = {}
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
        ctx=ctx
    )


def as_audiofloat32(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        ctx: dict = {}
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
        ctx=ctx
    )

    
def as_audiofloat64(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        ctx: dict = {},
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
        ctx=ctx
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
    ctx: dict = {}, 
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
        ctx (dict): Dictionary containing context variables.
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

    for idx, metric in enumerate(metrics):
        dataset[idx] = metric
        ctx["queue"].put((partition_idx, 1))


def as_int8(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: Optional[int] = None,
        data_end_idx: Optional[int] = None,
        ctx: dict = {}
) -> None:
    """Alias of generic parser for single value data as `int8`."""
    _as_dtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        dtype=np.int8,
        parser_name="as_int8",
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        ctx=ctx
    )


def as_int16(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: Optional[int] = None,
        data_end_idx: Optional[int] = None,
        ctx: dict = {}
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
        ctx=ctx
    )


def as_float32(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: Optional[int] = None,
        data_end_idx: Optional[int] = None,
        ctx: dict = {}
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
        ctx=ctx
    )


def as_float64(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: Optional[int] = None,
    data_end_idx: Optional[int] = None,
    ctx: dict = {}
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
        ctx=ctx
    )


def as_utf8str(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: Optional[int] = None,
    data_end_idx: Optional[int] = None,
    ctx: dict = {}
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
    dataset.attrs["parser"] = "as_utf8str"

    for idx, value in enumerate(values):
        # Store data
        dataset[idx] = value

        # Update progress bar
        ctx["queue"].put((partition_idx, 1))


def _as_listdtype(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    dtype: np.dtype,
    parser_name: str,
    data_start_idx: Optional[int] = None,
    data_end_idx: Optional[int] = None,
    ctx: dict = {}
) -> None:
    """Parses columns having a list of objects of a single data type such
    as `int16` or `float32`.
    
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
        ctx (dict): Dictionary containing context variables.
    """
    # Get lists as str
    lists = (
        data_frame[data_column_name].to_list()[data_start_idx:data_end_idx]
    )

    # Transform list to dtype
    lists = [ast.literal_eval(li) for li in lists]

    # Check if lists have same length
    vlen = not are_lists_equal_len(*lists)

    # Transform lists to target data type
    lists = [np.array(li, dtype=dtype) for li in lists]
    
    # Add group data
    if vlen:
        dataset = partition_data_group.create_dataset(
            name=partition_field_name,
            shape=(len(lists),),
            dtype=h5py.vlen_dtype(np.dtype(dtype))
        )
    
    else:
        dataset = partition_data_group.create_dataset(
            name=partition_field_name,
            shape=(len(lists), len(lists[0])),
            dtype=dtype
        )

    dataset.attrs["parser"] = parser_name

    for idx, data in enumerate(lists):
        if vlen:
            dataset[idx] = data

        else:
            dataset[idx, :] = data
        
        ctx["queue"].put((partition_idx, 1))


def as_listint8(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: Optional[int] = None,
    data_end_idx: Optional[int] = None,
    ctx: dict = {}
) -> None:
    """Alias of generic parser for list of `int8` values."""
    _as_listdtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        dtype=np.int8,
        parser_name="as_listint8",
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        ctx=ctx
    )


def as_listint16(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: Optional[int] = None,
    data_end_idx: Optional[int] = None,
    ctx: dict = {}
) -> None:
    """Alias of generic parser for list of `int16` values."""
    _as_listdtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        dtype=np.int16,
        parser_name="as_listint16",
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        ctx=ctx
    )


def as_listfloat32(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: Optional[int] = None,
    data_end_idx: Optional[int] = None,
    ctx: dict = {}
) -> None:
    """Alias of generic parser for list of `float32` values."""
    _as_listdtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        dtype=np.float32,
        parser_name="as_listfloat32",
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        ctx=ctx
    )


def as_listfloat64(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: Optional[int] = None,
    data_end_idx: Optional[int] = None,
    ctx: dict = {}
) -> None:
    """Alias of generic parser for list of `float64` values."""
    _as_listdtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        dtype=np.float64,
        parser_name="as_listfloat64",
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        ctx=ctx
    )
