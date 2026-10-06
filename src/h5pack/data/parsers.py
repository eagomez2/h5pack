import io
import os
import ast
import json
import h5py
import polars as pl
import numpy as np
import soundfile as sf
from ..core.io import (
    read_audio,
    read_audio_metadata,
    resample_audio
)
from ..core.guards import are_lists_equal_len


def get_common_dir(files: list[str], root_dir: str) -> str:
    """Returns the deepest folder shared by all `files`.

    Args:
        files (list[str]): Audio files. Relative paths are resolved from
            `root_dir`.
        root_dir (str): Root folder of the dataset.

    Returns:
        (str): Absolute path of the common folder.
    """
    return os.path.commonpath(
        [
            os.path.dirname(os.path.abspath(os.path.join(root_dir, f)))
            for f in files
        ]
    )


def get_resampled_len(num_samples: int, fs: int, target_fs: int) -> int:
    """Returns the number of samples of an audio signal after resampling.

    Args:
        num_samples (int): Number of samples per channel.
        fs (int): Original sample rate.
        target_fs (int): Target sample rate.

    Returns:
        (int): Number of samples per channel after resampling.
    """
    if fs == target_fs:
        return num_samples

    return int(round(num_samples * target_fs / fs))


def _create_dataset(
        group: h5py.Group,
        name: str,
        shape: tuple,
        dtype: np.dtype,
        ctx: dict,
        chunks: tuple | None = None
) -> h5py.Dataset:
    """Creates a dataset applying the compression settings stored in `ctx`.
    Variable-length data types are never compressed, since HDF5 filters only
    compress the references to their data.

    Args:
        group (h5py.Group): Group where the dataset will be created.
        name (str): Dataset name.
        shape (tuple): Dataset shape.
        dtype (np.dtype): Dataset data type.
        ctx (dict): Dictionary containing context variables.
        chunks (tuple | None): Chunk shape. If `None`, it is chosen
            automatically when compression is enabled.

    Returns:
        (h5py.Dataset): Created dataset.
    """
    kwargs = {}
    compression = ctx.get("compression")
    is_vlen = (
        h5py.check_vlen_dtype(np.dtype(dtype)) is not None
        or h5py.check_string_dtype(np.dtype(dtype)) is not None
    )

    if compression not in (None, "none") and not is_vlen:
        kwargs["compression"] = compression
        kwargs["shuffle"] = True
        kwargs["chunks"] = chunks if chunks is not None else True

        if compression == "gzip":
            kwargs["compression_opts"] = ctx.get("compression_level", 4)

    return group.create_dataset(name=name, shape=shape, dtype=dtype, **kwargs)


def _get_source_dir(common_dir: str, ctx: dict) -> str:
    """Returns the folder of the source audio files as stored in the `.h5`
    file. It is stored relative to the output file, so no absolute paths of
    the machine used for packing end up in the file.

    Args:
        common_dir (str): Absolute folder shared by all audio files.
        ctx (dict): Dictionary containing context variables.

    Returns:
        (str): Source folder relative to the output file, or absolute if it
            cannot be expressed as a relative path (e.g. different drives).
    """
    output_dir = ctx.get("output_dir")

    if output_dir is None:
        return common_dir

    try:
        return os.path.relpath(common_dir, output_dir).replace(os.sep, "/")

    except ValueError:  # Different drives on Windows
        return common_dir


def _encode_flac(
        file: str,
        data: np.ndarray | None,
        fs: int,
        subtype: str
) -> np.ndarray:
    """Returns the bytes of an audio file encoded as FLAC.

    Args:
        file (str): Original audio file. If it is a FLAC file and `data` is
            `None`, its bytes are returned without re-encoding.
        data (np.ndarray | None): Audio data with shape
            `(num_channels, num_samples)`. If `None`, `file` is read.
        fs (int): Sample rate.
        subtype (str): FLAC subtype (`PCM_16` or `PCM_24`).

    Returns:
        (np.ndarray): FLAC bytes as an `uint8` array.
    """
    if data is None and os.path.splitext(file)[1].lower() == ".flac":
        with open(file, "rb") as f:
            return np.frombuffer(f.read(), dtype=np.uint8)

    if data is None:
        data, _ = read_audio(file, dtype="float64")

    buffer = io.BytesIO()
    sf.write(buffer, data.T, samplerate=fs, format="FLAC", subtype=subtype)

    return np.frombuffer(buffer.getvalue(), dtype=np.uint8)


def _get_flac_subtype(subtypes: list[str]) -> str:
    """Returns the FLAC subtype able to represent all source subtypes.

    Args:
        subtypes (list[str]): Subtypes of the source audio files.

    Returns:
        (str): `PCM_24` if any source uses more than 16 bits, `PCM_16`
            otherwise.
    """
    high_res = ("PCM_24", "PCM_32", "FLOAT", "DOUBLE")
    return "PCM_24" if any(s in high_res for s in subtypes) else "PCM_16"


def _as_audiodtype(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        dtype: np.dtype | None,
        parser_name: str,
        ctx: dict,
        sample_rate: int | None = None
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
        dtype (np.dtype | None): Data type used to read the audio data. If
            `None`, audio data is stored as FLAC.
        parser_name (str): Name of parser method.
        ctx (dict): Dictionary containing context variables.
        sample_rate (int | None): If provided, all audio files are resampled
            to this sample rate.
    """
    # NOTE: Files are already validated at this point
    files = data_frame[data_column_name].to_list()[data_start_idx:data_end_idx]
    
    # Prepend root from context if path is relative
    files = [
        f if os.path.isabs(f) else os.path.join(ctx["root_dir"], f)
        for f in files
    ]

    # NOTE: Paths are stored relative to the folder shared by all files of the
    # column (computed before creating the partitions), so files with the same
    # name in different subfolders do not collide when unpacking
    field_ctx = ctx.get("fields", {}).get(partition_field_name, {})
    common_dir = field_ctx.get("common_dir")

    if common_dir is None:
        common_dir = get_common_dir(files, ctx["root_dir"])

    # Get metadata of all files
    metas = [read_audio_metadata(file) for file in files]
    num_channels = metas[0]["num_channels"]
    target_fs = sample_rate if sample_rate is not None else metas[0]["fs"]
    resampled = any(m["fs"] != target_fs for m in metas)
    output_lens = [
        get_resampled_len(m["num_samples_per_channel"], m["fs"], target_fs)
        for m in metas
    ]

    # Check if files are fixed length or vlen
    vlen = len(set(output_lens)) > 1
    is_flac = dtype is None

    # Add group data
    if is_flac:
        dataset = _create_dataset(
            group=partition_data_group,
            name=partition_field_name,
            shape=(len(files),),
            dtype=h5py.vlen_dtype(np.dtype(np.uint8)),
            ctx=ctx
        )
        flac_subtype = _get_flac_subtype([m["subtype"] for m in metas])
        dataset.attrs["codec"] = "flac"
        dataset.attrs["flac_subtype"] = flac_subtype

    elif vlen:
        dataset = _create_dataset(
            group=partition_data_group,
            name=partition_field_name,
            shape=(len(files),),
            dtype=h5py.vlen_dtype(np.dtype(dtype)),
            ctx=ctx
        )
    
    else:
        # NOTE: Mono audio keeps the (num_files, num_samples) layout used by
        # previous versions
        audio_shape = (
            (output_lens[0],) if num_channels == 1
            else (num_channels, output_lens[0])
        )
        dataset = _create_dataset(
            group=partition_data_group,
            name=partition_field_name,
            shape=(len(files), *audio_shape),
            dtype=dtype,
            ctx=ctx,
            chunks=(1, *audio_shape)
        )
    
    # Add auxiliary meta data for audio files
    dataset.attrs["parser"] = parser_name
    dataset.attrs["sample_rate"] = str(target_fs)
    dataset.attrs["num_channels"] = num_channels
    dataset.attrs["source_dir"] = _get_source_dir(common_dir, ctx)

    if resampled:
        dataset.attrs["resampled"] = True

    if not ctx.get("skip_filepaths", False):
        filenames_dataset = partition_data_group.create_dataset(
            name=f"{partition_field_name}__filepath",
            shape=(len(files),),
            dtype=h5py.string_dtype()
        )
    
    else:
        filenames_dataset = None

    for idx, (file, meta) in enumerate(zip(files, metas, strict=True)):
        needs_resampling = meta["fs"] != target_fs

        if is_flac:
            data = None

            if needs_resampling:
                data, _ = read_audio(file, dtype="float64")
                data = resample_audio(
                    data,
                    fs=meta["fs"],
                    target_fs=target_fs,
                    num_samples=output_lens[idx]
                )

            dataset[idx] = _encode_flac(
                file,
                data=data,
                fs=target_fs,
                subtype=flac_subtype
            )

        else:
            data, _ = read_audio(file, dtype=dtype)

            if needs_resampling:
                data = resample_audio(
                    data,
                    fs=meta["fs"],
                    target_fs=target_fs,
                    num_samples=output_lens[idx]
                )

            if num_channels == 1:
                data = data[0]

            if vlen:
                # NOTE: Multichannel vlen audio is stored channel by channel
                # and restored using the 'num_channels' attribute
                dataset[idx] = data.reshape(-1)
            
            else:
                dataset[idx] = data

        # Store path relative to the common folder of all files
        if filenames_dataset is not None:
            filenames_dataset[idx] = (
                os.path.relpath(os.path.abspath(file), common_dir)
                .replace(os.sep, "/")
            )
        
        # Update progress bar
        ctx["queue"].put((partition_idx, partition_field_name, 1))


def as_audioint16(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        ctx: dict,
        sample_rate: int | None = None
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
        ctx=ctx,
        sample_rate=sample_rate
    )


def as_audiofloat32(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        ctx: dict,
        sample_rate: int | None = None
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
        ctx=ctx,
        sample_rate=sample_rate
    )

    
def as_audiofloat64(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        ctx: dict,
        sample_rate: int | None = None
) -> None:
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
        ctx=ctx,
        sample_rate=sample_rate
    )


def as_audioflac(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        ctx: dict,
        sample_rate: int | None = None
) -> None:
    """Alias of generic parser for audio data stored as FLAC bytes."""
    return _as_audiodtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        dtype=None,
        parser_name="as_audioflac",
        ctx=ctx,
        sample_rate=sample_rate
    )


def _as_dtype(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: int,
    data_end_idx: int,
    dtype: np.dtype,
    parser_name: str,
    ctx: dict, 
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
    dataset = _create_dataset(
        group=partition_data_group,
        name=partition_field_name,
        shape=(len(metrics),),
        dtype=dtype,
        ctx=ctx
    )
    dataset.attrs["parser"] = parser_name

    for idx, metric in enumerate(metrics):
        dataset[idx] = metric
        ctx["queue"].put((partition_idx, partition_field_name, 1))


def as_int8(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        ctx: dict
) -> None:
    """Alias of generic parser for single value data as `int8`."""
    _as_dtype(
        partition_idx=partition_idx,
        partition_data_group=partition_data_group,
        partition_field_name=partition_field_name,
        data_frame=data_frame,
        data_column_name=data_column_name,
        data_start_idx=data_start_idx,
        data_end_idx=data_end_idx,
        dtype=np.int8,
        parser_name="as_int8",
        ctx=ctx
    )


def as_int16(
        partition_idx: int,
        partition_data_group: h5py.Group,
        partition_field_name: str,
        data_frame: pl.DataFrame,
        data_column_name: str,
        data_start_idx: int,
        data_end_idx: int,
        ctx: dict
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
        data_start_idx: int,
        data_end_idx: int,
        ctx: dict
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
    data_start_idx: int,
    data_end_idx: int,
    ctx: dict
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
    data_start_idx: int,
    data_end_idx: int,
    ctx: dict
) -> None:
    """Alias of generic parser for single value data as `str`."""
    values = (
        data_frame[data_column_name].to_list()[data_start_idx:data_end_idx]
    )

    # Add group data
    dataset = _create_dataset(
        group=partition_data_group,
        name=partition_field_name,
        shape=(len(values),),
        dtype=h5py.string_dtype(encoding="utf-8"),
        ctx=ctx
    )
    dataset.attrs["parser"] = "as_utf8str"

    for idx, value in enumerate(values):
        # Store data
        dataset[idx] = value

        # Update progress bar
        ctx["queue"].put((partition_idx, partition_field_name, 1))


def _as_listdtype(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    dtype: np.dtype,
    parser_name: str,
    data_start_idx: int,
    data_end_idx: int,
    ctx: dict
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
        dataset = _create_dataset(
            group=partition_data_group,
            name=partition_field_name,
            shape=(len(lists),),
            dtype=h5py.vlen_dtype(np.dtype(dtype)),
            ctx=ctx
        )
    
    else:
        dataset = _create_dataset(
            group=partition_data_group,
            name=partition_field_name,
            shape=(len(lists), len(lists[0])),
            dtype=dtype,
            ctx=ctx
        )

    dataset.attrs["parser"] = parser_name

    for idx, data in enumerate(lists):
        if vlen:
            dataset[idx] = data

        else:
            dataset[idx, :] = data
        
        ctx["queue"].put((partition_idx, partition_field_name, 1))


def as_listint8(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: int,
    data_end_idx: int,
    ctx: dict
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
    data_start_idx: int,
    data_end_idx: int,
    ctx: dict
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
    data_start_idx: int,
    data_end_idx: int,
    ctx: dict
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
    data_start_idx: int,
    data_end_idx: int,
    ctx: dict
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


def get_categories(values: list) -> list:
    """Returns the sorted unique values of a column used by `as_categorical`.

    Args:
        values (list): Column values.

    Returns:
        (list): Sorted unique values.
    """
    return sorted(set(values), key=lambda v: (str(type(v)), v))


def get_categorical_dtype(num_categories: int) -> np.dtype:
    """Returns the smallest unsigned integer type able to store all category
    codes.

    Args:
        num_categories (int): Number of categories.

    Returns:
        (np.dtype): `uint8`, `uint16` or `uint32`.
    """
    if num_categories <= 2 ** 8:
        return np.dtype(np.uint8)

    if num_categories <= 2 ** 16:
        return np.dtype(np.uint16)

    return np.dtype(np.uint32)


def as_categorical(
    partition_idx: int,
    partition_data_group: h5py.Group,
    partition_field_name: str,
    data_frame: pl.DataFrame,
    data_column_name: str,
    data_start_idx: int,
    data_end_idx: int,
    ctx: dict
) -> None:
    """Parses columns with a small set of repeated values (e.g. labels,
    splits or speaker ids). Each value is stored as an integer code and the
    list of categories is stored in the `categories` attribute as JSON.

    Args:
        partition_idx (int): Partition index.
        partition_data_group (h5py.Group): Data group where the data will be
            written.
        partition_field_name (str): Field name where the data will be stored.
        data_frame (pl.DataFrame): `DataFrame` containing the data.
        data_column_name (str): Column name where the data is stored.
        data_start_idx (int): Index of first row to parse.
        data_end_idx (int): Index of last row to parse.
        ctx (dict): Dictionary containing context variables.
    """
    # NOTE: Categories are computed using the full column before creating the
    # partitions, so codes are consistent across partitions
    field_ctx = ctx.get("fields", {}).get(partition_field_name, {})
    categories = field_ctx.get("categories")

    if categories is None:
        categories = get_categories(data_frame[data_column_name].to_list())

    category_to_code = {c: i for i, c in enumerate(categories)}
    values = (
        data_frame[data_column_name].to_list()[data_start_idx:data_end_idx]
    )

    dataset = _create_dataset(
        group=partition_data_group,
        name=partition_field_name,
        shape=(len(values),),
        dtype=get_categorical_dtype(len(categories)),
        ctx=ctx
    )
    dataset.attrs["parser"] = "as_categorical"
    dataset.attrs["categories"] = json.dumps(categories)
    dataset[:] = np.array(
        [category_to_code[v] for v in values],
        dtype=dataset.dtype
    )
    ctx["queue"].put((partition_idx, partition_field_name, len(values)))
