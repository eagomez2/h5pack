import os
import h5py
import polars as pl
import numpy as np
from typing import List
from tqdm import tqdm
from ..core.io import (
    read_audio,
    read_audio_metadata
)


def as_audiofloat32(
        partition_idx: int,
        field_name: str,
        data_group: h5py.Group,
        data_df: pl.DataFrame,
        df_start_idx: int,
        df_end_idx: int,
        col: str,
        pbar_color: str = "green",
        verbose: bool = False
) -> None:
    # NOTE: Files are already validated at this point
    files = data_df[col].to_list()[df_start_idx:df_end_idx]

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
    
    fs = read_audio_metadata(files[0])["fs"]

    # Add group attrs
    ...

    # Add group data
    if vlen is False:
        dataset = data_group.create_dataset(
            name=field_name,
            shape=(len(files), num_samples),
            dtype=np.float32
        )
        dataset.attrs["parser"] = "as_audiofloat32"
        dataset.attrs["sample_rate"] = str(fs)

        filenames_dataset = data_group.create_dataset(
            name=f"{field_name}_filenames",
            shape=(len(files),),
            dtype=h5py.string_dtype()
        )
    
    else:
        dataset = data_group.create_dataset(
            name=field_name,
            shape=(len(files),),
            dtype=h5py.vlen_dtype(np.dtype(np.float32))
        )

    for idx, file in enumerate(
        tqdm(
            files,
            desc=f"Writing partition #{partition_idx}",
            colour=pbar_color,
            leave=False,
            disable=not verbose
        )
    ):
        data, _ = read_audio(file, dtype=np.float32)
        dataset[idx, :] = data
        filenames_dataset[idx] = os.path.basename(file)

    
def as_audiofloat64(data: pl.DataFrame, col: str) -> List[np.ndarray]:
    ...


# def as_float32(file: str, col: str) -> np.ndarray:
#     ...


# def as_float64(file: str, col: str) -> np.ndarray:
#     ...
