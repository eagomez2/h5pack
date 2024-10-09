import polars as pl
import numpy as np
from typing import List


def as_audiofloat32(df: pl.DataFrame, col: str) -> dict:
    # Validate column
    all_files = df[col].to_list()

    # Check all files are mono .wav and have same sample rate
    ...

    import pdb;pdb.set_trace()
    ...

    return {
        "data": ...,
        "attrs": {
            "fs"       
        }
    }


def as_audiofloat64(data: pl.DataFrame, col: str) -> List[np.ndarray]:
    ...


# def as_float32(file: str, col: str) -> np.ndarray:
#     ...


# def as_float64(file: str, col: str) -> np.ndarray:
#     ...
