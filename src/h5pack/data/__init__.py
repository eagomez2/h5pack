__all__ = [
    "as_audiofloat32",
    "as_audiofloat64",
    "as_float32",
    "as_float64",
]


from .parsers import (
    as_audiofloat32,
    as_audiofloat64,
    as_float32
)

from .validators import (
    validate_file_as_audiofloat32,
    validate_file_as_audiofloat64
)

import polars as pl


def get_parsers_map() -> dict:
    return {
        pl.String: {
            "as_audiofloat32": as_audiofloat32,
            "as_audiofloat64": as_audiofloat64
        },
        pl.Float32: {
            "as_float32": as_float32
        },
        pl.Float64: {
            "as_float32": as_float32
        }
    }

def get_validators_map() -> dict:
    return {
        "as_audiofloat32": [validate_file_as_audiofloat32],
        "as_audiofloat64": [validate_file_as_audiofloat64]
    }
