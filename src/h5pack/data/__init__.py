from .extractors import (
    from_audioint16,
    from_audiofloat32,
    from_audiofloat64,
    from_float32,
    from_float64,
    from_utf8_str
)

from .parsers import (
    as_audioint16,
    as_audiofloat32,
    as_audiofloat64,
    as_float32,
    as_float64,
    as_utf8_str
)

from .validators import (
    validate_file_as_audioint16,
    validate_file_as_audiofloat32,
    validate_file_as_audiofloat64
)

import polars as pl


def get_parsers_map() -> dict:
    return {
        pl.String: {
            "as_audioint16": as_audioint16,
            "as_audiofloat32": as_audiofloat32,
            "as_audiofloat64": as_audiofloat64,
            "as_utf8_str": as_utf8_str,
        },
        pl.Float32: {
            "as_float32": as_float32,
            "as_float64": as_float64
        },
        pl.Float64: {
            "as_float32": as_float32,
            "as_float64": as_float64
        }
    }


def get_extractors_map() -> dict:
    return {
        "as_audioint16": from_audioint16,
        "as_audiofloat32": from_audiofloat32,
        "as_audiofloat64": from_audiofloat64,
        "as_float32": from_float32,
        "as_float64": from_float64,
        "as_utf8_str": from_utf8_str,
    }


def get_validators_map() -> dict:
    return {
        "as_audioint16": [validate_file_as_audioint16],
        "as_audiofloat32": [validate_file_as_audiofloat32],
        "as_audiofloat64": [validate_file_as_audiofloat64]
    }
