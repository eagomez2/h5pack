import polars as pl
from .extractors import (
    from_audioint16,
    from_audiofloat32,
    from_audiofloat64,
    from_int16,
    from_float32,
    from_float64,
    from_utf8_str
)
from .parsers import (
    as_audioint16,
    as_audiofloat32,
    as_audiofloat64,
    as_int16,
    as_float32,
    as_float64,
    as_utf8_str
)
from .validators import (
    validate_file_as_audioint16,
    validate_file_as_audiofloat32,
    validate_file_as_audiofloat64
)


def get_parsers_map() -> dict:
    """Mapping between parsers defined by the user and parser functions based
    on `polars` data types used to read each column in the input `.csv` file.

    Returns:
        dict: Mapping `str` values and the corresponding parsers.
    """
    return {
        pl.String: {
            "as_audioint16": as_audioint16,
            "as_audiofloat32": as_audiofloat32,
            "as_audiofloat64": as_audiofloat64,
            "as_utf8_str": as_utf8_str
        },
        pl.Int16: {
            "as_int16": as_int16
        },
        pl.Int32: {
            "as_int16": as_int16
        },
        pl.Int64: {
            "as_int16": as_int16
        },
        pl.Int128: {
            "as_int16": as_int16
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
    """Mapping between extractor identifiers and extractor methods.
    
    Returns:
        dict: Mapping between extractor identifiers and extractor methods.
    """
    return {
        "as_audioint16": from_audioint16,
        "as_audiofloat32": from_audiofloat32,
        "as_audiofloat64": from_audiofloat64,
        "as_int16": from_int16,
        "as_float32": from_float32,
        "as_float64": from_float64,
        "as_utf8_str": from_utf8_str
    }


def get_validators_map() -> dict:
    """Maping between validator identifiers and validator methods.
    
    Returns:
        dict: Mapping between validator identifiers and validator methods.
            Each method can be a `list` of one or more methods that are applied
            in series.
    """
    return {
        "as_audioint16": [validate_file_as_audioint16],
        "as_audiofloat32": [validate_file_as_audiofloat32],
        "as_audiofloat64": [validate_file_as_audiofloat64]
    }
