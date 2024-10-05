import polars as pl
from itertools import chain
from typing import (
    Any,
    List
)
from .guards import is_file_with_ext_or_error


def make_list(x: Any) -> List[Any]:
    """If `x` is a single element, turns it into a `list` of one element.

    Args:
        x (Any): Element(s) to be returned as a `list`.

    Returns:
        (list): `x` as a `list`.
    """
    return [x] if not isinstance(x, list) and x is not None else x


def flatten_nested_list(nl: List[List]) -> List[Any]:
    """Flattens a list of lists of arbitraty depth.

    Args:
        nl (List[List]): Nested `list` of arbitrary depth.

    Return:
        (list): Flattened `list`.
    """
    return (
        list(chain.from_iterable(map(flatten_nested_list, nl)))
        if isinstance(nl, list)
        else [nl]
    )


def list_from_csv_col(file: str, col: str) -> List[str]:
    """Creates a ``list`` of ``str`` from a given column of a ``.csv`` file.
    
    Args:
        file (str): Input ``.csv`` file.
        col (str): Column name.
    
    Returns:
        List[str]: List containing values found in column ``col``.
    """
    is_file_with_ext_or_error(file, ext=".csv")
    df = pl.read_csv(file)
    col_data = df[col].to_list()
    return col_data


def list_from_tsv_col(file: str, col: str) -> List[str]:
    """Creates a ``list`` of ``str`` from a given column of a ``.tsv`` file.
    
    Args:
        file (str): Input ``.tsv`` file.
        col (str): Column name.
    
    Returns:
        List[str]: List containing values found in column ``col``.
    """
    is_file_with_ext_or_error(file, ext=".tsv")
    df = pl.read_csv(file, separator="\t")
    col_data = df[col].to_list()
    return col_data


def time_to_str(time: float, abbrev: bool = False) -> str:
    """Returns a time in seconds in a human readable format.
    
    Args:
        time (float): Time in seconds.
        abbrev (bool): If `True`, abbreviations to represent different time
            units are used.
    
    Returns:
        (str): `str` representation of `time`.
    """
    if abbrev:
        ms_repr = "ms"
        s_repr = "s"
        m_repr = "m"
        h_repr = "h"
    
    else:
        ms_repr = "millisecond(s)"
        s_repr = "second(s)"
        m_repr = "minute(s)"
        h_repr = "hour(s)"

    if time < 1.0:
        time_repr = f"{time * 1e3:.1f} {ms_repr}"

    elif time < 60.0:
        time_repr = f"{time:.1f} {s_repr}"
    
    elif time < 3600.0:
        time_mins = time // 60.0
        time_secs = time % 60
        time_repr = f"{int(time_mins)} {m_repr} {int(time_secs)} {s_repr}" 

    else:            
        time_hours = time // 3600.0
        remaining_time = time % 3600.0
        time_mins = remaining_time // 60.0
        time_secs = remaining_time % 60
        time_repr = (
            f"{int(time_hours)} {h_repr} {int(time_mins)} {m_repr} "
            f"{int(time_secs)} {s_repr}"
        )

    return time_repr


def bytes_to_str(bytes: int) -> str:
    """Returns an amount of bytes in a human readable format.
    
    Args:
        bytes (int): Number of bytes to represent.

    Returns:
        (str): `str` representation of `bytes`.
    """
    if bytes / (1024 ** 4) > 1.0:
        repr = f"{bytes / 1024 ** 4:.1f}T"

    elif bytes / (1024 ** 3) > 1.0:
        repr = f"{bytes / 1024 ** 3:.1f}G"
        
    elif bytes / (1024 ** 2) > 1.0:
        repr = f"{bytes / 1024 ** 2:.1f}M"
        
    elif bytes / 1024 > 1.0:
        repr = f"{bytes / 1024:.1f}K"
        
    else:
        repr = f"{bytes:d}B"
    
    return repr
