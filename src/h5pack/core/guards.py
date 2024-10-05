import os
from typing import (
    List,
    Union
)
from .exceptions import FileExtensionError


def is_file_or_error(file: str) -> None:
    """Raises and exception if `file` is not a file.

    Args:
        file (str): File path to check.

    Raises:
        FileNotFoundError: If `file` is not a valid file.
    """
    if not os.path.isfile(file):
        raise FileNotFoundError(f"File not found: {file}")


def has_ext(file: str, ext: Union[str, List[str]]) -> bool:
    """Returns `True` if a file has a certain extension.
    
    Args:
        file (str): File to check.
        ext (Union[str, List[str]]): Single extension to check as `str` or
            `list` of extensions to check.
    
    Returns:
        bool: `True` if `file` has one of the specified extensions, `False`
            otherwise.
    """
    ext = [ext] if not isinstance(ext, list) and ext is not None else ext
    _, ext_ = os.path.splitext(file)
    return ext_ in ext


def has_ext_or_error(file: str, ext: Union[str, List[str]]) -> None:
    """Raises an exception if a file does not have an extension among a set
    of specified extensions.
    
    Args:
        file (str): File to check.
        ext (Union[str, List[str]]): Single extension to check as `str` or
            `list` of extensions to check.
    
    Raises:
        FileExtensionError: If `file` does not have any of the specified
            extensions.
    """
    if not has_ext(file, ext=ext):
        ext_repr = ", ".join([f"'{e}'" for e in ext])

        raise FileExtensionError(
            f"Invalid file extension of '{file}'. Expected file extensions: "
            f"{ext_repr}"
        )


def is_file_with_ext(file: str, ext: Union[str, List[str]]) -> bool:
    """Returns `True` if `file` exists and has one of the specified
    extensions.
    
    Args:
        file (str): File to check.
        ext (Union[str, List[str]]): Single extension to check as `str` or
             `list` of extensions to check.
    
    Returns:
        bool: `True` if `file` exists and has one of the specified extensions,
            `False` otherwise.
    """
    return os.path.isfile(file) and has_ext(file, ext)


def is_file_with_ext_or_error(file: str, ext: Union[str, List[str]]) -> bool:
    """Raises an exception if `file` does not exists or does not have one of
    the specified extensions.
    
    Args:
        file (str): File to check.
        ext (Union[str, List[str]]): Single extension to check as `str` or
            `list` of extensions to check.
    """
    is_file_or_error(file)
    has_ext_or_error(file, ext=ext)
