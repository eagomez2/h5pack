import sys
from tqdm import tqdm
from typing import Optional
from .config import (
    _get_text_color_tags,
    _get_text_decorator_tags
)


def _decorate_str(s: str) -> str:
    """Replace colors and decorators in a string.

    Args:
        s (str): The input string to be decorated.

    Returns:
        str: The decorated string.
    """
    # Replace colors and decorators
    for k, v in _get_text_decorator_tags().items():
        s = s.replace(k, v)

    for k, v in _get_text_color_tags().items():
        s = s.replace(k, v)

    return s


def printc(s: str, writer: Optional[tqdm] = None) -> None:
    """Prints a formatted string.

    Args:
        s (str): The string to print.
        writer (Optional[tqdm]): Writer to use.
    """
    return (
        print(_decorate_str(s)) if writer is None
        else tqdm.write(_decorate_str(s))
    )


def printc_exit(s: str, code: int = 1, writer: Optional[tqdm] = None) -> None:
    """Prints a formatted string and exits the program with a specified exit
    code.

    Args:
        s (str): The string to print.
        code (int): Exit code.
        writer (Optional[tqdm]): Writer to use.
    """
    printc(s=s, writer=writer)
    sys.exit(code)


def print_warning(s: str, writer: Optional[tqdm] = None) -> None:
    return printc(f"<warning>{s}</warning>", writer=writer)


def exit_error(s: str, code: int = 1, writer: Optional[tqdm] = None) -> None:
    return printc(f"<error>{s}</error>", writer=writer)
