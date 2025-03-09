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


def print_error(s: str, writer: Optional[tqdm] = None) -> None:
    """Prints an error message.
    
    Args:
        s (str): Error message to print.
        writer (Optional[tqdm]): Writer to use.
    """
    return printc(f"<error>{s}</error>", writer=writer)


def exit_error(s: str, code: int = 1, writer: Optional[tqdm] = None) -> None:
    """Prints an error message and shuts down the program execution.
    
    Args:
        s (str): Error message to print.
        code (int): Error code to return.
        writer (Optional[tqdm]): Writer to use.
    """
    return printc_exit(f"<error>{s}</error>", code=code, writer=writer)


def print_warning(s: str, writer: Optional[tqdm] = None) -> None:
    """Prints an warning message.
    
    Args:
        s (str): Warning message to print.
        writer (Optional[tqdm]): Writer to use.
    """
    return printc(f"<warning>{s}</warning>", writer=writer)


def exit_warning(s: str, code: int = 1, writer: Optional[tqdm] = None) -> None:
    """Prints a warning message and shuts down the program execution.
    
    Args:
        s (str): Warning message to print.
        code (int): Warning code to return.
        writer (Optional[tqdm]): Writer to use.
    """
    return printc_exit(f"<warning>{s}</warning>", code=code, writer=writer)

def ask_confirmation(
          s: str = "Do you want to continue? [y/n]:",
          exit: bool = True
    ) -> Optional[bool]:
        """Request user input to confirm or reject an instruction.

        Args:
            s (str): Message to be printed to ask user confirmation.
            exit (bool): If ``True`` and user answer is ``n`` (no), then
                the program execution is terminated.

        Returns:
            (Optional[bool]): User response.
        """
        user_input = None

        while str(user_input) not in ["y", "n"]:
            if user_input is not None:
                print_error(f"Invalid input '{user_input}'")

            user_input = input(_decorate_str(s))

            if str(user_input) == "y":
                response = True

            elif str(user_input) == "n":
                if exit:
                    exit_warning("Program finished by the user")

                else:
                    response = False
            else:
                pass

        return response
