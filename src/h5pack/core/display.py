import os
import sys
from rich.console import Console
from rich.markup import escape
from .utils import (
    format_size,
    time_to_str
)


# NOTE: rich disables colors automatically when NO_COLOR is set or when the
# output is not a terminal
_console = Console(highlight=False)
_err_console = Console(highlight=False, stderr=True)

# Verbosity levels
QUIET = 0
NORMAL = 1
VERBOSE = 2

_verbosity = {"level": NORMAL}


def set_verbosity(level: int) -> None:
    """Sets the verbosity level used by all printing functions.

    Args:
        level (int): `QUIET` (0), `NORMAL` (1) or `VERBOSE` (2).
    """
    _verbosity["level"] = level


def get_verbosity() -> int:
    """Returns the current verbosity level.

    Returns:
        (int): Current verbosity level.
    """
    return _verbosity["level"]


def get_console() -> Console:
    """Returns the console used to print messages, so progress bars can
    share it.

    Returns:
        (Console): `rich` console.
    """
    return _console


def is_progress_enabled() -> bool:
    """Returns `True` if progress bars should be shown. Progress bars are
    hidden in quiet mode and when the output is not a terminal (e.g. when it
    is redirected to a log file).

    Returns:
        (bool): `True` if progress bars should be shown.
    """
    return get_verbosity() >= NORMAL and _console.is_terminal


def print_step(
        verb: str,
        s: str,
        details: str | None = None,
        elapsed: float | None = None
) -> None:
    """Prints a step of a command in the style `Verb message (details) in
    1.2s`.

    Args:
        verb (str): Past or present tense verb describing the step (e.g.
            `Validated` or `Packing`).
        s (str): Message.
        details (str | None): Optional details shown dimmed in parentheses.
        elapsed (float | None): Optional elapsed time in seconds.
    """
    if get_verbosity() < NORMAL:
        return

    line = f"[bold green]{escape(verb):>10}[/bold green] {escape(s)}"

    if details is not None:
        line += f" [dim]({escape(details)})[/dim]"

    if elapsed is not None:
        line += f" in {time_to_str(elapsed, abbrev=True).replace(' ', '')}"

    _console.print(line)


def print_output(file: str, details: str | None = None) -> None:
    """Prints a file created by a command in the style ` + file details`.

    Args:
        file (str): Created file.
        details (str | None): Optional details shown dimmed. If `file`
            exists and no details are given, its size is shown.
    """
    if get_verbosity() < NORMAL:
        return

    if details is None:
        try:
            details = format_size(os.path.getsize(file))

        except OSError:
            details = ""

    _console.print(
        f" [bold green]+[/bold green] {escape(file)} [dim]{escape(details)}"
        "[/dim]"
    )


def print_info(s: str) -> None:
    """Prints a regular message.

    Args:
        s (str): Message to print.
    """
    if get_verbosity() >= NORMAL:
        _console.print(escape(s))


def print_debug(s: str) -> None:
    """Prints a message only in verbose mode (`-v/--verbose`).

    Args:
        s (str): Message to print.
    """
    if get_verbosity() >= VERBOSE:
        _console.print(f"[dim]{escape(s)}[/dim]")


def print_error(s: str, cause: str | None = None) -> None:
    """Prints an error message.

    Args:
        s (str): Error message to print.
        cause (str | None): Optional cause of the error.
    """
    _err_console.print(f"[bold red]error:[/bold red] {escape(s)}")

    if cause is not None:
        _err_console.print(f"  [bold]Caused by:[/bold] {escape(cause)}")


def print_warning(s: str) -> None:
    """Prints a warning message.

    Args:
        s (str): Warning message to print.
    """
    if get_verbosity() >= NORMAL:
        _err_console.print(f"[bold yellow]warning:[/bold yellow] {escape(s)}")


def print_hint(s: str) -> None:
    """Prints a hint that tells the user how to solve a problem.

    Args:
        s (str): Hint to print.
    """
    _err_console.print(f"[bold cyan]hint:[/bold cyan] {escape(s)}")


def exit_error(
        s: str,
        code: int = 1,
        cause: str | None = None,
        hint: str | None = None
) -> None:
    """Prints an error message and shuts down the program execution.

    Args:
        s (str): Error message to print.
        code (int): Error code to return.
        cause (str | None): Optional cause of the error.
        hint (str | None): Optional hint to solve the error.
    """
    print_error(s, cause=cause)

    if hint is not None:
        print_hint(hint)

    sys.exit(code)


def exit_warning(s: str, code: int = 1) -> None:
    """Prints a warning message and shuts down the program execution.

    Args:
        s (str): Warning message to print.
        code (int): Warning code to return.
    """
    _err_console.print(f"[bold yellow]warning:[/bold yellow] {escape(s)}")
    sys.exit(code)


def ask_confirmation(
        s: str = "Do you want to continue? [y/n]:",
        exit: bool = True
) -> bool | None:
        """Request user input to confirm or reject an instruction.

        Args:
            s (str): Message to be printed to ask user confirmation.
            exit (bool): If ``True`` and user answer is ``n`` (no), then
                the program execution is terminated.

        Returns:
            (bool | None): User response.
        """
        user_input = None

        while str(user_input) not in ["y", "n"]:
            if user_input is not None:
                print_error(f"Invalid input '{user_input}'")

            user_input = _console.input(f"{escape(s)} ")

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
