import os
import h5py
from argparse import Namespace
from rich.table import Table
from rich.markup import escape
from ..core.guards import is_file_with_ext
from ..core.utils import format_size
from ..core.display import (
    exit_error,
    get_console,
    get_verbosity,
    print_warning,
    NORMAL
)


def _new_table(title: str) -> Table:
    """Returns an empty table used to print information.

    Args:
        title (str): Table title.

    Returns:
        (Table): `rich` table.
    """
    return Table(
        title=title,
        title_justify="left",
        title_style="bold",
        header_style="bold",
        show_edge=False,
        box=None,
        padding=(0, 2, 0, 2)
    )


def cmd_info(args: Namespace) -> None:
    """Inspects a `.h5` file generated with `h5pack`.

    Args:
        args (Namespace): Input user arguments provided through the console.
    """
    # Check file exists
    if not is_file_with_ext(args.input, ext=".h5"):
        exit_error(f"Invalid input file '{args.input}'")

    console = get_console()
    show = get_verbosity() >= NORMAL

    with h5py.File(args.input, "r") as h5_file:
        # Check if producer is h5pack, otherwise will not be correctly parsed
        if not str(h5_file.attrs.get("producer", "")).startswith("h5pack"):
            exit_error(
                "This file was not created using h5pack, so it may not be "
                "formatted as expected and cannot be reliably parsed"
            )

        if show:
            console.print(
                f"[bold]{escape(args.input)}[/bold] "
                f"[dim]({format_size(os.path.getsize(args.input))})[/dim]\n"
            )

        # File attributes
        if show and len(h5_file.attrs) > 0:
            table = _new_table("File attributes")
            table.add_column("Name", style="cyan")
            table.add_column("Value")

            for k, v in h5_file.attrs.items():
                table.add_row(escape(k), escape(str(v)))

            console.print(table)
            console.print()

        for data_group_name, data_group_data in h5_file.items():
            # Group level attributes
            if show and len(data_group_data.attrs) > 0:
                table = _new_table(f"'{data_group_name}' attributes")
                table.add_column("Name", style="cyan")
                table.add_column("Value")

                for k, v in data_group_data.attrs.items():
                    table.add_row(escape(k), escape(str(v)))

                console.print(table)
                console.print()

            # Dataset level information
            if show:
                table = _new_table(f"'{data_group_name}' fields")
                table.add_column("Field", style="cyan")
                table.add_column("Shape")
                table.add_column("Dtype")
                table.add_column("Attributes", style="dim")

                for dataset_name, dataset_data in data_group_data.items():
                    attrs_repr = "\n".join(
                        f"{k}: {v}" for k, v in dataset_data.attrs.items()
                    )
                    table.add_row(
                        escape(dataset_name),
                        escape(str(dataset_data.shape)),
                        escape(str(dataset_data.dtype)),
                        escape(attrs_repr)
                    )

                console.print(table)
                console.print()

        # If virtual, check paths are accesible and report broken paths
        if h5_file.attrs.get("is_virtual") is not None:
            sources = {}

            for data_group_data in h5_file.values():
                for dataset_data in data_group_data.values():
                    if not dataset_data.is_virtual:
                        continue

                    for source in dataset_data.virtual_sources():
                        sources[source.file_name] = None

            if show:
                table = _new_table("Virtual dataset sources")
                table.add_column("File", style="cyan")
                table.add_column("Status")

            num_missing = 0

            for file in sources:
                # NOTE: Relative sources are resolved from the folder of the
                # virtual file
                file_path = os.path.join(
                    os.path.dirname(os.path.abspath(args.input)),
                    file
                )

                if os.path.isfile(file_path):
                    status = "[green]ok[/green]"

                else:
                    status = "[bold red]not found[/bold red]"
                    num_missing += 1

                if show:
                    table.add_row(escape(file), status)

            if show:
                console.print(table)
                console.print()

            if num_missing > 0:
                print_warning(
                    f"{num_missing} source file(s) of the virtual dataset "
                    "not found"
                )
