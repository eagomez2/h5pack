import os
import math
import fnmatch
import polars as pl
from argparse import Namespace
from datetime import datetime
from threading import Thread
from time import perf_counter
from multiprocessing import (
    Pool,
    Manager
)
from h5pack import __version__
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn
)
from queue import Empty
from ..core.io import (
    add_extension,
    add_suffix,
    change_extension,
    get_dir_files
)
from ..core.guards import is_file_with_ext
from ..core.display import (
    ask_confirmation,
    exit_error,
    exit_warning,
    print_warning
)
from ..core.utils import (
    dict_from_interleaved_list,
    get_file_checksum,
    time_to_str,
    total_to_list_slices,
)
from ..data.validators import validate_config_file
from ..data import (
    get_validators_map
)
from .utils import (
    create_partition_from_data_,
    create_virtual_dataset_from_partitions
)


def cmd_virtual(args: Namespace) -> None:
    """Creates a virtual dataset that can accumulate multiple `.h5` files in
    a single view.

    Args:
        args (Namespace): User input arguments provided through the console.
    """
    # All input file candidates
    h5_files = []
    
    # Process user provided attributes
    root_attrs = None

    if args.attrs is not None:
        if len(args.attrs) % 2 != 0:
            exit_error(
                "--attrs should be an even number of items where each odd item"
                " represents a key and each even item represents its value"
            )
    
        root_attrs = dict_from_interleaved_list(args.attrs)

    print("Collecting input files ...")
    
    for file_or_dir in args.input:
        if is_file_with_ext(file_or_dir, ext=".h5"):
            h5_files.append(file_or_dir)
        
        elif os.path.isdir(file_or_dir):
            h5_files += get_dir_files(
                dir=file_or_dir,
                ext=".h5",
                recursive=args.recursive
            )

    if len(h5_files) == 0:
        exit_warning(
            "0 .h5 files found. Use --recursive if you intended to perform a "
            "recursive search"
        )
    
    else:
        print(f"{len(h5_files)} .h5 file(s) found")

    # Apply select/filter patterns
    if args.select is not None:
        print(f"Applying --select pattern '{args.select}' ...")
        
        selected_files = []

        for f in h5_files:
            if fnmatch.fnmatch(f, args.select):
                selected_files.append(f)
        
        h5_files = [f for f in h5_files if f in selected_files]
        print(f"{len(h5_files)} selected .h5 file(s) after applying --select")
    
    if args.filter is not None:
        print(f"Applying --filter pattern '{args.filter}' ...")

        filtered_files = []

        for f in h5_files:
            if fnmatch.fnmatch(f, args.filter):
                filtered_files.append(f)
        
        h5_files = [f for f in h5_files if f not in filtered_files]

        print(f"{len(h5_files)} selected .h5 file(s) after applying --filter")

    partition_files_repr = "\n".join(
        [
            f"  {idx}. '{f}'" for idx, f in enumerate(h5_files, start=1)
        ]
    )
    print(
        "A virtual dataset will be created for the following file(s):\n"
        f"{partition_files_repr}"
    )

    if not args.unattended:
        ask_confirmation()
    
    # Create virtual dataset
    output_file = add_extension(args.output, ext=".h5")
    create_virtual_dataset_from_partitions(
        file=add_extension(args.output, ext=".h5"),
        partitions=h5_files,
        attrs=root_attrs
    )
    print(f"Virtual dataset saved to '{os.path.basename(output_file)}'")
