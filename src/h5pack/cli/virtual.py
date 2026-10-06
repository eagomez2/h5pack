import os
import fnmatch
from argparse import Namespace
from ..core.io import (
    add_extension,
    get_dir_files
)
from ..core.guards import is_file_with_ext
from ..core.display import (
    ask_confirmation,
    exit_error,
    exit_warning,
    print_debug,
    print_info,
    print_output,
    print_step
)
from ..core.utils import dict_from_interleaved_list
from .utils import create_virtual_dataset_from_partitions


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
        print_step("Found", f"{len(h5_files)} .h5 file(s)")

    # Apply select/filter patterns
    if args.select is not None:
        print_debug(f"Applying --select pattern '{args.select}'")
        
        selected_files = []

        for f in h5_files:
            if fnmatch.fnmatch(f, args.select):
                selected_files.append(f)
        
        h5_files = [f for f in h5_files if f in selected_files]

        if len(h5_files) == 0:
            exit_warning(
                f"{len(h5_files)} selected .h5 file(s) after applying --select"
            )
        
        else:
            print_step(
                "Selected",
                f"{len(h5_files)} .h5 file(s)",
                details=f"--select '{args.select}'"
            )
    
    if args.filter is not None:
        print_debug(f"Applying --filter pattern '{args.filter}'")

        filtered_files = []

        for f in h5_files:
            if fnmatch.fnmatch(f, args.filter):
                filtered_files.append(f)
        
        h5_files = [f for f in h5_files if f not in filtered_files]

        if len(h5_files) == 0:
            exit_warning(
                f"{len(h5_files)} selected .h5 file(s) after applying --filter"
            )
        
        else:
            print_step(
                "Selected",
                f"{len(h5_files)} .h5 file(s)",
                details=f"--filter '{args.filter}'"
            )

    partition_files_repr = "\n".join(
        [
            f"  {idx}. '{f}'" for idx, f in enumerate(h5_files, start=1)
        ]
    )

    print_info(
        "A virtual dataset will be created for the following file(s):\n"
        f"{partition_files_repr}"
    )

    if not args.unattended:
        ask_confirmation()
    
    # Create virtual dataset
    output_file = add_extension(args.output, ext=".h5")
    create_virtual_dataset_from_partitions(
        file=output_file,
        partitions=h5_files,
        attrs=root_attrs,
        force_abspath=args.force_abspath
    )
    print_step(
        "Created",
        "virtual dataset",
        details=f"{len(h5_files)} source file(s)"
    )
    print_output(output_file)
