import os
from time import perf_counter
from argparse import Namespace
from ..core.io import (
    add_extension,
    get_dir_files
)
from ..core.guards import is_file_with_ext
from ..core.display import (
    exit_error,
    exit_warning,
    print_warning,
    print_error,
)
from ..core.utils import (
    get_file_checksum,
    time_to_str
)


def cmd_checksum(args: Namespace) -> None:
    """Calculates or verifies checksums associated with `.h5` files.

    Args:
        args (Namespace): User input arguments provided through the console.
    """
    # Check input type
    if is_file_with_ext(file=args.input, ext=".sha256"):  # Verify
        if args.save:
            print_warning("--save ignored for .sha256 input")     
        
         # Read lines and check they contain only two elements
        root_dir = os.path.dirname(args.input)
        print(f"Using root folder '{root_dir}'")
        print(f"Verifying checksum in '{args.input}' ...")

        start_time = perf_counter()

        with open(args.input, "r") as f:
            for line_idx, line in enumerate(f):
                h5_filename, saved_checksum = line.split("\t")
                h5_file = os.path.join(root_dir, h5_filename)
                saved_checksum = saved_checksum.rstrip("\n")

                if not is_file_with_ext(h5_file, ext=".h5"):
                    exit_error(
                        f"Invalid file '{h5_file}' reference in '{args.input}'"
                        f" (line {line_idx + 1})"
                    )
                
                checksum = get_file_checksum(h5_file, hash="sha256")

                if saved_checksum == checksum:
                    print(
                        f"{h5_filename}\t{saved_checksum} [OK]"
                    )
                
                else:
                    print_error(
                        f"{h5_filename} [MISMATCH]"
                        f"\n  - Saved:      {saved_checksum}"
                        f"\n  - Calculated: {checksum}"
                    )
                
        end_time = perf_counter()
        elapsed_time_repr = time_to_str(end_time - start_time, abbrev=False)
        print(f"Checksum verification completed in {elapsed_time_repr}")
    
    else:  # Calculate
        if is_file_with_ext(args.input, ext=".h5"):
            all_files = [args.input]
        
        elif os.path.isdir(args.input):
            all_files = get_dir_files(
                args.input,
                ext=".h5",
                recursive=args.recursive
            )
        
        else:
            exit_error(
                "Input must be an .h5 file or a folder containing .h5 files"
            )
        
        if len(all_files) == 0:
            if not args.recursive:
                exit_warning(
                    f"0 .h5 files found in in '{args.input}'. Use "
                    "--recursive/-r if you intended to perform a recursive "
                    "search"
                )
            
            else:
                exit_warning(f"0 .h5 files found in '{args.input}'")

        print(f"Calculating checksum for .h5 files in '{args.input}' ...")

        if args.save:
            checksum_file = add_extension(args.save, ext=".sha256")

            with open(add_extension(args.save, ext=".sha256"), "w") as f:
                start_time = perf_counter()

                for file in all_files:
                    checksum = get_file_checksum(file, hash="sha256")
                    checksum_repr = f"{os.path.basename(file)}\t{checksum}"
                    f.write(f"{checksum_repr}\n")
                    print(checksum_repr)
                
                end_time = perf_counter()
                elapsed_time_repr = time_to_str(end_time - start_time)
                print(
                    f"Checksum calculation completed in {elapsed_time_repr} "
                    f"and saved to '{checksum_file}'"
                )
        
        else:
            start_time = perf_counter()

            for file in all_files:
                checksum = get_file_checksum(file, hash="sha256")
                print(f"{os.path.basename(file)}\t{checksum}")
             
            end_time = perf_counter()
            elapsed_time_repr = time_to_str(end_time - start_time)
            print(f"Checksum calculation completed in {elapsed_time_repr}")
