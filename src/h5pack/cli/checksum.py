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
    print_debug,
    print_error,
    print_info,
    print_output,
    print_step,
    print_warning
)
from ..core.utils import get_file_checksum


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
        print_debug(f"Using root folder '{os.path.abspath(root_dir)}'")

        start_time = perf_counter()
        num_mismatches = 0
        num_files = 0

        with open(args.input) as f:
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
                num_files += 1

                if saved_checksum == checksum:
                    print_info(f"{h5_filename}\t{saved_checksum} [OK]")
                
                else:
                    num_mismatches += 1
                    print_error(
                        f"{h5_filename} does not match its checksum",
                        cause=(
                            f"saved {saved_checksum}, calculated {checksum}"
                        )
                    )
                
        if num_mismatches > 0:
            exit_error(
                f"{num_mismatches} file(s) failed checksum verification"
            )

        print_step(
            "Verified",
            f"{num_files} file(s) in '{args.input}'",
            elapsed=perf_counter() - start_time
        )
    
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


        if args.save:
            checksum_file = add_extension(args.save, ext=".sha256")

            with open(add_extension(args.save, ext=".sha256"), "w") as f:
                start_time = perf_counter()

                for file in all_files:
                    checksum = get_file_checksum(file, hash="sha256")
                    checksum_repr = f"{os.path.basename(file)}\t{checksum}"
                    f.write(f"{checksum_repr}\n")
                    print(checksum_repr)

            print_step(
                "Hashed",
                f"{len(all_files)} file(s)",
                elapsed=perf_counter() - start_time
            )
            print_output(checksum_file)
        
        else:
            start_time = perf_counter()

            for file in all_files:
                checksum = get_file_checksum(file, hash="sha256")
                print(f"{os.path.basename(file)}\t{checksum}")
             
            print_step(
                "Hashed",
                f"{len(all_files)} file(s)",
                elapsed=perf_counter() - start_time
            )
