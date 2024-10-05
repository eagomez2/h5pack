import os
import argparse
import numpy as np
from typing import List
from ..core.display import exit_error
from ..core.guards import is_file_with_ext
from ..core.io import (
    add_extension,
    add_suffix,
    get_dir_files
)
from ..core.utils import (
    list_from_csv_col,
    list_from_tsv_col,
    make_list
)


def create_partition_specs(files: List[str], args: argparse.Namespace) -> dict:
    if args.partitions > len(files):
        exit_error(
            "The number of partitions should be greater than the number of "
            f"files. Found {len(files)} file(s) for {args.partitions}"
        )
    
    # Split files
    partitions = np.array_split(files, indices_or_sections=args.partitions)

    # Get .h5 partition filenames
    if len(partitions) == 1:
        filenames = make_list(add_extension(args.output, ext=".h5"))

    else:
        filenames = []
        zfill = len(str(len(partitions)))

        for idx in range(len(partitions)):
            filename = add_extension(args.output, ext=".h5")
            filename = add_suffix(
                filename,
                suffix=f".pt{str(idx).zfill(zfill)}"
            )
            filenames.append(filename)
    
    # Assemble partition specs
    specs = []
    audio_info = aml.read_audio_info(files[0])


def create_partitions(args: argparse.Namespace) -> None:
    # Check input dir or file exists
    if is_file_with_ext(args.input, ext=".csv"):
        files = list_from_csv_col(args.input, col=args.audio_col)
    
    elif is_file_with_ext(args.input, ext=".tsv"):
        files = list_from_tsv_col(args.input, col=args.audio_col)

    elif os.path.isdir(args.input):
        files = get_dir_files(
            args.input,
            ext=args.ext,
            recursive=args.recursive
        )
    
    else:
        exit_error(f"Invalid input file or folder '{args.input}'")

    if len(files) == 0:
        if os.path.isdir(args.input):
            exit_error(
                f"0 files found in '{args.input}'. Use --recursive if you "
                "intended to perform a recursive search"
            )
        
        else:
            exit_error(
                f"0 files found in column '{args.audio_col}' of '{args.input}'"
            )
    
    # Verify audio files
    if not args.skip_verification:
        ...

    partition_specs = ...
