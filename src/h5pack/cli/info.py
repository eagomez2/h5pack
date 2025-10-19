import os
import h5py
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
    exit_warning
)
from ..core.utils import dict_from_interleaved_list
from .utils import create_virtual_dataset_from_partitions


def cmd_info(args: Namespace) -> None:
    """Inspects a `.h5` file generated with `h5pack`.
    
    Args:
        args (Namespace): Input user arguments provided through the console.
    """
    # Check file exists
    if not is_file_with_ext(args.input, ext=".h5"):
        exit_error(f"Invalid input file '{args.input}'")

    # Print info
    print(f"Input file: '{args.input}'" )

    with h5py.File(args.input, "r") as h5_file:
        # Check if producer is h5pack, otherwise will not be correctly parsed
        if not h5_file.attrs.get("producer", "").startswith("h5pack"):
            exit_error(
                "This file was not created using h5pack, so it may not be "
                "formatted as expected and cannot be reliably parsed"
            )
        
        if len(h5_file.attrs) > 0:
            print("File attribute(s):")
            key_ljust = max([len(k) for k in h5_file.attrs]) + 6

            for k, v in h5_file.attrs.items():
                print(f"  - {k}:".ljust(key_ljust) + f"{v}")
        
        # Get data group level attributes
        for data_group_name, data_group_data in h5_file.items():
            print(f"Data group '{data_group_name}':")

            if len(data_group_data.attrs) > 0:
                key_ljust = max([len(k) for k in data_group_data.attrs]) + 6
                
                print(f"'{data_group_name}' attribute(s):")

                for k, v in data_group_data.items():
                    print(f"  - {k}:".ljust(key_ljust) + f"{v}") 

            # Get dataset level info
            for dataset_name, dataset_data in data_group_data.items():
                if len(dataset_data.attrs) > 0:
                    key_ljust = max([len(k) for k in dataset_data.attrs]) + 8
                    print(f"  - '{dataset_name}' attribute(s):")
                
                for k, v in dataset_data.attrs.items():
                    print(f"    - {k}:".ljust(key_ljust) + f"{v}") 

                print(f"  - '{dataset_name}' data attribute(s):")
                print(f"    - shape: {dataset_data.shape}")
                print(f"    - dtype: {dataset_data.dtype}")
