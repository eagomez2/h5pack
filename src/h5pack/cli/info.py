import os
import h5py
from argparse import Namespace
from ..core.guards import is_file_with_ext
from ..core.display import (
    exit_error,
    print_warning
)


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

        # If virtual, check paths are accesible and report broken paths
        if h5_file.attrs.get("is_virtual") is not None:
            print("Virtual dataset sources:")

            for data_group_name, data_group_data in h5_file.items():
                for dataset_name, dataset_data in data_group_data.items():
                    plist = dataset_data.id.get_create_plist()

                    print(f"  - '{dataset_name}' source(s):")

                    for file_idx in range(plist.get_virtual_count()):
                        file = plist.get_virtual_filename(file_idx)

                        if not os.path.isfile(file):
                            print_warning(
                                f"    - {file_idx}: {file} [NOT FOUND]"
                            )
                        
                        else:
                            print(f"    - {file_idx}: {file} [OK]")
