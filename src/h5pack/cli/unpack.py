import os
import yaml
import h5py
from packaging import version
from argparse import Namespace
from time import perf_counter
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn
)
from ..core.guards import is_file_with_ext
from ..core.display import exit_error
from ..core.utils import time_to_str
from ..data import get_extractors_map


def cmd_unpack(args: Namespace) -> None:
    """Extracts raw data from a `.h5` file.

    Args:
        args (Namespace): User input arguments provided through the console.
    """
    # Check if file exists
    if not is_file_with_ext(args.input, ext=".h5"):
        exit_error(f"Invalid input fille '{args.input}'")
    
    # Automatically get path if not provided
    if not args.output:
        args.output = os.path.splitext(args.input)[0]
    
    with h5py.File(args.input, mode="r") as h5_file:
        # Check if producer is h5pack, otherwise will not be correctly parsed
        if not h5_file.attrs.get("producer", "").startswith("h5pack"):
            exit_error(
                "This file was not created using h5pack, so it may not be "
                "formatted as expected and cannot be reliably parsed"
            )
    
    # Generate output folder
    if not os.path.isdir(args.output):
        print(f"Creating output folder '{args.output}' ...")
        os.makedirs(args.output, exist_ok=True)
    
    start_time = perf_counter()

    # Extract h5pack.yaml
    dataset_name = os.path.basename(args.output)
    h5pack_yaml = {
        "datasets": {
            dataset_name: {
                "attrs": {},
                "data": {"file": "dataset.csv", "fields": {}}  # Fixed name
            }
        }
    }

    with h5py.File(args.input, mode="r") as h5_file:
        # Extract attributes
        print("Extracting file attribute(s) ...")
        
        h5pack_yaml["datasets"][dataset_name]["attrs"].update(
            {
                k: v for k, v in h5_file.attrs.items()
                if k not in ("producer", "creation_date")  # Reserved names
            }
        )

        # Extract data
        os.makedirs(os.path.join(args.output, "data"), exist_ok=True)

        # Creat empty dataset.csv
        dataset = os.path.join(args.output, "dataset.csv")
        open(dataset, "w").close()

        progress_bar = Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("{task.completed}/{task.total}"),
            TimeRemainingColumn(),
            transient=True,
            # disable=True
        )
        ctx = {
            "producer_version": version.parse(
                h5_file.attrs["producer"].replace("h5pack ", "")
            ),
            "progress_bar": progress_bar
        }

        # Fill out data
        for field_name in h5_file["data"]:
            parser = h5_file["data"][field_name].attrs.get("parser")

            if parser is None:
                continue

            else:
                print(f"Unpacking 'data/{field_name}' ({parser}) ...")
        
                extractor = get_extractors_map()[parser]
                output_dir = os.path.join(args.output, "data", field_name)
                extractor(
                    output_csv=dataset,
                    output_yaml=h5pack_yaml,
                    output_dir=output_dir,
                    dataset_name=dataset_name,
                    field_name=field_name,
                    data=h5_file["data"],
                    attrs=h5_file["data"][field_name].attrs,
                    ctx=ctx
                )

                print(f"Field 'data/{field_name}' successfully unpacked")
        
        with open(os.path.join(args.output, "h5pack.yaml"), "w") as f:
            yaml.dump(h5pack_yaml, f, sort_keys=False, allow_unicode=True)
    
    end_time = perf_counter()
    elapsed_time_repr = time_to_str(end_time - start_time)
    print(f"Unpacking process completed in {elapsed_time_repr}")
