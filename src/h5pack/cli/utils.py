import os
import h5py
import polars as pl
from typing import (
    List,
    Optional,
    Tuple
)
from datetime import datetime
from argparse import Namespace
from importlib.metadata import version
from ..core.guards import is_file_with_ext
from ..core.display import exit_error
from ..core.utils import stack_shape
from ..core.io import (
    add_extension,
    add_suffix
)
from ..data import get_parsers_map


def create_partition_from_data(
        idx: int,
        specs: dict,
        data: pl.DataFrame,
        args: Namespace,
        ctx: dict = {}
) -> Tuple[int, str]:
    """Creates a single partition file from a given `DataFrame` and
    accompanying set of specifications.

    Args:
        idx (int): Partition index.
        specs (dict): Set of specifications used to process the data.
        data (pl.DataFrame): Input `DataFrame` containing the raw data.
        args (Namespace): User provided arguments.
        ctx (dict): Context information.
    
    Returns:
        (Tuple[int, str]): Partition index and generated `.h5` filename.
    """
    # Create file
    h5_filename = add_extension(args.output, ext=".h5")

    if ctx["num_partitions"] != 1:
        h5_filename = add_suffix(
            h5_filename,
            f".pt{str(idx).zfill(len(str(ctx['num_partitions'])))}"
        )
    
    # Create output folder if it doesn't exist
    if os.path.dirname(args.output) != "":
        os.makedirs(os.path.dirname(args.output), exist_ok=True)

    h5_file = h5py.File(h5_filename, "w")

    # Add root attrs
    for k, v in specs["attrs"].items():
        h5_file.attrs[k] = v
    
    # Add data group
    data_group = h5_file.create_group("data")

    # Add data
    for field_name, field_data in specs["fields"].items():
        # Get parser
        parser = (
            get_parsers_map()[data[field_data["column"]].dtype][
                field_data["parser"]
            ]
        )

        # Get data slice indices
        start_idx, end_idx = field_data["slices"][idx]

        # Parse field data
        parser(
            partition_idx=idx,
            partition_data_group=data_group,
            partition_field_name=field_name,
            data_frame=data,
            data_column_name=field_data["column"],
            data_start_idx=start_idx,
            data_end_idx=end_idx,
            ctx=ctx,
            **field_data.get("parser_args", {})
        )
 
    # Close file
    h5_file.close()

    return idx, h5_filename


def create_virtual_dataset_from_partitions(
        file: str,
        partitions: List[str],
        attrs: Optional[dict] = None
) -> None:
    """Creates a virtual dataset given a set of partitions.
    
    Args:
        file (str): Virtual dataset output file.
        partitions (List[str]): List of partitions to accumulate in a single
            virtual dataset.
        attrs (Optional[dict]): Virtual dataset attributes.
    """
    # Check all partition files exist
    for partition in partitions:
        if not is_file_with_ext(partition, ext=".h5"):
            exit_error(f"Invalid partition file '{partition}'")
    
    virtual_specs = {"file": file, "fields": {}}
    partition_specs = []
    accum_idx = {}
    
    for partition in partitions:
        with h5py.File(partition) as f:
            partition_specs.append(
                {
                    # NOTE: Relative path may create empty virtual dataset
                    "file": os.path.abspath(partition),
                    "fields": {},
                    "attrs": dict(f.attrs)
                }
            )

            for field_name, field_data in f["data"].items():
                # Update virtual specs
                if field_name not in virtual_specs["fields"]:
                    virtual_specs["fields"][field_name] = {
                        "dtype": field_data.dtype,
                        "attrs": dict(field_data.attrs)
                    }
                
                virtual_specs["fields"][field_name]["shape"] = (
                    field_data.shape
                    if virtual_specs["fields"][field_name].get("shape") is None
                    else  stack_shape(
                        virtual_specs["fields"][field_name]["shape"],
                        field_data.shape,
                        axis=0
                    )
                )

                # Update partition specs
                if field_name not in accum_idx:
                    accum_idx[field_name] = 0

                # Assumes elements are grouped by first index (axis=0)
                partition_specs[-1]["fields"][field_name] = (
                    {
                        "dtype": field_data.dtype,
                        "shape": field_data.shape,
                        "start_idx": accum_idx[field_name],
                        "end_idx": (
                            accum_idx[field_name] + field_data.shape[0]
                        )
                    }
                )
                
                accum_idx[field_name] = (
                    accum_idx[field_name] + field_data.shape[0]
                )
    
    # Create virtual layout(s)
    layouts = {}

    for field_name, field_specs in virtual_specs["fields"].items():
        layouts[field_name] = h5py.VirtualLayout(
            shape=field_specs["shape"],
            dtype=field_specs["dtype"]
        )

        for specs in partition_specs:
            src = h5py.VirtualSource(
                specs["file"],
                name=f"data/{field_name}",
                shape=specs["fields"][field_name]["shape"],
                dtype=specs["fields"][field_name]["dtype"]
            )

            start_idx = specs["fields"][field_name]["start_idx"]
            end_idx = specs["fields"][field_name]["end_idx"]
            layouts[field_name][start_idx:end_idx, ...] = src
    
    # Fill virtual .h5 file
    with h5py.File(file, "w", libver="latest") as h5_file:
        # Create data group
        data_group = h5_file.create_group("data")

        # Create virtual datasets
        for layout_name, layout in layouts.items():
            virtual_dataset = data_group.create_virtual_dataset(
                name=layout_name,
                layout=layout
            )
            
            for k, v in virtual_specs["fields"][layout_name]["attrs"].items():
                virtual_dataset.attrs[k] = v

        # Add root attrs and ovewrite whenever necessary
        for k, v in partition_specs[0]["attrs"].items():
            h5_file.attrs[k] = v
        
        # For user provided attrs with --attrs argument in h5pack virtual        
        if attrs is not None:
            for k, v in attrs.items():
                h5_file.attrs[k] = v

        h5_file.attrs["creation_date"] = (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        h5_file.attrs["source"] = (
            ", ".join([os.path.basename(p) for p in partitions])
        )
        h5_file.attrs["producer"] = f"h5pack {version('h5pack')}"
        h5_file.attrs["is_virtual"] = True
