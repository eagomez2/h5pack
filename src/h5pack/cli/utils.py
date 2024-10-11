import os
import json
import h5py
import polars as pl
from tqdm import tqdm
from typing import (
    List,
    Tuple
)
from time import perf_counter
from datetime import datetime
from argparse import Namespace
from h5pack import __version__
from multiprocessing import (
    Pool,
    RLock
)
from ..core.guards import is_file_with_ext
from ..core.display import (
    exit_error,
    print_warning
)
from ..core.utils import (
    get_file_checksum,
    stack_shape,
    total_to_list_slices,
    time_to_str
)
from ..core.io import (
    add_extension,
    add_suffix
)
from ..data import (
    get_parsers_map,
    get_validators_map
)
from ..data.validators import validate_attrs


def create_partition_from_data(
        idx: int,
        data_specs: dict,
        data_df: pl.DataFrame,
        args: Namespace
) -> Tuple[int, str]:
    # Create file
    h5_filename = add_extension(args.output, ext=".h5")

    if args.partitions != 1:
        h5_filename = add_suffix(
            h5_filename,
            f".pt{str(idx).zfill(len(str(args.partitions)))}"
        )
    
    h5_file = h5py.File(h5_filename, "w")

    # Add root attrs
    for k, v in data_specs["attrs"].items():
        h5_file.attrs[k] = v
    
    # Add data group
    data_group = h5_file.create_group("data")

    # Add data
    for field_name, field_data in data_specs["fields"].items():
        # Get parser
        parser = (
            get_parsers_map()[data_df[field_data["column"]].dtype]
            [field_data["parser"]]
        )

        # Get data slice indices
        start_idx, end_idx = field_data["slices"][idx]

        parser(
            partition_idx=idx,
            partition_data_group=data_group,
            partition_field_name=field_name,
            data_frame=data_df,
            data_column_name=field_data["column"],
            data_start_idx=start_idx,
            data_end_idx=end_idx,
            verbose=args.verbose
        )
    
    h5_file.close()

    return idx, h5_filename


def create_virtual_dataset_from_partitions(
        file: str,
        partitions: List[str],
        verbose: bool = False
) -> None:
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
                    "file": partition,
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

        h5_file.attrs["creation_date"] = (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        h5_file.attrs["source"] = (
            ", ".join([os.path.basename(p) for p in partitions])
        )
        h5_file.attrs["producer"] = f"h5pack {__version__}"


def cmd_create(args: Namespace) -> None:
    # Check specs file exist
    if not is_file_with_ext(args.input, ext=".json"):
        exit_error(f"Input file '{args.input}' not found")
    
    # Infer root based on input file
    root_dir = os.path.dirname(os.path.abspath(args.input))

    if args.verbose:
        print(f"Using root folder '{root_dir}'")

    # Validate specs file
    if args.verbose:
        print(f"Validating input file '{args.input}' ...")
    
    try:
        with open(args.input, "r") as f:
            specs = json.load(f)
        
    except Exception as e:
        exit_error(f"Input file could not be parsed: {e}")

    # Validate attrs key
    root_attrs = specs.get("attrs", None)

    if root_attrs is not None:
        try:
            validate_attrs(root_attrs)
        
        except TypeError as e:
            exit_error(f"Input attributes error: {e}")
    
    # Validate data key
    data = specs.get("data", None)

    if data is None:
        exit_error("Missing 'data' key in input file")
    
    for k in ("file", "fields"):
        if k not in data:
            exit_error(f"Missing '{k}' key in 'data'")

    data_file = os.path.join(root_dir, data["file"])

    if not is_file_with_ext(data_file, ext=".csv"):
        exit_error(f"Data file '{data_file}' not found")
 
    for field_name, field_data in data["fields"].items():
        # Check fields exist
        for k in ("column", "parser"):
            if k not in field_data:
                exit_error(f"Missing '{k}' key in data field '{field_name}'")
    
    if args.verbose:
        print("Input file validation completed")
    
    # Get input data
    data_df = pl.read_csv(data_file, has_header=True)

    if not args.skip_validation:
        if args.verbose:
            print("Validating input data ...")

        for field_name, field_data in data["fields"].items():
            col_name = data["fields"][field_name]["column"]
            parser_name = data["fields"][field_name]["parser"]

            if col_name not in data_df.columns:
                exit_error(
                    f"Column '{col_name}' of field '{field_name}' not found in"
                    f" file '{data_file}'"
                )
            
            # Validate field
            if args.verbose:
                print(f"Validating data of '{field_name}' field ...")

            validators = get_validators_map().get(parser_name, None)

            if validators is not None:
                for validator in validators:
                    try:
                        validator(data_df, col=col_name, verbose=args.verbose)
                    
                    except Exception as e:
                        exit_error(
                            f"Data validation of '{field_name}' failed: {e}"
                        )
            
            if args.verbose:
                print(f"Validation of '{field_name}' field data completed")
        
        if args.verbose:
            print("Input data validation completed")
    
    else:
        if args.verbose:
            print_warning(
               "Skipping data validation (--skip-validation enabled)"
            )
        
    # Generate partition specs
    if args.verbose:
        print(f"Generating {args.partitions} partition spec(s) ...")

    for field_name, field_data in data["fields"].items():
        try:
            # Calculate partition slices
            col_name = data["fields"][field_name]["column"]
            data["fields"][field_name]["slices"] = total_to_list_slices(
                total=data_df[col_name].len(), slices=args.partitions
            )
        
        except Exception as e:
            exit_error(
                f"Partition slices for field '{field_name}' failed: {e}"
            )
    
    if args.verbose:
        print(f"{args.partitions} partition spec(s) completed")
    
    # Create dataset and parse data
    if args.verbose:
        print("Creating partitions ...")
    
    # Add root attrs
    if root_attrs is not None:
        data["attrs"] = root_attrs
        data["attrs"]["producer"] = f"h5pack {__version__}"
        data["attrs"]["creation_date"] = (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    
    start_time = perf_counter()
    partition_filenames = []
    
    if args.workers == 1:
        for partition_idx in range(args.partitions):
            idx, filename = create_partition_from_data(
                idx=partition_idx,
                data_specs=data,
                data_df=data_df,
                args=args
            )
            partition_filenames.append(filename)
            print(f"Partition #{idx} saved to '{filename}'")
    
    else:
        pool = Pool(
            processes=None if args.workers == 0 else args.workers,
            initargs=(RLock(),),
            initializer=tqdm.set_lock
        )

        jobs = []

        for partition_idx in range(args.partitions):
            jobs.append(
                pool.apply_async(
                    create_partition_from_data,
                    args=(partition_idx, data, data_df, args)
                )
            )

        pool.close()
        results = [job.get() for job in jobs]

        for idx, filename in results:
            partition_filenames.append(filename)
            tqdm.write(f"Partition #{idx} saved to '{filename}'")
        
    # Create virtual layout
    if not args.skip_virtual and args.partitions > 1:
        if args.verbose:
            print("Creating virtual dataset ...")
        
        virtual_dataset_filename = add_extension(args.output, ext=".h5")

        create_virtual_dataset_from_partitions(
            file=virtual_dataset_filename,
            partitions=partition_filenames,
            verbose=args.verbose
        )

        print(f"Virtual dataset saved to '{virtual_dataset_filename}'")
    
    else:
        if args.verbose:
            print_warning(
                "Skipping virtual layout generation (--skip-virtual enabled)"
            )
    
    # Create checksum file
    if not args.skip_checksum:
        if args.verbose:
            print("Creating checksum file ...")
        
        with open("checksum.sha256", "w") as f:
            for partition_filename in partition_filenames:
                partition_file = os.path.join(root_dir, partition_filename)
                partition_file_sha256 = get_file_checksum(file=partition_file)
                f.write(f"{partition_filename}\t{partition_file_sha256}\n")
            
            if not args.skip_virtual and args.partitions > 1:
                virtual_dataset_file = os.path.join(
                    root_dir,
                    virtual_dataset_filename
                )
                virtual_dataset_file_sha256 = get_file_checksum(
                    file=virtual_dataset_file
                )
                f.write(
                    f"{virtual_dataset_filename}\t"
                    f"{virtual_dataset_file_sha256}\n"
                )

    end_time = perf_counter()
    elapsed_time_repr = time_to_str(end_time - start_time, abbrev=True)
    print(f"{args.partitions} partition(s) created in {elapsed_time_repr}")
