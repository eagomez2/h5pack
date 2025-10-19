import os
import math
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
    change_extension
)
from ..core.guards import is_file_with_ext
from ..core.display import (
    ask_confirmation,
    exit_error,
    print_warning
)
from ..core.utils import (
    get_file_checksum,
    time_to_str,
    total_to_list_slices
)
from ..data.validators import validate_config_file
from ..data import (
    get_validators_map
)
from .utils import (
    create_partition_from_data_,
    create_virtual_dataset_from_partitions
)


def cmd_pack(args: Namespace) -> None:
    # --------------------------------------------------------------------------
    # SECTION: VALIDATE DATA AND PREPARE SPECS
    # --------------------------------------------------------------------------
    # Check config file exists
    if not is_file_with_ext(args.config, ext=[".yaml", ".yml"]):
        exit_error(f"Invalid configuration file '{args.config}'")
    
    # Infer root based on input .yaml file and add to parsing context
    ctx = {
        "root_dir": os.path.dirname(os.path.abspath(args.config))
    }

    print(f"Using root folder '{ctx['root_dir']}'")

    # NOTE: args.files_per_partition and args.partitions are mutually exclusive
    # but if args.files_per_partition is set, both arguments will be different
    # from None since args.partitions has a default value
    if args.files_per_partition is not None:
        args.partitions = None
    
    # Validate specifications
    print(f"Validating configuration file '{args.config}' ...")
    config = validate_config_file(file=args.config, ctx=ctx)
    print("Configuration file validation completed")

    # Check if selected dataset exists
    if args.dataset not in config["datasets"]:
        datasets_repr = ", ".join(f"'{d}'" for d in config["datasets"] )
        exit_error(
            f"Dataset '{args.dataset}' not found in '{args.config}'. Available"
            f" datasets: {datasets_repr}"
        )
    
    # Get input data
    data_file = os.path.join(
        ctx["root_dir"],
        config["datasets"][args.dataset]["data"]["file"]
    )

    if not is_file_with_ext(data_file, ".csv"):
        exit_error(f"Invalid data file '{data_file}'")

    data_df = pl.read_csv(data_file, has_header=True)
    specs = config["datasets"][args.dataset]["data"]

    # Validate data fields
    if not args.skip_validation:
        print("Validating input data ...")

        for field_name, field_data in specs["fields"].items():
            col_name = field_data["column"]
            parser_name = field_data["parser"]
            parser_args = field_data.get("parser_args", {})  # Optional

            if col_name not in data_df.columns:
                exit_error(
                    f"Column '{col_name}' of field '{field_name}' not found in"
                    f" file '{data_file}'"
                )
            
            # Validate field
            print(f"Validating data of '{field_name}' field ...")
            validation_progress_bar = Progress(
                TextColumn("{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("{task.completed}/{task.total}"),
                TimeRemainingColumn(),
                transient=True
            )
            ctx["progress_bar"] = validation_progress_bar
            validators = get_validators_map().get(parser_name, None)

            if validators is not None:
                for validator in validators:
                    try:
                        validator(
                            data_df,
                            col=col_name,
                            ctx=ctx,
                            **parser_args
                        )
                    
                    except Exception as e:
                        exit_error(
                            f"Data validation of '{field_name}' failed: {e}"
                        )
            
            del ctx["progress_bar"]
            print(f"Validation of '{field_name}' field data completed")
        
        print("Input data validation completed")
    
    else:
        print_warning("Skipping data validation (--skip-validation enabled)")
    
    # Generate partition specs
    print(f"Generating {args.partitions} partition spec(s) ...")

    num_partitions = (
        math.ceil(len(data_df) / args.files_per_partition)
        if args.files_per_partition is not None else args.partitions
    )

    for field_name, field_data in specs["fields"].items():
        try:
            # Calculate partition slices
            col_name = specs["fields"][field_name]["column"]
            num_rows = data_df[col_name].len()
            specs["fields"][field_name]["slices"] = total_to_list_slices(
                total=num_rows,
                slices=num_partitions
            )
            
        except Exception as e:
            exit_error(
                f"Partition slices for field '{field_name}' failed: {e}"
            )

    print("Partition spec(s) completed")

    if not args.unattended:
        print(f"{num_partitions} partition(s) will be created")
        ask_confirmation()
    
    # --------------------------------------------------------------------------
    # SECTION: CREATE PARTITIONS
    # --------------------------------------------------------------------------
    # Create dataset and parse data
    print("Creating partitions ...")

    # Add root attrs
    h5pack_attrs = {
        "producer": f"h5pack {__version__}",
        "creation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    if specs.get("attrs", None) is not None:
        specs["attrs"].update(h5pack_attrs)
    
    else:
        specs["attrs"] = h5pack_attrs

    # Add user attrs
    specs["attrs"].update(config["datasets"][args.dataset]["attrs"])

    # Generate partitions
    manager = Manager()
    queue = manager.Queue()
    ctx["queue"] = queue
    ctx["num_partitions"] = num_partitions  # Used in workers

    # Add progress bar per field
    progress_bar = Progress(
        TextColumn("{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        transient=True
        # disable=True
    )
    task_ids = {}

    # Check partition files do not exist
    if num_partitions == 1:
        partition_file = add_extension(args.output, ext=".h5")

        if os.path.isfile(partition_file) and not args.overwrite:
            exit_error(
                f"File '{partition_file}' already exists. Use --overwrite "
                "to allow replacing existing files"
            )
    
    else:
        for partition_idx in range(num_partitions):
            partition_file = add_suffix(
                add_extension(args.output, ext=".h5"),
                f".pt{str(partition_idx).zfill(len(str(num_partitions)))}"
            )

            if os.path.isfile(partition_file) and not args.overwrite:
                exit_error(
                    f"File '{partition_file}' already exists. Use "
                    "--overwrite to allow replacing existing files"
                )

    with progress_bar:
        # Add each partition field as a separate task
        for partition_idx in range(num_partitions):
            for field_name, field_data in specs["fields"].items():
                start_idx, end_idx = field_data["slices"][partition_idx]
                task_ids[f"{partition_idx}_{field_name}"] =\
                    progress_bar.add_task(
                        f"Partition #{partition_idx} ({field_name})",
                        total=end_idx - start_idx
                    )

        partition_filenames = []
        start_time = perf_counter()

        if args.workers == 1:  # Sequential
            for partition_idx in range(num_partitions):
                container = {}

                def worker():
                    container["result"] = create_partition_from_data_(
                        idx=partition_idx,
                        specs=specs,
                        data=data_df,
                        args=args,
                        ctx=ctx
                    )
                
                # Run worker in a thread
                t = Thread(target=worker)
                t.start()

                while t.is_alive() or not queue.empty():
                    try:
                        partition_idx, step = queue.get(timeout=0.1)
                        progress_bar.update(
                            task_ids[f"{partition_idx}_{field_name}"],
                            advance=step
                        )
                    
                    except Empty:
                        pass

                t.join()
                partition_idx, filename = container["result"]

                for task_id in task_ids:
                    # Remove all partition tasks
                    if task_id.startswith(str(partition_idx)):
                        progress_bar.remove_task(task_ids[task_id])

                partition_filenames.append(filename)
                print(f"Partition #{partition_idx} saved to '{filename}'")
        
        else:  # Recurrent
            finished_jobs = set()
            pool = Pool(processes=args.workers)
            jobs = [
                pool.apply_async(
                    func=create_partition_from_data_,
                    args=(partition_idx, specs, data_df, args, ctx)
                )
                for partition_idx in range(num_partitions)
            ]

            while len(finished_jobs) < num_partitions:
                try:
                    partition_idx, step = queue.get(timeout=0.1)
                    progress_bar.update(
                        task_ids[f"{partition_idx}_{field_name}"],
                        advance=step
                    )
                
                except Empty:
                    pass
            
                for job_idx, job in enumerate(jobs):
                    if job.ready() and job_idx not in finished_jobs:
                        finished_jobs.add(job_idx)
                        partition_idx, filename = job.get()

                        for task_id in task_ids:
                            # Remove all partition tasks
                            if task_id.startswith(str(partition_idx)):
                                progress_bar.remove_task(task_ids[task_id])

                        partition_filenames.append(filename)
                        print(
                            f"Partition #{partition_idx} saved to '{filename}'"
                        )
                    
            pool.close()
            pool.join()

    print("\033[F\033[K", end="")  # Clear blank line from Rich

    # --------------------------------------------------------------------------
    # SECTION: CREATE VIRTUAL DATASET
    # --------------------------------------------------------------------------
    if args.create_virtual and num_partitions > 1:
        print("Creating virtual dataset ...")

        virtual_dataset_filename = add_extension(args.output, ext=".h5")
        create_virtual_dataset_from_partitions(
            file=virtual_dataset_filename,
            partitions=partition_filenames
        )
        print(f"Virtual dataset saved to '{virtual_dataset_filename}'")

    # --------------------------------------------------------------------------
    # SECTION: CREATE CHECKSUM FILE
    # --------------------------------------------------------------------------
    if not args.skip_checksum:
        print("Creating checksum file ...")
        
        checksum_filename = change_extension(
            args.output,
            new_ext=".sha256"
        )

        checksum_progress_bar = Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("{task.completed}/{task.total}"),
            TimeRemainingColumn(),
            transient=True
        )
        task_id = checksum_progress_bar.add_task(
            "Computing checksum",
            total=len(partition_filenames)
        )

        with checksum_progress_bar:
            with open(checksum_filename, "w") as f:
                for partition_filename in partition_filenames:
                    partition_file = os.path.join(
                        ctx["root_dir"],
                        partition_filename
                    )
                    partition_file_sha256 = get_file_checksum(
                        file=partition_file
                    )
                    f.write(
                        f"{os.path.basename(partition_filename)}"
                        f"\t{partition_file_sha256}\n"
                    )
                    checksum_progress_bar.advance(task_id)
                
                if args.create_virtual and num_partitions > 1:
                    virtual_dataset_file = os.path.join(
                        ctx["root_dir"],
                        virtual_dataset_filename
                    )
                    virtual_dataset_file_sha256 = get_file_checksum(
                        file=virtual_dataset_file
                    )
                    f.write(
                        f"{os.path.basename(virtual_dataset_filename)}\t"
                        f"{virtual_dataset_file_sha256}\n"
                    )
            
            print(f"Checksum file saved to '{checksum_filename}'")

    end_time = perf_counter()
    elapsed_time_repr = time_to_str(end_time - start_time)
    print(f"{num_partitions} partition(s) created in {elapsed_time_repr}")
