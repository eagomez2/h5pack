import os
import sys
import math
import polars as pl
from argparse import Namespace
from datetime import datetime
from threading import Thread
from time import perf_counter
from multiprocessing import get_context
from importlib.metadata import version
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn
)
from queue import (
    Empty,
    Queue
)
from ..core.io import (
    add_extension,
    change_extension
)
from ..core.guards import is_file_with_ext
from ..core.display import (
    ask_confirmation,
    exit_error,
    get_console,
    is_progress_enabled,
    print_debug,
    print_info,
    print_output,
    print_step,
    print_warning
)
from ..core.utils import (
    format_size,
    get_file_checksum,
    total_to_list_slices
)
from ..data.parsers import (
    get_categories,
    get_common_dir
)
from ..data.validators import validate_config_file
from ..data import (
    get_parsers_map,
    get_validators_map
)
from .utils import (
    create_partition_from_data_safe,
    create_virtual_dataset_from_partitions,
    get_partition_filename
)


def _get_progress_bar() -> Progress:
    """Returns the progress bar used by all tools.

    Returns:
        (Progress): Transient `rich` progress bar.
    """
    return Progress(
        TextColumn("[bold cyan]{task.description:>10}[/bold cyan]"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(),
        console=get_console(),
        transient=True,
        disable=not is_progress_enabled()
    )


def _advance_progress(
        progress_bar: Progress,
        task_id: int,
        queue: Queue
) -> bool:
    """Consumes one progress message sent by a partition worker.

    Args:
        progress_bar (Progress): Progress bar to update.
        task_id (int): Progress bar task id.
        queue (Queue): Queue shared with the workers.

    Returns:
        (bool): `True` if a message was consumed, `False` if the queue was
            empty.
    """
    try:
        _, _, step = queue.get(timeout=0.1)

    except Empty:
        return False

    progress_bar.update(task_id, advance=step)

    return True


def _describe_audio(info: dict) -> str:
    """Returns a short description of the audio files of a field.

    Args:
        info (dict): Audio information collected during validation.

    Returns:
        (str): Description such as `16 kHz, mono`.
    """
    fs = info["sample_rates"][0] if info["sample_rates"] else None
    fs_repr = f"{fs / 1000:g} kHz" if fs is not None else "unknown rate"
    channels = info["num_channels"]
    channels_repr = (
        "mono" if channels == 1
        else "stereo" if channels == 2
        else f"{channels} channels"
    )

    return f"{fs_repr}, {channels_repr}"


def cmd_pack(args: Namespace) -> None:
    """Creates a HDF5 dataset in one or multiple partitions given a set of user
    input aguments.

    Args:
        args (Namespace): User input arguments provided through the console.
    """
    # -------------------------------------------------------------------------
    # SECTION: VALIDATE DATA AND PREPARE SPECS
    # -------------------------------------------------------------------------
    start_time = perf_counter()

    # Assign workers equal to cpu cores if value is 0
    if args.workers == 0:
        args.workers = os.cpu_count()

    # Check config file exists
    if not is_file_with_ext(args.config, ext=[".yaml", ".yml"]):
        exit_error(
            f"Invalid configuration file '{args.config}'",
            hint=(
                "Use 'h5pack init <csv-file>' to create a configuration file"
            )
        )

    # Infer root based on input .yaml file and add to parsing context
    ctx = {
        "root_dir": os.path.dirname(os.path.abspath(args.config))
    }
    print_debug(f"Using root folder '{ctx['root_dir']}'")

    # NOTE: args.files_per_partition and args.partitions are mutually exclusive
    # but if args.files_per_partition is set, both arguments will be different
    # from None since args.partitions has a default value
    if args.files_per_partition is not None:
        args.partitions = None

    # Validate specifications
    step_time = perf_counter()
    config = validate_config_file(file=args.config, ctx=ctx)

    # Check if selected dataset exists
    if args.dataset not in config["datasets"]:
        datasets_repr = ", ".join(f"'{d}'" for d in config["datasets"] )
        exit_error(
            f"Dataset '{args.dataset}' not found in '{args.config}'",
            hint=f"Available datasets: {datasets_repr}"
        )

    # Get input data
    data_file = os.path.join(
        ctx["root_dir"],
        config["datasets"][args.dataset]["data"]["file"]
    )

    if not is_file_with_ext(data_file, ".csv"):
        exit_error(f"Invalid data file '{data_file}'")

    specs = config["datasets"][args.dataset]["data"]

    # NOTE: Columns used by text, audio and list parsers are read as str, so
    # values such as '007' are not converted to numbers
    str_columns = {
        field_data["column"]: pl.String
        for field_data in specs["fields"].values()
        if field_data["parser"].startswith(("as_audio", "as_list"))
        or field_data["parser"] == "as_utf8str"
    }

    try:
        data_df = pl.read_csv(
            data_file,
            has_header=True,
            schema_overrides=str_columns
        )

    except Exception as e:
        exit_error(f"Cannot read '{data_file}'", cause=str(e))

    print_step(
        "Validated",
        args.config,
        details=(
            f"dataset '{args.dataset}', {len(specs['fields'])} field(s), "
            f"{len(data_df)} row(s)"
        ),
        elapsed=perf_counter() - step_time
    )

    # Check columns and parsers before reading any data
    for field_name, field_data in specs["fields"].items():
        col_name = field_data["column"]
        parser_name = field_data["parser"]

        if col_name not in data_df.columns:
            exit_error(
                f"Column '{col_name}' of field '{field_name}' not found in "
                f"'{data_file}'",
                hint=f"Available columns: {', '.join(data_df.columns)}"
            )

        col_dtype = data_df[col_name].dtype
        available_parsers = get_parsers_map().get(col_dtype, {})

        if parser_name not in available_parsers:
            exit_error(
                f"Parser '{parser_name}' of field '{field_name}' cannot be "
                f"used with column '{col_name}' of type '{col_dtype}'",
                hint=(
                    "Parsers available for this column: "
                    f"{', '.join(available_parsers) or 'none'}"
                )
            )

    # Validate data fields
    if not args.skip_validation:
        step_time = perf_counter()

        for field_name, field_data in specs["fields"].items():
            col_name = field_data["column"]
            parser_name = field_data["parser"]
            parser_args = field_data.get("parser_args", {}) or {}  # Optional
            print_debug(f"Validating data of field '{field_name}' ...")
            ctx["progress_bar"] = _get_progress_bar()
            validators = get_validators_map().get(parser_name, [])

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
                        f"Validation of field '{field_name}' failed",
                        cause=str(e)
                    )

            del ctx["progress_bar"]

        audio_details = [
            _describe_audio(info)
            for info in ctx.get("audio_info", {}).values()
        ]
        print_step(
            "Validated",
            f"{len(data_df)} row(s)",
            details=", ".join(audio_details) if audio_details else None,
            elapsed=perf_counter() - step_time
        )

        # Warn when float parsers are used with 16-bit audio files
        for field_name, field_data in specs["fields"].items():
            info = ctx.get("audio_info", {}).get(field_data["column"])

            if (
                info is not None
                and field_data["parser"] in (
                    "as_audiofloat32",
                    "as_audiofloat64"
                )
                and all(
                    s in ("PCM_16", "PCM_S8", "PCM_U8")
                    for s in info["subtypes"]
                )
            ):
                print_warning(
                    f"Field '{field_name}' uses '{field_data['parser']}' but "
                    "its audio files are 16-bit or less. Use 'as_audioint16' "
                    "or 'as_audioflac' to reduce the file size without losing "
                    "information"
                )

    else:
        print_warning("Skipping data validation (--skip-validation enabled)")

    # Generate partition specs
    num_partitions = (
        math.ceil(len(data_df) / args.files_per_partition)
        if args.files_per_partition is not None else args.partitions
    )

    if num_partitions < 1 or num_partitions > max(len(data_df), 1):
        exit_error(
            f"Invalid number of partitions ({num_partitions}) for "
            f"{len(data_df)} row(s)"
        )

    slices = total_to_list_slices(total=len(data_df), slices=num_partitions)

    for field_name in specs["fields"]:
        specs["fields"][field_name]["slices"] = slices

    # Pre-compute information shared by all partitions
    ctx["fields"] = {}

    for field_name, field_data in specs["fields"].items():
        values = data_df[field_data["column"]].to_list()

        if field_data["parser"].startswith("as_audio"):
            ctx["fields"][field_name] = {
                "common_dir": get_common_dir(values, ctx["root_dir"])
            }

        elif field_data["parser"] == "as_categorical":
            ctx["fields"][field_name] = {"categories": get_categories(values)}

    # Compression settings used by the parsers
    ctx["compression"] = args.compression
    ctx["compression_level"] = args.compression_level
    ctx["skip_filepaths"] = args.skip_filepaths

    partition_files = [
        get_partition_filename(
            output=args.output,
            idx=partition_idx,
            num_partitions=num_partitions
        )
        for partition_idx in range(num_partitions)
    ]

    # Check partition files do not exist
    for partition_file in partition_files:
        if os.path.isfile(partition_file) and not args.overwrite:
            exit_error(
                f"File '{partition_file}' already exists",
                hint="Use --overwrite to allow replacing existing files"
            )

    # Show planned partitions without writing any file
    if args.dry_run:
        print_step(
            "Planned",
            f"{num_partitions} partition(s) with "
            f"{min(args.workers, num_partitions)} worker(s)",
            details=(
                f"compression: {args.compression}"
                if args.compression != "none" else None
            )
        )

        for partition_file, (start_idx, end_idx) in zip(
            partition_files,
            slices,
            strict=True
        ):
            print_output(
                partition_file,
                details=f"rows {start_idx} to {end_idx - 1}"
            )

        if args.create_virtual and num_partitions > 1:
            print_output(
                add_extension(args.output, ext=".h5"),
                details="virtual dataset"
            )

        if not args.skip_checksum:
            print_output(
                change_extension(args.output, new_ext=".sha256"),
                details="checksums"
            )

        print_info("Dry run completed, no files were written")
        return

    if not args.unattended:
        ask_confirmation(
            f"{num_partitions} partition(s) will be created. Do you want to "
            "continue? [y/n]:"
        )

    # -------------------------------------------------------------------------
    # SECTION: CREATE PARTITIONS
    # -------------------------------------------------------------------------
    # Add root attrs
    h5pack_attrs = {
        "producer": f"h5pack {version('h5pack')}",
        "creation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    if specs.get("attrs", None) is None:
        specs["attrs"] = {}

    # Add user attrs (optional)
    specs["attrs"].update(config["datasets"][args.dataset].get("attrs", {}))

    # NOTE: h5pack attrs are added last since 'producer' is required by
    # h5pack info and h5pack unpack and cannot be overwritten by the user
    specs["attrs"].update(h5pack_attrs)

    # Generate partitions
    # NOTE: 'spawn' is used on every platform. The default on Linux is 'fork',
    # which copies the parent while polars, h5py and rich hold threads and
    # locks, and that can deadlock workers. macOS and Windows already default
    # to 'spawn'
    mp_ctx = get_context("spawn")
    manager = mp_ctx.Manager()
    queue = manager.Queue()
    ctx["queue"] = queue
    ctx["num_partitions"] = num_partitions  # Used in workers

    # NOTE: Each worker only receives the rows of its own partition to reduce
    # the memory used by each worker
    partition_data = [
        data_df.slice(start_idx, end_idx - start_idx)
        for start_idx, end_idx in slices
    ]

    num_workers = min(args.workers, num_partitions)
    progress_bar = _get_progress_bar()
    step_time = perf_counter()
    partition_filenames = []

    with progress_bar:
        task_id = progress_bar.add_task(
            "Packing",
            total=len(data_df) * len(specs["fields"])
        )

        if num_workers == 1:  # Sequential
            for partition_idx in range(num_partitions):
                container = {}

                def worker(partition_idx: int, container: dict) -> None:
                    try:
                        container["result"] = create_partition_from_data_safe(
                            idx=partition_idx,
                            specs=specs,
                            data=partition_data[partition_idx],
                            args=args,
                            ctx=ctx
                        )

                    except BaseException as e:
                        container["error"] = e

                # Run worker in a thread
                t = Thread(target=worker, args=(partition_idx, container))
                t.start()

                while t.is_alive():
                    _advance_progress(progress_bar, task_id, queue)

                t.join()

                # Drain progress messages left after the worker finished
                while _advance_progress(progress_bar, task_id, queue):
                    pass

                if "error" in container:
                    exit_error(
                        f"Partition #{partition_idx} failed",
                        cause=str(container["error"])
                    )

                _, filename = container["result"]
                partition_filenames.append(filename)
                print_debug(
                    f"Partition #{partition_idx} saved to '{filename}'"
                )

        else:  # Parallel
            pool = mp_ctx.Pool(processes=num_workers)
            jobs = {
                partition_idx: pool.apply_async(
                    func=create_partition_from_data_safe,
                    args=(
                        partition_idx,
                        specs,
                        partition_data[partition_idx],
                        args,
                        ctx
                    )
                )
                for partition_idx in range(num_partitions)
            }

            try:
                while jobs:
                    _advance_progress(progress_bar, task_id, queue)
                    finished = [
                        idx for idx, job in jobs.items() if job.ready()
                    ]

                    if not finished:
                        continue

                    # NOTE: Workers put progress messages synchronously, so
                    # once a job is ready all of its messages are queued
                    while _advance_progress(progress_bar, task_id, queue):
                        pass

                    for partition_idx in finished:
                        job = jobs.pop(partition_idx)

                        try:
                            _, filename = job.get()

                        except Exception as e:
                            exit_error(
                                f"Partition #{partition_idx} failed",
                                cause=str(e)
                            )

                        partition_filenames.append(filename)
                        print_debug(
                            f"Partition #{partition_idx} saved to '{filename}'"
                        )

                pool.close()

            finally:
                # Stops remaining workers on errors or Ctrl+C
                pool.terminate()
                pool.join()

        # Keep partitions in order regardless of which worker finished first
        partition_filenames.sort()

    manager.shutdown()

    total_size = sum(os.path.getsize(f) for f in partition_filenames)
    print_step(
        "Packed",
        f"{num_partitions} partition(s) with {num_workers} worker(s)",
        details=f"{len(data_df)} row(s), {format_size(total_size)}",
        elapsed=perf_counter() - step_time
    )

    for partition_filename in partition_filenames:
        print_output(partition_filename)

    # -------------------------------------------------------------------------
    # SECTION: CREATE VIRTUAL DATASET
    # -------------------------------------------------------------------------
    if args.create_virtual and num_partitions > 1:
        virtual_dataset_filename = add_extension(args.output, ext=".h5")
        create_virtual_dataset_from_partitions(
            file=virtual_dataset_filename,
            partitions=partition_filenames
        )
        print_output(virtual_dataset_filename, details="virtual dataset")

    # -------------------------------------------------------------------------
    # SECTION: CREATE CHECKSUM FILE
    # -------------------------------------------------------------------------
    if not args.skip_checksum:
        checksum_filename = change_extension(
            args.output,
            new_ext=".sha256"
        )
        checksum_progress_bar = _get_progress_bar()
        task_id = checksum_progress_bar.add_task(
            "Hashing",
            total=len(partition_filenames)
        )

        with checksum_progress_bar, open(checksum_filename, "w") as f:
            for partition_filename in partition_filenames:
                # NOTE: Output paths are relative to the current working
                # directory, not to the configuration file folder
                partition_file_sha256 = get_file_checksum(
                    file=partition_filename
                )
                f.write(
                    f"{os.path.basename(partition_filename)}"
                    f"\t{partition_file_sha256}\n"
                )
                checksum_progress_bar.advance(task_id)

            if args.create_virtual and num_partitions > 1:
                virtual_dataset_file_sha256 = get_file_checksum(
                    file=virtual_dataset_filename
                )
                f.write(
                    f"{os.path.basename(virtual_dataset_filename)}\t"
                    f"{virtual_dataset_file_sha256}\n"
                )

        print_output(checksum_filename, details="checksums")

    print_debug(f"Total time: {perf_counter() - start_time:.2f}s")
    sys.stdout.flush()
