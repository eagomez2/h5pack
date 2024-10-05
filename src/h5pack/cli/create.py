import os
import argparse
import h5py
import numpy as np
from datetime import datetime
from typing import (
    List,
    Tuple
)
from tqdm import tqdm
from multiprocessing import (
    Pool,
    RLock
)
from ..core.display import (
    ask_confirmation,
    exit_error
)
from ..core.guards import is_file_with_ext
from ..core.io import (
    add_extension,
    add_suffix,
    get_dir_files,
    read_audio,
    read_audio_metadata
)
from ..core.utils import (
    dict_from_interleaved_list,
    list_from_csv_col,
    list_from_tsv_col,
    make_list
)


def verify_audio_files(files: List[str], vlen: bool = False) -> None:
    observed_fs = []

    if not vlen:
        observed_samples = []

    for file in tqdm(
            files,
            desc="Verifying files",
            colour="green",
            unit="file",
            leave=False
    ):
        audio_meta = read_audio_metadata(file)

        # Only mono files are supported
        if audio_meta["num_channels"] != 1:
            exit_error(
                "Currently only mono files are supported. "
                f"{audio_meta['num_channels']} channels found in '{file}'"
            )

        # All files should have same sample rate
        if audio_meta["fs"] not in observed_fs:
            observed_fs.append(audio_meta["fs"])

        if len(observed_fs) > 1:
            exit_error(
                f"All files should have the same sample rate. Previous "
                f"files have sample_rate={observed_fs[0]} but '{file}' has "
                f"sample_rate={audio_meta['fs']}",
                writer=tqdm
            )

        # If not --vlen, all files should have the same length
        if (
            not vlen
            and audio_meta["num_samples_per_channel"] not in observed_samples
        ):
            observed_samples.append(audio_meta["num_samples_per_channel"])
            
            if len(observed_samples) > 1:
                exit_error(
                    "All files should have the same length. Previous files "
                    f"have sample_len={observed_samples[0]}, but '{file}' has "
                    f"sample_len= {audio_meta['num_samples_per_channel']}. If "
                    "you intend to write a variable length .h5 files, please "
                    "use the --vlen option",
                    writer=tqdm
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
    audio_meta = read_audio_metadata(files[0])

    for filename, files in zip(filenames, partitions):
        specs.append({
            "filename": filename,
            "files": files,
            "dtype": args.dtype,
            "sample_len": audio_meta["num_samples_per_channel"],
            "attrs": {
                "creation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "fs": audio_meta["fs"]
            }
        })

        if args.meta is not None:
            if len(args.meta) % 2 != 0:
                exit_error(
                    "--meta should have an even number of elements where each "
                    "odd value corresponds to a key and each even value "
                    "corresponds to a value to be added as metadata"
                )

            specs[-1]["attrs"].update(
                dict_from_interleaved_list(args.meta)
            )

    return specs


def create_partition_from_specs(
        idx: int,
        specs: dict,
        args: argparse.Namespace
) -> Tuple[int, str]:
    # Create partition file
    h5_file = h5py.File(specs["filename"], "w")

    # Create "data" data group
    data_group = h5_file.create_group("data")

    # Create dataset
    dataset = data_group.create_dataset(
        name="audio",
        shape=(len(specs["files"]), specs["sample_len"]),
        dtype=specs["dtype"]
    )

    # Add attributes
    # TODO: Organize data levels (file, group or data attributes)
    for name, attr in specs["attrs"].items():
        h5_file.attrs[name] = attr
    
    # Add partition data
    for idx, file in enumerate(
            tqdm(
                specs["files"],
                desc=f"Writing partition #{idx}",
                colour="green" if args.workers == 1 else "cyan",
                leave=False,
                unit="files",
                position=idx
            )
    ):
        # NOTE: Files have been verified at this point so there is no need for
        # additional assertions
        data, _ = read_audio(file, dtype=specs["dtype"])
        dataset[idx, :] = data

    # Close file
    h5_file.close()
    return idx, specs["filename"]


def create_partitions(args: argparse.Namespace) -> None:
    # Check input dir or file exists
    if is_file_with_ext(args.input, ext=".csv"):
        if args.type == "audio":
            audio_files = list_from_csv_col(args.input, col=args.audio_col)
        
        elif args.type == "audio->metric":
            audio_files = list_from_csv_col(args.input, col=args.audio_col)
            metrics = list_from_csv_col(args.input, col=args.metric_col)
        
        else:
            raise AssertionError
    
    elif is_file_with_ext(args.input, ext=".tsv"):
        if args.type == "audio":
            audio_files = list_from_tsv_col(args.input, col=args.audio_col)
        
        elif args.type == "audio->metric":
            audio_files = list_from_tsv_col(args.input, col=args.audio_col)
            metrics = list_from_tsv_col(args.input, col=args.metric_tol)

    elif os.path.isdir(args.input):
        if args.type != "audio":
            exit_error(
                "Using a folder as --input is only supported for datasets of "
                "--type audio"
            )

        audio_files = get_dir_files(
            args.input,
            ext=args.extension,
            recursive=args.recursive
        )
    
    else:
        exit_error(f"Invalid input file or folder '{args.input}'")
    
    if len(audio_files) == 0:
        if os.path.isdir(args.input):
            exit_error(
                f"0 files found in '{args.input}'. Use --recursive if you "
                "intended to perform a recursive search"
            )
        
        else:
            exit_error(
                f"0 files found in column '{args.audio_col}' of '{args.input}'"
            )
    
    if args.type == "audio->metric" and len(metrics) == 0:
        exit_error(
            f"0 metrics found in column '{args.metric_col}' of '{args.input}'"
        )
    
    # Verify audio files
    if not args.skip_verification:
        verify_audio_files(files, vlen=args.vlen)
    
    # Create data for each partition
    partition_specs = create_partition_specs(files, args)

    # User confirmation
    if not args.unattended:
        print(
            f"{len(partition_specs)} HDF5 partition(s) will be generated "
            f"for {len(files)} file(s)"
        )

        ask_confirmation()

    # Create partition(s)
    if args.workers == 1:
        for idx, specs in enumerate(partition_specs):
            _, filename = create_partition_from_specs(idx, specs, args)
            print(f"Partition #{idx} saved to '{filename}'")

    else:
        pool = Pool(
            processes=None if args.workers == 0 else args.workers,
            initargs=(RLock(),),
            initializer=tqdm.set_lock
        )
        jobs = [
            pool.apply_async(
                create_partition_from_specs,
                args=(idx, specs, args)
            )
            for idx, specs in enumerate(partition_specs)
        ]
        pool.close()
        results = [job.get() for job in jobs]

        for _, filename in results:
            print(f"Partition saved to '{filename}'")
