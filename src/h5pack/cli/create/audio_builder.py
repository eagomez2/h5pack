import os
import h5py
import polars as pl
import numpy as np
from time import perf_counter
from datetime import datetime
from argparse import Namespace
from typing import (
    List,
    Optional,
    Tuple,
    Union
)
from tqdm import tqdm
from multiprocessing import (
    Pool,
    RLock
)
from .builder import DatasetBuilder
from ...core.display import (
    ask_confirmation,
    print_warning
)
from ...core.guards import is_file_with_ext
from ...core.io import (
    add_extension,
    add_suffix,
    change_extension,
    get_dir_files,
    read_audio,
    read_audio_metadata
)
from ...core.utils import (
    dict_from_interleaved_list,
    get_array_checksum,
    make_list,
    list_from_csv_col,
    list_from_tsv_col,
    time_to_str
)
from ...core.display import exit_error


class AudioDatasetBuilder(DatasetBuilder):
    def __init__(self, verbose: bool = False) -> None:
        super().__init__()

        # Params
        self.verbose = verbose

    def collect_data(
            self,
            input: str,
            root_dir: Optional[str] = None,
            col: Optional[str] = None,
            ext: Optional[Union[str, List[str]]] = None,
            recursive: bool = False
    ) -> List[str]:
        # Check input dir of file exists
        if is_file_with_ext(input, ext=".csv"):
            files = list_from_csv_col(input, col=col)

            if root_dir is not None:
                files = [
                    os.path.join(root_dir, os.path.relpath(f)) for f in files
                ]
        
        elif is_file_with_ext(input, ext=".tsv"):
            files = list_from_tsv_col(input, col=col)

            if root_dir is not None:
                files = [
                    os.path.join(root_dir, os.path.relpath(f)) for f in files
                ]
        
        elif os.path.isdir(input):
            files = get_dir_files(input, ext=ext, recursive=recursive)
        
        else:
            exit_error(f"Invalid input file or folder '{input}'")
        
        if len(files) == 0:
            if os.path.isdir(input):
                exit_error(
                    f"0 files found in '{input}'. Use --recursive if you "
                    "intended to perform a recursive search"
                )
            
            else:
                exit_error(f"0 files found in column '{col}' of '{input}'")
        
        return files
    
    def validate_data(self, files: List[str], vlen: bool = False) -> None:
        # Only single sample rate is required
        observed_fs = []

        # If not variable length, only a single length is expected
        if not vlen:
            observed_lens = []
        
        for file in tqdm(
            files,
            desc="Validating files",
            colour="green",
            unit="file",
            leave=False
        ):
            # Check files exist
            if not os.path.isfile(file):
                exit_error(f"Invalid file '{file}'")

            # Check files are mono
            audio_meta = read_audio_metadata(file)

            if audio_meta["num_channels"] != 1:
                exit_error(
                    f"Currently only mono files are supported, but '{file}' "
                    f"has {audio_meta['num_channels']} channels",
                    writer=tqdm
                )

            # Check files have same sample rate
            if audio_meta["fs"] not in observed_fs:
                observed_fs.append(audio_meta["fs"])
            
            if len(observed_fs) > 1:
                exit_error(
                    f"All files should have the same sample rate. Previous "
                    f"files have sample_rate={observed_fs[0]} but '{file}' has"
                    f"has sample_rate={audio_meta['fs']}",
                    writer=tqdm
                )

            # Check files have same length (if vlen=False)
            if (
                not vlen
                and audio_meta["num_samples_per_channel"] not in observed_lens 
            ):
                observed_lens.append(audio_meta["num_samples_per_channel"])
            
            if len(observed_lens) > 1:
                exit_error(
                    "All files should have the same length. Previous files "
                    f"have sample_len={observed_lens[0]}, but '{file}' has "
                    f"sample_len= {audio_meta['num_samples_per_channel']}. If "
                    "you intend to write a variable length .h5 files, please "
                    "use the --vlen option",
                    writer=tqdm
                )

    def create_partition_specs(
            self,
            files: List[str],
            output: str,
            num_partitions: int,
            dtype: str,
            meta: Optional[dict] = None,
            vlen: bool = False
    ) -> dict:
        if num_partitions > len(files):
            exit_error(
                "The number of partitions should be greater than the number of"
                f" files. Found {len(files)} file(s) for {num_partitions} "
                "partitions"
            )
        
        # Split files
        partitions = np.array_split(files, indices_or_sections=num_partitions)

        # Form .h5 partition filenames
        if len(partitions) == 1:
            filenames = make_list(add_extension(output, ext=".h5"))
        
        else:
            filenames = []
            zfill = len(str(len(partitions)))

            for idx in range(len(partitions)):
                filename = add_extension(output, ext=".h5")
                filename = add_suffix(
                    filename,
                    suffix=f".pt{str(idx).zfill(zfill)}"
                )
                filenames.append(filename)
        
        # Form partition specs
        specs = []
        audio_meta = read_audio_metadata(files[0])

        for filename, files in zip(filenames, partitions):
            specs.append(
                {
                    "filename": filename,
                    "attrs": {
                        "creation_date": (
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        ),
                    },
                    "data": {
                        "audio": {
                            "files": files,
                            "sample_len": (
                                audio_meta["num_samples_per_channel"]
                                if not vlen else None
                            ),
                            "dtype": dtype,
                            "attrs": {
                                "fs": audio_meta["fs"]
                            }
                        },
                    }
                }
            )

            # Add custom metadata
            if meta is not None:
                specs[-1]["attrs"].update(meta)
        
        return specs
    
    def create_partition_from_specs(
            self,
            idx: int,
            specs: dict, 
            args: Namespace
    ) -> Tuple[int, str]:
        # Create partition file
        h5_file = h5py.File(specs["filename"], "w")

        # Create data group
        data_group = h5_file.create_group("data")

        # Create dataset
        if specs["data"]["audio"]["sample_len"] is None:  # None = vlen
            dataset = data_group.create_dataset(
                name="audio",
                shape=(len(specs["files"]),),
                dtype=(
                    h5py.vlen_dtype(np.dtype(specs["data"]["audio"]["dtype"]))
                )
            )
        
        else:
            dataset = data_group.create_dataset(
                name="audio",
                shape=(
                    len(specs["data"]["audio"]["files"]),
                    specs["data"]["audio"]["sample_len"]
                ),
                dtype=specs["data"]["audio"]["dtype"]
            )

        # Add top level attributes
        for name, attr in specs["attrs"].items():
            h5_file.attrs[name] = attr

        # Add data attributes
        for name, attr in specs["data"]["audio"]["attrs"].items():
            dataset.attrs[name] = attr
        
        # Add partition data
        for idx, file in enumerate(
            tqdm(
                specs["data"]["audio"]["files"],
                desc=f"Writing partition #{idx}",
                colour="green" if args.workers == 1 else "cyan",
                leave=False,
                unit="file",
                position=idx
            )
        ):
            # NOTE: Files have already been validated at this point so there is
            # no need for additional assertions
            data, _ = read_audio(file, dtype=specs["data"]["audio"]["dtype"])

            if specs["data"]["audio"]["sample_len"] is None:  # vlen
                dataset[idx] = data
            
            else:
                dataset[idx, :] = data

        # Close file
        h5_file.close()
        return idx, specs["filename"]
    
    def create_trace(self, files: List[str], dtype: str) -> pl.DataFrame:
        df = pl.DataFrame(
            {
                "filename": pl.Series([], dtype=pl.Utf8),
                "data_sha256": pl.Series([], dtype=pl.Utf8),
                "fs": pl.Series([], dtype=pl.Int64),
                "subtype": pl.Series([], dtype=pl.Utf8) 
            }
        )

        for file in tqdm(
                files,
                desc="Creating trace",
                leave=False,
                unit="file",
                colour="green"
        ):
            filename = os.path.basename(file)

            if filename in df["filename"].to_list():
                exit_error(
                    "Filenames must be unique in order to generate a trace "
                    f"file. Found {filename=} in {file=} is a repeated "
                    "filename. Please solve repeated filenames either by "
                    "changing one of the names or by using --skip-trace if "
                    "this is expected and you do not need a trace file to "
                    "expand the resulting .h5 file later.",
                    writer=tqdm
                )
            
            meta = read_audio_metadata(file)
            audio, _ = read_audio(file, dtype=dtype)
            checksum = get_array_checksum(audio, hash="sha256")
            row = pl.DataFrame(
                {
                    "filename": filename,
                    "data_sha256": checksum,
                    "fs": meta["fs"],
                    "subtype": meta["subtype"] 
                }
            )
            df = pl.concat([df, row], how="vertical")

        return df
    
    def create_partitions(self, args: Namespace) -> None:
        # Assertions
        if args.meta is not None and len(args.meta) % 2 != 0:
            exit_error(
                "--meta should have an even number of elements where each "
                "odd value corresponds to a key and each even value "
                "corresponds to a value to be added as metadata"
            )
        
        start_time = perf_counter()

        if self.verbose:
            print(
                f"Collecting files from '{args.input}' ... This may take some "
                "time for large folders"
             )

        files = self.collect_data(
            input=args.input,
            root_dir=args.audio_root,
            col=args.audio_col,
            ext=args.extension,
            recursive=args.recursive
        )

        if self.verbose:
            print(f"{len(files)} file(s) found")
        
        if not args.skip_validation:
            if self.verbose:
                print("Validating files ...")
            
            self.validate_data(files, vlen=args.vlen)

            if self.verbose:
                print("Validation completed")
        
        else:
            if self.verbose:
                print_warning("Validation skipped (--skip-validation enabled)")

        # Create specifications for each partition
        if self.verbose:
            print(
                f"Creating partition specifications for {args.partitions} "
                "partition(s) ..."
            )

        partition_specs = self.create_partition_specs(
            files,
            output=args.output,
            num_partitions=args.partitions,
            dtype=args.dtype,
            meta=(
                dict_from_interleaved_list(args.meta) if args.meta is not None
                else None
            ),
            vlen=args.vlen
        )

        if self.verbose:
            print(
                f"{len(partition_specs)} partition specification(s) completed"
            )

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
                _, filename = self.create_partition_from_specs(
                    idx,
                    specs,
                    args
                )
                print(f"Partition #{idx} saved to '{filename}'")
        
        else:
            pool = Pool(
                processes=None if args.workers == 0 else args.workers,
                initargs=(RLock(),),
                initializer=tqdm.set_lock
            )
            jobs = [
                pool.apply_async(
                    self.create_partition_from_specs,
                    args=(idx, specs, args)
                )
                for idx, specs in enumerate(partition_specs)
            ]
            pool.close()
            results = [job.get() for job in jobs]

            for _, filename in results:
                tqdm.write(f"Partition saved to '{filename}'")

        # Generate virtual dataset
        ...

        # Generate trace file
        if not args.skip_trace:
            if self.verbose:
                print("Generating trace file ...")

            df = self.create_trace(files, dtype=args.dtype)
            trace_file = change_extension(args.output, new_ext=".csv")
            trace_file = add_suffix(trace_file, suffix="_trace")
            df.write_csv(trace_file)
            print(f"Trace file saved to '{trace_file}'")
        
        else:
            print_warning("Trace file skipped (--skip-trace-file enabled)")

        end_time = perf_counter()
        elapsed_time_repr = time_to_str(end_time - start_time, abbrev=False)
        print(f"Task completed in {elapsed_time_repr}")
