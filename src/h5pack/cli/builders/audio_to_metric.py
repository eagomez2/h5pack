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
    Tuple
)
from tqdm import tqdm
from multiprocessing import (
    Pool,
    RLock
)
from .builder import DatasetBuilder
from ...core.display import (
    ask_confirmation,
    exit_error,
    print_warning
)
from ...core.guards import (
    has_ext,
    is_file_with_ext
)
from ...core.io import (
    add_extension,
    add_suffix,
    read_audio,
    read_audio_metadata
)
from ...core.utils import (
    dict_from_interleaved_list,
    list_from_csv_col,
    list_from_tsv_col,
    make_list
)
from h5pack import __version__


class AudioToMetricDatasetBuilder(DatasetBuilder):
    def __init__(self, verbose: bool = False) -> None:
        super().__init__()

        # Params
        self.verbose = verbose

    def collect_data(
            self,
            input: str,
            audio_col: str,
            metric_col: str,
            root_dir: Optional[str] = None,
    ) -> Tuple[List[str], List[float]]:
        # NOTE: At this point the file is already a .csv or .tsv file
        if has_ext(input, ".csv"):
            try:
                files = list_from_csv_col(input, col=audio_col)
            
            except pl.exceptions.ColumnNotFoundError:
                exit_error(f"Column '{audio_col}' not found in '{input}'")
            
            try:
                metrics = list_from_csv_col(input, col=metric_col)
            
            except pl.exceptions.ColumnNotFoundError:
                exit_error(f"Column '{metric_col}' not found in '{input}'")
        
        elif has_ext(input, ".tsv"):
            try:
                files = list_from_tsv_col(input, col=audio_col)
            
            except pl.exceptions.ColumnNotFoundError:
                exit_error(f"Column '{audio_col}' not found in '{input}'")
            
            try:
                metrics = list_from_tsv_col(input, col=metric_col)
            
            except pl.exceptions.ColumnNotFoundError:
                exit_error(f"Column '{metric_col}' not found in '{input}'") 
        
        else:
            exit_error(f"Invalid input file or folder '{input}'")
        
        if root_dir is not None:
            files = [os.path.join(root_dir, os.path.relpath(f)) for f in files]
        
        if len(files) == 0:
            exit_error(f"0 files found in column '{audio_col}' of '{input}'")
        
        if len(files) != len(metrics):
            exit_error(
                f"Column '{audio_col}' should have the same number of rows "
                f"than column '{metric_col}', but {len(files)} and "
                f"{len(metrics)} were found, respectively"
            )
        
        return files, metrics
    
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
                exit_error(f"Invalid file '{file}'", writer=tqdm)

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
                        "All files should have the same length. Previous files"
                        f" have sample_len={observed_lens[0]}, but '{file}' "
                        "has sample_len="
                        f"{audio_meta['num_samples_per_channel']}. If you "
                        "intend to write a variable length .h5 files, please "
                        "use the --vlen option",
                        writer=tqdm
                    )
    
    def create_partition_specs(
            self,
            files: List[str],
            metrics: List[str],
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
        
        # Split files and metrics
        partition_files = np.array_split(
            files,
            indices_or_sections=num_partitions
        )
        partition_metrics = np.array_split(
            metrics,
            indices_or_sections=num_partitions
        )

        # Form .h5 partition filenames
        if len(partition_files) == 1:
            filenames = make_list(add_extension(output, ext=".h5"))
        
        else:
            filenames = []
            zfill = len(str(len(partition_files)))

            for idx in range(len(partition_files)):
                filename = add_extension(output, ext=".h5")
                filename = add_suffix(
                    filename,
                    suffix=f".pt{str(idx).zfill(zfill)}"
                )
                filenames.append(filename)
        
        # Form partition specs
        specs = []
        audio_meta = read_audio_metadata(files[0])

        for filename, files, metrics in zip(
            filenames,
            partition_files,
            partition_metrics
        ):
            specs.append(
                {
                    "filename": filename,
                    "attrs": {
                        "creation_date": (
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        ),
                        "producer_id": f"h5pack {__version__}"
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
                        "metric": {
                            "values": metrics,
                            "dtype": metrics[0].dtype
                        }
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
            dataset_audio = data_group.create_dataset(
                name="audio",
                shape=(len(specs["data"]["audio"]["files"]),),
                dtype=(
                    h5py.vlen_dtype(np.dtype(specs["data"]["audio"]["dtype"]))
                )
            )
            dataset_metric = data_group.create_dataset(
                name="metric",
                shape=(len(specs["data"]["metric"]["values"]),),
                dtype=specs["data"]["metric"]["dtype"]
            )
        
        else:
            dataset_audio = data_group.create_dataset(
                name="audio",
                shape=(
                    len(specs["data"]["audio"]["files"]),
                    specs["data"]["audio"]["sample_len"]
                ),
                dtype=specs["data"]["audio"]["dtype"]
            )
            dataset_metric = data_group.create_dataset(
                name="metric",
                shape=(len(specs["data"]["metric"]["values"]),),
                dtype=specs["data"]["metric"]["dtype"]
            )
        
        # Add top level attributes
        for name, attr in specs["attrs"].items():
            h5_file.attrs[name] = attr

        # Add audio data attributes
        for name, attr in specs["data"]["audio"]["attrs"].items():
            dataset_audio.attrs[name] = attr
        
        # Add partition data
        for idx, (file, metric) in enumerate(
            tqdm(
                zip(
                    specs["data"]["audio"]["files"],
                    specs["data"]["metric"]["values"]
                ),
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
                dataset_audio[idx] = data
            
            else:
                dataset_audio[idx, :] = data
            
            dataset_metric[idx] = metric
        
        # Close file
        h5_file.close()
        return idx, specs["filename"]
    
    def create_virtual_dataset_from_partitions(
            self,
            file: str,
            partitions: List[str],
            meta: Optional[dict] = None 
    ) -> h5py.VirtualLayout:
        # Check all partition files exist
        for p in partitions:
            if not is_file_with_ext(p, ext=".h5"):
                exit_error(f"Invalid partition file '{p}'")
        
        # NOTE: Specs is a list because this allows doing a virtual dataset
        # where the same file is repeated twice. It can allow quickly creating
        # many-to-one dataset pairs
        specs = []
        accum_idx = 0

        # Get specs from partition files
        # NOTE: This specs are not the same used to generate files
        for partition in partitions:
            with h5py.File(partition) as f:
                specs.append(
                    {
                        "file": partition,
                        "audio_shape": f["data"]["audio"].shape,
                        "audio_dtype": f["data"]["audio"].dtype,
                        "metric_shape": f["data"]["metric"].shape,
                        "metric_dtype": f["data"]["metric"].dtype,
                        "start_idx": accum_idx,
                        "end_idx": accum_idx + f["data"]["audio"].shape[0]
                    }
                )

                accum_idx += f["data"]["audio"].shape[0]
        
        # Calculate virtual layout shape based on first partition specs
        audio_sample_len = specs[0]["audio_shape"][-1]
        audio_dtype = specs[0]["audio_dtype"]
        audio_shape = (
            sum([p["audio_shape"][0] for p in specs]), audio_sample_len
        )
        metric_dtype = specs[0]["metric_dtype"]
        metric_shape = sum([p["metric_shape"][0] for p in specs])

        # Create virtual layouts
        audio_layout = h5py.VirtualLayout(shape=audio_shape, dtype=audio_dtype)
        metric_layout = h5py.VirtualLayout(
            shape=metric_shape,
            dtype=metric_dtype
        )

        for idx, partition in enumerate(partitions):
            src_audio = h5py.VirtualSource(
                partition,
                name="data/audio",
                shape=specs[idx]["audio_shape"],
                dtype=audio_dtype
            )
            src_metric = h5py.VirtualSource(
                partition,
                name="data/metric",
                shape=specs[idx]["metric_shape"],
                dtype=metric_dtype
            )
            start_idx = specs[idx]["start_idx"]
            end_idx = specs[idx]["end_idx"]

            audio_layout[start_idx:end_idx, :] = src_audio
            metric_layout[start_idx:end_idx] = src_metric
        
        virtual_dataset_file = add_extension(file, ext=".h5")

        with h5py.File(virtual_dataset_file, "w", libver="latest") as h5_file:
            # Create data group
            data_group = h5_file.create_group("data")

            # Create virtual datasets
            data_group.create_virtual_dataset(
                name="audio",
                layout=audio_layout
            )
            data_group.create_virtual_dataset(
                name="metric",
                layout=metric_layout
            )

            # Add metadata
            h5_file.attrs["creation_date"] = (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            h5_file.attrs["producer_id"] = f"h5pack {__version__}"
            origin = [os.path.basename(p) for p in partitions]
            h5_file.attrs["source"] = ", ".join(origin)

            # Add custom metadata
            if meta is not None:
                for k, v in meta.items():
                    h5_file.attrs[k] = v

    def create_partitions(self, args: Namespace) -> None:
        # Assertions
        if args.meta is not None and len(args.meta) % 2 != 0:
            exit_error(
                "--meta should have an even number of elements where each "
                "odd value corresponds to a key and each even value "
                "corresponds to a value to be added as metadata"
            )
        
        if not has_ext(args.input, ext=[".csv", ".tsv"]):
            exit_error(
                f"If --type is '{args.type}' --input should a .csv or .tsv "
                "file"
            )
        
        if not os.path.isfile(args.input):
            exit_error(f"Invalid input file '{args.input}'")
        
        if args.audio_col is None:
            exit_error(
                f"If --type if '{args.type}' --audio-col should be defined"
            )
        
        if args.metric_col is None:
            exit_error(
                f"If --type if '{args.type}' --metric-col should be defined"
            )
        
        start_time = perf_counter()

        if self.verbose:
            print(
                f"Collecting files from '{args.input}' ... This may take some "
                "time for large folders"
             )
            
        # Collect data
        files, metrics = self.collect_data(
            input=args.input,
            audio_col=args.audio_col,
            metric_col=args.metric_col,
            root_dir=args.audio_root
        )

        if self.verbose:
            print(f"{len(files)} row(s) found")
        
        if not args.skip_validation:
            if self.verbose:
                print("Validating files ...")
            
            # NOTE: No need to validate metrics as these are only values
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
            metrics,
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
        if not args.skip_virtual_layout and args.partitions > 1:
            if self.verbose:
                print("Creating virtual layout ...")

            partition_filenames = [
                spec["filename"] for spec in partition_specs
            ]
            self.create_virtual_dataset_from_partitions(
                file=args.output,
                partitions=partition_filenames,
                meta=(
                    dict_from_interleaved_list(args.meta)
                    if args.meta is not None else None
                )
            )
            print(f"Virtual layout saved to '{args.output}'")

        # Generate trace file
        ...
