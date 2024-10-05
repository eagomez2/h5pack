import os
from argparse import Namespace
from typing import (
    List,
    Optional,
    Tuple,
    Union
)
from tqdm import tqdm
from .builder import DatasetBuilder
from ...core.display import print_warning
from ...core.guards import is_file_with_ext
from ...core.io import (
    get_dir_files,
    read_audio_metadata
)
from ...core.utils import (
    list_from_csv_col,
    list_from_tsv_col
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

    
    def create_partition_specs(self, *args, **kwargs) -> dict:
        ...
    
    def create_partition_from_specs(self, *args, **kwargs) -> Tuple[int, str]:
        ...
    
    def create_partitions(self, args: Namespace) -> None:
        if self.verbose:
            print(
                f"Collecting files from '{args.input}' ... This may take some "
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
