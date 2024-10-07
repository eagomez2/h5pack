import os
import polars as pl
from time import perf_counter
from argparse import Namespace
from typing import (
    List,
    Optional,
    Tuple
)
from .builder import DatasetBuilder
from ...core.display import exit_error
from ...core.guards import has_ext
from ...core.utils import (
    list_from_csv_col,
    list_from_tsv_col
)


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
    
    def validate_data(self, *args, **kwargs) -> None:
        ...
    
    def create_partition_specs(self, *args, **kwargs) -> dict:
        ...

    def create_partition_from_specs(self, *args, **kwargs) -> Tuple[int, str]:
        ...
    
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
            
        files, metrics = self.collect_data(
            input=args.input,
            audio_col=args.audio_col,
            metric_col=args.metric_col,
            root_dir=args.audio_root
        )
