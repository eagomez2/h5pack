import os
import json
import polars as pl
from argparse import Namespace
from ..core.guards import is_file_with_ext
from ..core.display import exit_error
from ..data import (
    get_parsers_map,
    get_validators_map
)
from ..data.validators import validate_attrs


def add_native_attrs(specs: dict) -> None:
    # Add built-in metadata to specs
    ...


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

    # Parse data
    if args.verbose:
        print("Parsing data ...")

    data_df = pl.read_csv(data_file, has_header=True)

    for field_name, field_data in data["fields"].items():
        col_name = data["fields"][field_name]["column"]
        parser_name = data["fields"][field_name]["parser"]

        if col_name not in data_df.columns:
            exit_error(
                f"Column '{col_name}' of field '{field_name}' not found in "
                f"file '{data_file}'"
            )

        parser = get_parsers_map()[data_df[col_name].dtype].get(
            parser_name, None
        )

        if parser is None:
            exit_error(
                f"Invalid parser '{parser_name}' for column '{col_name}'"
            )
        
        # Validate data
        validators = get_validators_map().get(parser_name, None)

        if validators is not None:
            for validator in validators:
                validator(data_df, col=col_name)

        # Parse data 
        data["fields"][field_name].update(parser(data_df, col=col_name))

    # Check columns length
    ...

    # Add native attrs
    ...

    # Parse data
    ...
    
    import pdb;pdb.set_trace()

    # Create dataset
    ...

    # Create virtual layout
    ...

    # Create trace or just call it checksum_{col}?
    ...
