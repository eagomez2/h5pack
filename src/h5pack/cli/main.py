import sys
import argparse
from datetime import datetime
from h5pack import __version__
from .utils import (
    cmd_checksum,
    cmd_create,
    cmd_extract,
    cmd_info,
    cmd_virtual
)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="create/expand/inspect hierarchical data format datasets",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    subparser = parser.add_subparsers(dest="action")

    # Create parser
    create_parser = subparser.add_parser(
        "create",
        description="create HDF5 datasets",
        help="create HDF5 datasets",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    create_parser.add_argument(
        "-i", "--input",
        type=str,
        required=True,
        help=".yaml configuration file containing dataset specifications"
    )
    create_parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="output HDF5 partition file(s) with .h5 extension"
    ) 
    create_parser.add_argument(
        "-p", "--partitions",
        type=int,
        default=1,
        help="number of partitions to generate"
    )
    create_parser.add_argument(
        "-d", "--dataset",
        type=str,
        help="dataset name if .json configuration contains many"
    )
    create_parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="skip validating files before generating the partition(s)"
    )
    create_parser.add_argument(
        "--skip-virtual",
        action="store_true",
        help="skip generating a virtual layout when two or more partitions "
             "are created"
    )
    create_parser.add_argument(
        "--skip-checksum",
        action="store_true",
        help="skip generating the checksum file"
    )
    create_parser.add_argument(
        "-w", "--workers",
        type=int,
        default=0,
        help="number of workers (0 means 1 worker per core)"
    )
    create_parser.add_argument(
        "-u", "--unattended",
        action="store_true",
        help="unattended mode (no user prompts)"
    )
    create_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="verbose output"
    )

    # Virtual parser
    virtual_parser = subparser.add_parser(
        "virtual",
        description="create virtual HDF5 datasets",
        help="create virtual HDF5 datasets",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    virtual_parser.add_argument(
        "-i", "--input",
        type=str,
        required=True,
        nargs="+",
        help="input .h5 file(s) or folder(s) containing .h5 file(s)"
    )
    virtual_parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="output HDF5 file(s) with .h5 extension"
    )
    virtual_parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="search folders recursively"
    )
    virtual_parser.add_argument(
        "-a", "--attrs",
        type=str,
        nargs="+",
        metavar="KEY VALUE",
        help="top level attributes to write as a list of 'key' 'value' pairs"
    )
    virtual_pattr_parser = virtual_parser.add_mutually_exclusive_group()
    virtual_pattr_parser.add_argument(
        "-s", "--select",
        type=str,
        metavar="PATTERN",
        help="select pattern to filter out non-matching elements from --input"
    )
    virtual_pattr_parser.add_argument(
        "-e", "--exclude",
        type=str,
        metavar="PATTERN",
        help="exclude pattern to filter out matching elements from --input"
    )
    virtual_parser.add_argument(
        "-u", "--unattended",
        action="store_true",
        help="unattended mode (no user prompts)"
    )
    virtual_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="verbose output"
    )

    # Checksum parser
    checksum_parser = subparser.add_parser(
        "checksum",
        description="verify HDF5 datasets checksum",
        help="create virtual HDF5 datasets checksum",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    checksum_parser.add_argument(
        "input",
        type=str,
        nargs="+",
        help="checksum file"
    )
    checksum_parser.add_argument(
        "-g", "--generate",
        action="store_true",
        help="generate sha256 hash of input files"
    )
    checksum_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="verbose output"
    )

    # Info parser
    info_parser = subparser.add_parser(
        "info",
        description="inspect HDF5 datasets",
        help="inspect HDF5 datasets",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    info_parser.add_argument(
        "input",
        help="input .h5 file"
    )

    # Expand parder
    extract_parser = subparser.add_parser(
        "extract",
        description="extract HDF5 datasets into individual files",
        help="extract HDF5 datasets datasets into individual files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    extract_parser.add_argument(
        "input",
        help="input .h5 file"
    )
    extract_parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="output folder"
    )
    extract_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="verbose output"
    )

    return parser


def main() -> int:
    if (len(sys.argv) == 1
            or (len(sys.argv) == 2 and sys.argv[1] in ("--version"))
    ):
        print(
            f"h5pack version {__version__} 2024-{datetime.now().year} "
            "developed by Esteban Gómez"
        )
        sys.exit(0)
    
    parser = get_parser()
    args = parser.parse_args()

    if args.action == "create":
        cmd_create(args)
    
    elif args.action == "virtual":
        cmd_virtual(args)
    
    elif args.action == "checksum":
        cmd_checksum(args)
    
    elif args.action == "info":
        cmd_info(args)
    
    elif args.action == "extract":
        cmd_extract(args)
    
    else:
        raise AssertionError


if __name__ == "__main__":
    main()
