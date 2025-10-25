import sys
import argparse
from datetime import datetime
from importlib.metadata import version
from .checksum import cmd_checksum
from .info import cmd_info
from .pack import cmd_pack
from .unpack import cmd_unpack
from .virtual import cmd_virtual


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="pack/unpack/inspect hierarchical data format datasets",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    subparser = parser.add_subparsers(dest="action")

    # Pack parser
    pack_parser = subparser.add_parser(
        "pack",
        description="pack data into HDF5 dataset files",
        help="pack data into HDF5 dataset files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    pack_parser.add_argument(
        "-c", "--config",
        type=str,
        default="h5pack.yaml",
        help=".yaml configuration file containing dataset specifications"
    )
    pack_parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="output HDF5 partition file(s) with .h5 extension"
    ) 
    pack_partitions_parser = pack_parser.add_mutually_exclusive_group()
    pack_partitions_parser.add_argument(
        "-p", "--partitions",
        type=int,
        default=1,
        help="number of partitions to create"
    )
    pack_parser.add_argument(
        "-f", "--files-per-partition",
        type=int,
        help="number of files per partition"
    )
    pack_parser.add_argument(
        "-d", "--dataset",
        type=str,
        required=True,
        help="name of the dataset to generate"
    )
    pack_parser.add_argument(
        "--create-virtual",
        action="store_true",
        help="create a virtual layout when two or more partitions are created"
    )
    pack_parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="skip validating files before generating the partition(s)"
    )
    pack_parser.add_argument(
        "--skip-checksum",
        action="store_true",
        help="skip generating the checksum file"
    )
    pack_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="allow overwriting existing files"
    )
    pack_parser.add_argument(
        "-w", "--workers",
        type=int,
        default=1,
        help="number of workers (0 means 1 worker per core)"
    )
    pack_parser.add_argument(
        "-u", "--unattended",
        action="store_true",
        help="unattended mode (no user prompts)"
    )

    # Unpack parser
    unpack_parser = subparser.add_parser(
        "unpack",
        description="unpack HDF5 datasets into individual files",
        help="unpack HDF5 datasets datasets into individual files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    unpack_parser.add_argument(
        "input",
        help="input .h5 file"
    )
    unpack_parser.add_argument(
        "-o", "--output",
        type=str,
        help="output folder"
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
        "input",
        type=str,
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
        help="select pattern include matching elements from --input"
    )
    virtual_pattr_parser.add_argument(
        "-f", "--filter",
        type=str,
        metavar="PATTERN",
        help="filter pattern to remove matching elements from --input"
    )
    virtual_parser.add_argument(
        "--force-abspath",
        action="store_true",
        help="force all embedded paths to be absolute paths"
    )
    virtual_parser.add_argument(
        "-u", "--unattended",
        action="store_true",
        help="unattended mode (no user prompts)"
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

    # Checksum parser
    checksum_parser = subparser.add_parser(
        "checksum",
        help="create/verify virtual HDF5 datasets checksum",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    checksum_parser.add_argument(
        "input",
        type=str,
        help=".sha256 file (to verify) or file or folder (to calculate)"
    )
    checksum_parser.add_argument(
        "--save",
        type=str,
        help="save calculated checksum to a .sha256 file"
    )
    checksum_parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="search folders recursively if input is a folder"
    )

    return parser


def main() -> int:
    if (
        len(sys.argv) == 1
        or (len(sys.argv) == 2 and sys.argv[1] in ("--version"))
    ):
        print(
            f"h5pack version {version('h5pack')} 2024-{datetime.now().year} "
            "developed by Esteban Gómez (Speech Interaction Technology, Aalto "
            "University)"
        )
        sys.exit(0)
    
    parser = get_parser()
    args = parser.parse_args()

    if args.action == "pack":
        cmd_pack(args)
    
    elif args.action == "virtual":
        cmd_virtual(args)
    
    elif args.action == "checksum":
        cmd_checksum(args)
    
    elif args.action == "info":
        cmd_info(args)
    
    elif args.action == "unpack":
        cmd_unpack(args)
    
    else:
        raise AssertionError
