import sys
import argparse
from datetime import datetime
from importlib.metadata import version
from ..core.display import (
    NORMAL,
    QUIET,
    VERBOSE,
    set_verbosity
)
from .checksum import cmd_checksum
from .info import cmd_info
from .init import cmd_init
from .pack import cmd_pack
from .show import cmd_show
from .unpack import cmd_unpack
from .verify import cmd_verify
from .virtual import cmd_virtual


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="pack/unpack/inspect hierarchical data format datasets",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
    )
    subparser = parser.add_subparsers(dest="action")

    # Options shared by all tools
    common_parser = argparse.ArgumentParser(add_help=False)
    verbosity_parser = common_parser.add_mutually_exclusive_group()
    verbosity_parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="only print warnings and errors"
    )
    verbosity_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="print additional information"
    )

    # Pack parser
    pack_parser = subparser.add_parser(
        "pack",
        description="pack data into HDF5 dataset files",
        help="pack data into HDF5 dataset files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False,
        parents=[common_parser]
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
    pack_partitions_parser.add_argument(
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
    pack_parser.add_argument(
        "--compression",
        type=str,
        choices=["none", "gzip", "lzf"],
        default="none",
        help="compression applied to fixed-size data fields"
    )
    pack_parser.add_argument(
        "--compression-level",
        type=int,
        default=4,
        choices=range(10),
        metavar="[0-9]",
        help="gzip compression level"
    )
    pack_parser.add_argument(
        "--skip-filepaths",
        action="store_true",
        help="do not store the original path of audio files"
    )
    pack_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate data and show the planned partitions without writing"
    )

    # Unpack parser
    unpack_parser = subparser.add_parser(
        "unpack",
        description="unpack HDF5 datasets into individual files",
        help="unpack HDF5 datasets datasets into individual files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False,
        parents=[common_parser]
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
        allow_abbrev=False,
        parents=[common_parser]
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
        allow_abbrev=False,
        parents=[common_parser]
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
        allow_abbrev=False,
        parents=[common_parser]
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

    # Init parser
    init_parser = subparser.add_parser(
        "init",
        description="create a h5pack.yaml configuration file from a .csv file",
        help="create a h5pack.yaml configuration file from a .csv file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False,
        parents=[common_parser]
    )
    init_parser.add_argument(
        "input",
        type=str,
        help="input .csv file"
    )
    init_parser.add_argument(
        "-o", "--output",
        type=str,
        default="h5pack.yaml",
        help="output .yaml configuration file"
    )
    init_parser.add_argument(
        "-d", "--dataset",
        type=str,
        help="name of the dataset (defaults to the .csv filename)"
    )
    init_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="allow overwriting an existing configuration file"
    )

    # Show parser
    show_parser = subparser.add_parser(
        "show",
        description="show, save or play the data of one or more rows",
        help="show, save or play the data of one or more rows",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False,
        parents=[common_parser]
    )
    show_parser.add_argument(
        "input",
        type=str,
        help="input .h5 file"
    )
    show_parser.add_argument(
        "-r", "--rows",
        type=str,
        default="0",
        help="row index or range of rows (e.g. 42, 10:20, -1 or =-3:)"
    )
    show_parser.add_argument(
        "-f", "--fields",
        type=str,
        nargs="+",
        help="fields to show (defaults to all fields)"
    )
    show_parser.add_argument(
        "-s", "--save",
        type=str,
        metavar="FOLDER",
        help="save the audio of the selected rows to a folder"
    )
    show_parser.add_argument(
        "-p", "--play",
        action="store_true",
        help="play the audio of the selected rows (requires sounddevice)"
    )

    # Verify parser
    verify_parser = subparser.add_parser(
        "verify",
        description="verify packed audio against the original audio files",
        help="verify packed audio against the original audio files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False,
        parents=[common_parser]
    )
    verify_parser.add_argument(
        "input",
        type=str,
        help="input .h5 file"
    )
    verify_parser_rows = verify_parser.add_mutually_exclusive_group()
    verify_parser_rows.add_argument(
        "-n", "--num-rows",
        type=int,
        default=20,
        help="number of random rows to verify"
    )
    verify_parser_rows.add_argument(
        "-a", "--all",
        action="store_true",
        help="verify all rows"
    )
    verify_parser.add_argument(
        "--source",
        type=str,
        help="folder containing the original audio files (defaults to the "
        "folder stored in the .h5 file)"
    )
    verify_parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="seed used to select random rows"
    )

    return parser


def main() -> int:
    if (
        len(sys.argv) == 1
        or (len(sys.argv) == 2 and sys.argv[1] in ("--version",))
    ):
        print(
            f"h5pack version {version('h5pack')} 2024-{datetime.now().year} "
            "developed by Esteban Gómez (Speech Interaction Technology, Aalto "
            "University)"
        )
        sys.exit(0)
    
    parser = get_parser()
    args = parser.parse_args()

    if args.action is None:
        parser.print_help()
        sys.exit(0)

    if args.quiet:
        set_verbosity(QUIET)

    elif args.verbose:
        set_verbosity(VERBOSE)

    else:
        set_verbosity(NORMAL)

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

    elif args.action == "init":
        cmd_init(args)

    elif args.action == "show":
        cmd_show(args)

    elif args.action == "verify":
        cmd_verify(args)
    
    else:
        raise AssertionError
