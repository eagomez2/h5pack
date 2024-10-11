import sys
import argparse
from h5pack import __version__
from .utils import cmd_create


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
        help=".json configuration file containing dataset specifications"
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

    return parser


def main() -> int:
    if (len(sys.argv) == 1
            or (len(sys.argv) == 2 and sys.argv[1] in ("--version"))
    ):
        print(f"h5pack version {__version__} developed by Esteban Gómez")
        sys.exit(0)
    
    parser = get_parser()
    args = parser.parse_args()

    if args.action == "create":
        cmd_create(args)
    
    else:
        raise AssertionError


if __name__ == "__main__":
    main()
