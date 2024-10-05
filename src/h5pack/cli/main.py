import sys
import argparse
from h5pack import __version__
from .create.audio_builder import AudioDatasetBuilder


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
        help="input folder containing audio files of tabular data in .csv or "
             ".tsv format containing file and metrics map"
    )
    create_parser.add_argument(
        "-t", "--type",
        type=str,
        choices=["audio", "audio->metric"],
        default="audio",
        help="layout preset to use"
    )
    create_parser.add_argument(
        "--audio-col",
        type=str,
        help="column containing audio files (required if --input is .csv or "
             ".tsv file)"
    )
    create_parser.add_argument(
        "--audio-root",
        type=str,
        help="root folder to be added to --audio-col (only valid if --input is"
             " a .csv or .tsv file)"
    )
    create_parser.add_argument(
        "--metric-col",
        type=str,
        help="column containing metric values (required if --type is "
             "audio->metric)"
    )
    create_parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="output HDF5 partition file(s) with .h5 extension"
    ) 
    create_parser.add_argument(
        "-e", "--extension",
        type=str,
        nargs="+",
        default=[".wav"],
        help="audio file extension(s)"
    )
    create_parser.add_argument(
        "-p", "--partitions",
        type=int,
        default=1,
        help="number of partitions to generate"
    )
    create_parser.add_argument(
        "-d", "--dtype",
        type=str,
        choices=["float32", "float64", "int16"],
        default="float32",
        help="data type used to write the dataset"
    )
    create_parser.add_argument(
        "-m", "--meta",
        nargs="*",
        help="key/value pairs to be added as metadata"
    )
    create_parser.add_argument(
        "--vlen",
        action="store_true",
        help="create a variable length dataset"
    )
    create_parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="search folders recursively (only valid if --input is a folder)"
    )
    create_parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="skip validating files before generating the partition(s)"
    )
    create_parser.add_argument(
        "--skip-virtual-layout",
        action="store_true",
        help="skip generating a virtual layout when two or more partitions "
             "are created"
    )
    create_parser.add_argument(
        "--skip-trace",
        action="store_true",
        help="skip generating a trace file in .csv format. This file can be "
             "used to expand the dataset back to the original set of files"
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

    # Info parser
    info_parser = subparser.add_parser(
        "info",
        description="inspect HDF5 datasets generated with this tool",
        help="inspect HDF5 datasets generated with this tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False
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
        if args.type == "audio":
            builder = AudioDatasetBuilder(verbose=args.verbose)
            builder.create_partitions(args)
        
        elif args.type == "audio->metric":
            raise NotImplementedError
    
    elif args.action == "info":
        raise NotImplementedError
    
    else:
        raise AssertionError


if __name__ == "__main__":
    main()
