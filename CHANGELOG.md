# Changelog

## h5pack 1.3.0

### New features
- New `h5pack init` command to create a `h5pack.yaml` configuration file from a `.csv` file. It guesses a parser for each column (audio, lists, categorical values, text and numbers).
- New `h5pack show` command to show the content of one or more rows (e.g. `-r 42`, `-r -1` or `-r 10:20`). Audio can be saved as `.wav` files with `-s/--save` or played with `-p/--play`.
- New `h5pack verify` command to compare packed audio with the original audio files, sample by sample. It exits with a non-zero code if any audio does not match.
- New Python API to read packed data: `h5pack.open("dataset.h5")` returns each row as a `dict` with decoded values. It works with partitions and virtual datasets and can be safely used with PyTorch `DataLoader` workers.
- New `as_audioflac` parser to store audio as FLAC (lossless), often reducing speech and music to about half of their size.
- New `as_categorical` parser to store labels, splits, speaker ids and other repeated values as integer codes.
- Audio parsers now support multichannel audio.
- Audio can be resampled when packing using `parser_args: {sample_rate: 16000}`. This requires the new optional `resample` extra (`pip install "h5pack[resample]"`).
- New `h5pack pack` options:
    - `--compression gzip|lzf` and `--compression-level` to compress fields.
    - `--dry-run` to validate the data and show the planned partitions without writing any file.
    - `--skip-filepaths` to skip storing the paths of the original audio files.
- All commands accept `-q/--quiet` and `-v/--verbose`.
- `h5pack` can also be run as `python -m h5pack`.

### Changes
- New command line output: one line per step with the elapsed time, the size of each created file, a single progress bar, and errors that explain their cause and how to fix them. Colors respect `NO_COLOR` and progress bars are hidden when the output is redirected to a file.
- `h5pack pack` now validates all columns before writing any file:
    - Columns exist and their parsers can be used with their data type.
    - Integers and lists fit in the selected data type.
    - There are no empty values.
    - Parsers and parser arguments are valid.
    - All audio files of a column have the same sample rate and number of channels.
- `h5pack pack` shows a warning when 16-bit audio files are stored with a floating point parser, which doubles their size without adding information.
- Each worker now receives only the rows of its own partitions, reducing memory usage when packing large `.csv` files with several workers.
- Audio fields now store `num_channels` and `source_dir` (folder of the original audio files, relative to the `.h5` file) attributes.
- New `h5pack info` layout.
- `tqdm` and `packaging` are no longer dependencies. New optional extras: `resample` (`soxr`) and `play` (`sounddevice`).

### Bug fixes
- Fixed `h5pack unpack` crashing with `polars` 2.0.
- Fixed `h5pack unpack` writing integer fields as decimal numbers (e.g. `3.0` instead of `3`).
- Fixed `h5pack unpack` crashing on fields packed with `as_audiofloat64`.
- Fixed text that looks like a number (e.g. `007`) losing its leading zeros when packing with `as_utf8str` or unpacking.

### Backward compatibility
- Existing `h5pack.yaml` files and command line options work as before.
- Files created with previous versions can be read, inspected and unpacked with this version. Files created with 1.2.0 can also be checked with `h5pack verify --source <audio-folder>`.
- Files created with this version using the parsers available in previous versions keep the same layout, so they can still be read by previous versions. Files using `as_audioflac`, `as_categorical` or multichannel audio require `h5pack` 1.3.0 or newer.

### Documentation
- New pages for `h5pack init`, `h5pack show` and `h5pack verify`.
- New guides: Parsers, Reading data in Python (including a PyTorch `Dataset` example) and Saving space.
- Updated Quickstart and command outputs.

### Development
- Added a test suite and a GitHub Actions workflow running it on Linux, macOS and Windows with Python 3.10 and 3.13.

## h5pack 1.2.0

### Bug fixes
- Fixed `h5pack pack` crashing with a `KeyError` when using more than one worker (`-w/--workers`).
- Fixed `h5pack pack` hanging forever when a partition failed while using more than one worker. The error is now reported and the command exits with a non-zero code.
- Workers are now started with the `spawn` method on all platforms. This avoids deadlocks on Linux caused by `fork`.
- Fixed progress bars only advancing for the last field of each partition.
- Fixed virtual datasets returning empty data when opened from a folder other than the one used to create them. Partition paths are now stored relative to the virtual file.
- Fixed partitions being added to virtual datasets in completion order instead of row order when using multiple workers.
- Fixed `h5pack pack` failing at the checksum step when run from a folder other than the configuration file's folder.
- Fixed `h5pack pack` failing when a dataset has no `attrs` key in `h5pack.yaml`.
- Fixed audio files with the same name in different subfolders (e.g. `spk1/001.wav` and `spk2/001.wav`) overwriting each other when unpacking. Audio paths are now stored relative to the folder shared by all files of a field, and subfolders are preserved when unpacking.
- Fixed `h5pack unpack` writing invalid attributes to `h5pack.yaml` when unpacking a virtual dataset.
- Fixed user attributes being able to overwrite the reserved `producer` and `creation_date` attributes.
- Fixed `-p/--partitions` and `-f/--files-per-partition` being accepted together.
- Fixed `h5pack info` printing group members instead of group attributes, and reporting virtual dataset sources as missing when run from another folder.
- Fixed `--version` matching partial arguments.
- Incomplete partition files are now removed when a partition fails.
- Terminal escape codes are no longer printed when the output is redirected to a file.

### Changes
- `h5pack checksum` now exits with a non-zero code when one or more files fail verification.
- Python 3.10 or newer is now required. Supported Python versions are now listed in the package metadata.
- Type hints updated to built-in generics and `X | None` syntax.
- Added `ruff` linting (line length 79) and a lint workflow for GitHub Actions.

### Documentation
- New logo and color scheme for light and dark themes.
- Added GitHub repository information (version, stars, forks) and footer links to the documentation site.
- Added logo and badges to the README.
- Documented the Python 3.10 requirement, worker memory usage, `h5pack unpack` output layout, virtual dataset paths, and `h5pack checksum` exit codes.
