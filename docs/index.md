# Welcome to h5pack

[![PyPI version](https://img.shields.io/pypi/v/h5pack?color=0f172a)](https://pypi.org/project/h5pack/)
[![Python versions](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Feagomez2%2Fh5pack%2Fmain%2Fpyproject.toml&color=0f172a)](https://pypi.org/project/h5pack/)
[![Docs](https://img.shields.io/badge/docs-eagomez2.github.io%2Fh5pack-0284c7)](https://eagomez2.github.io/h5pack/)
[![License](https://img.shields.io/github/license/eagomez2/h5pack?color=0f172a)](license.md)

`h5pack` is a utility made to pack, analyze and unpack HDF5 audio datasets using data and annotations of various sources. HDF5 is an open-source file format for storing large, complex data. It uses a directory-like structure to organize data within the file. Click [here](https://www.hdfgroup.org/solutions/hdf5/) to learn more about HDF5. Among other, HDF5 provides:

- Efficient storage: Handles large datasets with chunking and compression, reducing disk space usage.
- Fast I/O: Optimized for rapid reading/writing of multi-dimensional data, improving data loading speeds.
- Hierarhical structures: Organizes metadata, features, and audio data efficiently within a single file.
- Concurrent access: Supports concurrent access, enabling efficient multi-threaded data loading.
- Scalability: Handles massive datasets without performance degradation, making it ideal for large-scale machine learning tasks.
- Cross-platform: Works across different OS and programming languages, ensuring flexibility.

`h5pack` was made to go from raw files to HDF5 files and back in a robust, consistent and simple way. It provides a collection of tools to facilitate all the necessary tasks to make it possible:

- `h5pack init`: Creates a configuration file from an annotations `.csv` file.
- `h5pack pack`: Converts raw data and/or annotation files into an HDF5 file.
- `h5pack unpack`: Extracts raw data from an HDF5 file, allowing regeneration of the original input data.
- `h5pack virtual`: Creates a virtual dataset by combining multiple datasets into a single logical dataset without duplication, enabling seamless access to fragmented or distributed data.
- `h5pack info`: Displays the contents of an HDF5 file generated with `h5pack`, providing a quick overview of its structure.
- `h5pack checksum`: Verifies the integrity of an HDF5 file by checking its checksum to detect potential corruption.
- `h5pack show`: Shows, saves or plays the data of any row of an HDF5 file.
- `h5pack verify`: Compares the packed audio with the original audio files.

Packed data can be read in Python with `h5pack.open()`, which also works with PyTorch `DataLoader` workers.


## Guides
To begin using `h5pack`, refer to the guides below:

- [Installation](install.md)
- [Quickstart](quickstart.md)
- [`h5pack init` documentation](init.md)
- [`h5pack pack` documentation](pack.md)
- [`h5pack unpack` documentation](unpack.md)
- [`h5pack info` documentation](info.md)
- [`h5pack virtual` documentation](virtual.md)
- [`h5pack checksum` documentation](checksum.md)
- [`h5pack show` documentation](show.md)
- [`h5pack verify` documentation](verify.md)
- [Parsers](parsers.md)
- [Reading data in Python](reading.md)
- [Saving space](space.md)
