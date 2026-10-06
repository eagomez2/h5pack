# User guide
This section documents every `h5pack` command and explains how your data is stored and read. If you're new to `h5pack`, we recommend starting with our [Quickstart](quickstart.md) guide.

## Commands
- [`h5pack init`](init.md): Create a configuration file from a `.csv` file.
- [`h5pack pack`](pack.md): Pack audio and annotations into `.h5` files.
- [`h5pack unpack`](unpack.md): Extract the original files from a `.h5` file.
- [`h5pack info`](info.md): Inspect the structure and attributes of a `.h5` file.
- [`h5pack show`](show.md): Show, save or play the data of any row.
- [`h5pack virtual`](virtual.md): Combine several `.h5` files into a virtual dataset.
- [`h5pack checksum`](checksum.md): Create or check the checksums of `.h5` files.

## Working with your data
- [Parsers](parsers.md): All available parsers and how data is stored.
- [Reading data in Python](reading.md): Python API and PyTorch example.
- [Saving space](space.md): FLAC, compression and choosing data types.
