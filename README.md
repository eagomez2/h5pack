# h5pack
`h5pack` is a utility made to create, analyze and expand HDF5 audio datasets using data and annotations of various sources. HDF5 is an open-source file format for storing large, complex data. It uses a directory-like structure to organize data within the file. Click [here](https://www.hdfgroup.org/solutions/hdf5/) to learn more about HDF5. Among other, HDF5 provides:

- Efficient storage: Handles large datasets with chunking and compression, reducing disk space usage.
- Fast I/O: Optimized for rapid reading/writing of multi-dimensional data, improving data loading speeds.
- Hierarhical structures: Organizes metadata, features, and audio data efficiently within a single file.
- Concurrent access: Supports concurrent access, enabling efficient multi-threaded data loading.
- Scalability: Handles massive datasets without performance degradation, making it ideal for large-scale machine learning tasks.
- Cross-platform: Works across different OS and programming languages, ensuring flexibility.

`h5pack` was made to go from raw files to HDF5 files and back in a robust, consistent and simple way. It provides a collection of tools to facilitate all the necessary tasks to make it possible:

- `h5pack create`: Converts raw audio and/or annotation files into an HDF5 file.
- `h5pack extract`: Extracts raw data from an HDF5 file, allowing regeneration of the original input data.
- `h5pack virtual`: Creates a virtual dataset by combining multiple datasets into a single logical dataset without duplication, enabling seamless access to fragmented or distributed data.
- `h5pack info`: Displays the contents of an HDF5 file generated with `h5pack`, providing a quick overview of its structure.
- `h5pack checksum`: Verifies the integrity of an HDF5 file by checking its checksum to detect potential corruption.

# Table of contents
- [Installation](#installation)
  - [Install through pip](#install-through-pip)
  - [Install through uv](#install-through-uvx)
  - [Install in developer mode](#install-in-developer-mode)
- [Quickstart](#quickstart)
- [Documentation](#documentation)
- [Cite](#cite)
- [License](#license)

# Installation
The following section shows different ways to install `h5pack`.

## Install through `pip` 
To install `h5pack`, run:
```bash
pip install h5pack
```

Verify the installation with:
```bash
h5pack --version
```

This should output:
```bash
h5pack version x.y.z yyyy-zzzz developed by Esteban Gómez
```
Where:
- `x.y.z` represents the major, minor, and patch version
- `yyyy-zzzz` indicates the development start year and the current

## Install through `uv`
Alternatively, you can install the tool using `uv`. This is adequate for when you can to keep it isolated from your python environment setup and just run it to analyze a certain data collection.

1. Install `uv` and `uvx` following the instructions for your operating system in [`uv` website](https://docs.astral.sh/uv/getting-started/installation/).
2. Run:
```bash
uv tool install h5pack
```

3. Verify the installation with
```bash
uv tool run h5pack --version
```

or you can use the shortcut version `uvx`:

```bash
uvx h5pack --version
```

This should output:
```bash
h5pack version x.y.z yyyy-zzzz developed by Esteban Gómez
```
Where:
- `x.y.z` represents the major, minor, and patch version
- `yyyy-zzzz` indicates the development start year and the current


## Install in developer mode
Developer mode installation is intended for those developing new features for the tool. To set it up:
1. Clone the repository to your desired folder using:
```bash
git clone <repository_url>
```
2. Navigate to the root directory (where `pyproject.toml` is located):
```bash
cd <repository_folder>
```
3. Install in developer mode with:
```bash
python -m flit install -s
```
This will allow immediate reflection of any code modifications when the tool is executed in the terminal.

Before proceeding, ensure that Flit is installed. If not, install it with:
```bash
python -m pip install flit
```
For more information on `flit`, refer to the [Flit Command Line Interface documentation](https://flit.pypa.io/en/stable/).

# Quickstart

# Documentation

## Example `.yaml` file
```yaml
datasets:
  first_example_dataset:
    attrs:
      author: &author_name "Esteban Gómez"
      description: "First dataset description"
      version: &version "0.0.1"

    data:
      file: "first_dataset.csv"
      fields:
        audio:
          column: "file"
          parser: "as_audioint16"
  
  second_example_dataset:
    attrs:
      author: *author_name
      description: "Second dataset description"
      version: *version
    
    data:
      file: "second_dataset.csv"
      fields:
        audio:
          column: "file"
          parser: "as_audioint16"
```

- `attrs` can have any string attributes.
- `data` needs a `.csv` in the `file` key and every field in `fields` needs a `column` and a `parser` description.
- Multiple datasets can be added in a single file under the `datasets` key.

# Cite
If this package contributed to your work, please consider citing it:

```
@misc{h5pack,
  author = {Esteban Gómez},
  title  = {h5pack},
  year   = 2024,
  url    = {https://github.com/eagomez2/h5pack}
}
```

This package was developed by <a href="https://estebangomez.me/" target="_blank">Esteban Gómez</a>, member of the <a href="https://www.aalto.fi/en/department-of-information-and-communications-engineering/speech-interaction-technology" target="_blank">Speech Interaction Technology group from Aalto University</a>.

# License
For further details about the license of this package, please see [LICENSE](LICENSE).
