# Welcome to h5pack
`h5pack` is a utility made to create, analyze and expand HDF5 audio datasets using data and annotations of various sources. HDF5 is an open-source file format for storing large, complex data. It uses a directory-like structure to organize data within the file. Click <a href="https://www.hdfgroup.org/solutions/hdf5/" target="_blank">here</a> to learn more about HDF5. Among other, HDF5 provides:

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

## Installation
### Install using `pip`
The easiest way to install `h5pack` is through `pip` by running:

```bash
pip install h5pack
```

After installing `h5pack` you can verify the installation by running:

```bash
h5pack --version
```

This should output:

```bash
h5pack version x.y.z yyyy-zzzz developed by Esteban Gómez
```

Where:

- `x.y.z` represents the major, minor, and patch version.
- `yyyy-zzzz` indicates the development start year and the current year.

### Install using `uv`
Alternatively, you can install the tool using `uv`. This is adequate for when you can to keep it isolated from your python environment setup and just run it to analyze a certain data collection.

First, install `uv` and `uvx` following the instructions for your operating system in <a href="https://docs.astral.sh/uv/getting-started/installation/" target="_blank">`uv` website</a>.

Then run:

```bash
uv tool install h5pack
```

You can verify the installation running:

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

- `x.y.z` represents the major, minor, and patch version.
- `yyyy-zzzz` indicates the development start year and the current year.

## Quick start
To create a HDF dataset with `h5pack` you need only two main ingredients:

- A `.csv` file where each row represents a data point to be stored in the `.h5` file. For example, if you arere storing an audio file along with associated metrics or annotations, each row should contain the file path in one column, followed by the corresponding metrics or annotations in the subsequent columns.
- A `.yaml` file that defines the storage layout for h5pack, specifying how the data should be organized within the `.h5` file. This file also allows you to include any metadata you wish to store.

To see an example, navigate to `examples/00_annotated-audio-dataset` inside your `h5pack` 
folder. Inside this you will fine two components:

- `data.csv` the raw data to store. `file` has paths to audio files in the `audio` folder and `split` is used as an example annotation to specify whether a certain file belongs to the train or validation split.

- `h5pack.yaml` describes the layout we want to use:

```yaml title="h5pack.yaml"
datasets:
  annotated_audio_dataset:
    attrs:
      author: Your name
      description: Your dataset description
      version: 0.1.0

    data:
      file: data.csv
      fields:
        audio:
          column: file
          parser: as_audioint16
        split:
          column: split
          parser: as_utf8_str
```

From this layout:

- `datasets`: Mandatory root key where each children key such as `annotated_audio_dataset` will contain the necessary information to create a dataset. A single `.yaml` file can contain descriptions of several datasets to build.
- Each dataset has one optional key to store metadata `attrs`, and a mandatory `data` key to describe the layout.
- The `attrs` key can contain any arbitraty metadata (`author`, `description` and `version` in this case) as long as each value can be parsed as a `str`.
- The `data` key should contain a `file` key with the path to the data file (`data.csv`) and information about the fields in the `fields` key.
- Each field has an assigned unique name (`audio` and `split`) and needs a `column` value specifying from which data column to use from `data.csv`, and a `parser` value specifying how the data in that column should be parsed.

Currently the following parsers are supported:

| Parser name       | Resulting data type             | Example `.csv` row value |
|-------------------|---------------------------------|--------------------------|
| `as_audioint16`   | Audio files stored as `int16`   | `/path/to/file.wav`      |
| `as_audiofloat32` | Audio files stored as `float32` | `/path/to/file.wav`      |
| `as_audiofloat64` | Audio files stored as `float64` | `/path/to/file.wav`      |
| `as_int16`        | Single `int16` value            | `32767`                  |
| `as_float32`      | Single `float32` value          | `0.707`                  |
| `as_float64`      | Single `float64` value          | `3.146`                  |
| `as_listint8`     | List of `int8` values           | `[0, 127]`               |
| `as_listint16`    | List of `int16` values          | `[32767, 32767]`         |
| `as_listfloat32`  | List of `float32` values        | `[0.707, 1.414, ...]`    |
| `as_listfloat64`  | List of `float64` values        | `[0.505, 2.125, ...]`    |
| `as_utf8str`      | Single `str` value              | `hello_world`            |

Without further ado, run the following to create your first `.h5` file:

```bash
h5pack create --input h5pack.yaml --dataset annotated_audio_dataset --output annotated_audio_dataset.h5 
```

Then you should see:

```bash
1 partition(s) will be created
Do you want to continue? [y/n]:
```
Confirm by typing `y` and pressing enter. If all run successfully, you should see
two generated files `annotated_audio_dataset.h5` and `annotated_audio_dataset.sha256`.
The first file is the actual dataset and the second one is the hash of that file that you
can later on use to check its integrity by running:

```bash
h5pack checksum annotated_audio_dataset.sha256 
```

You should then see:

```bash
'annotated_audio_dataset.h5' checksum matches saved checksum (1f40888dec752cac3d18748fe445581f49f713f5d3b481fa43d2da84dde7984c)
```

This confirm that the stored hash and the hash of your dataset match.

Now that your dataset is created, you can inspect it with software such as <a href="https://www.hdfgroup.org/download-hdfview/" target="_blank">HDFView</a> or directly in the terminal with the included `h5pack info` tool by running:

```bash
h5pack info annotated_audio_dataset.h5
```

This will display:
```bash
File attribute(s):
  - creation_date: 2025-03-12 00:19:35
  - producer:      h5pack 0.0.3
Data group 'data':
  - 'audio' attribute(s):
    - parser:      as_audioint16
    - sample_rate: 16000
  - 'audio' data attribute(s):
    - shape: (3, 16000)
    - dtype: int16
  - 'audio_filepaths' data attribute(s):
    - shape: (3,)
    - dtype: object
  - 'split' attribute(s):
    - parser: as_utf8str
  - 'split' data attribute(s):
    - shape: (3,)
    - dtype: object
```

This is a quick way to inspect the content of your `.h5` file.

Now to extract the content from a `.h5` file created with `h5pack` into its
original content you can use `h5pack extract` as follows:

```bash
h5pack extract annotated_audio_dataset.h5 --output annotated_audio_dataset_data
```
This will create a folder named `annotated_audio_dataset_data` containing
the stored `metadata`, as well as the extracted annotations and audio files.

Congratulations! / ¡Felicidades! / Felicitats! / Onnittelut! for finishing this quickstart section. Now you can use `h5pack` to manage your own datasets. For a more in-depth description of each tool you can see the [Documentation](docs-intro).