# Quickstart

If you haven't installed `h5pack` yet, please refer to our [Installation](install.md) guide before proceeding. This guide will help you get up and running with `h5pack` in minutes. To create an HDF5 dataset using `h5pack`, you need the following three components:

- **Data:** It is your raw data which may include one or multiple sets of audio files in any of the formats supported by <a href="https://github.com/bastibe/python-soundfile", target="_blank">`soundfile`</a> such was `.wav` or `.flac`.
- **Annotations:** Any additional data that is related to your audio data and relevant to pack in the dataset such as split (training, validation, test), speaker id or simlilar annotations. It is provided through a `.csv` file where one column has the path of the audio file and any other column can contain an annotation related to that audio file in any of the data types supported by `h5pack`.
- **Configuration file:** The configuration file is a `.yaml` file that relates your annotations and your audio data and tell `h5pack` what to include and how.

!!! note
    All the data mentioned below is in the `examples/00_annotated-audio-dataset` folder of `h5pack` repository.

In the next section we will go through each individual component of the exampled
located in `examples/00_annotated-audio-dataset`. This folder has the following data structure:

```bash title="examples/00_annotated-audio-dataset"
00_annotated-audio-dataset
├── data
│ ├── brownian-noise.flac
│ ├── pink-noise.flac
│ └── white-noise.flac
├── dataset.csv
└── h5pack.yaml
```

In this case, the datais in the **data** folder which consist of three `.flac`files, our **annotatios** are in the `dataset.csv` file and the **configuration** is inside the `h5pack.yaml` file.

## Data
It corresponds to the raw data that will be included in the dataset, this is typically
one or multiple sets of audio files in formats such as `.wav` or `.flac`. The resulting
`.h5` file will consolidate the data from multiple audio files into a single HDF5 file or a set of HDF5 partition files, ensuring efficient storage and access.

## Annotations
Annotations can be incorporated alongside each audio file using a `.csv` file. Each row in this `.csv` should include a column with the audio file's path, and the remaining columns can contain supplementary data to be embedded as annotations.

In this case, there is an arbitrary `Type` parameter describing type of noise included
in each audio file as a `str`. The `dataset.csv` content looks as follow:

| File                      | Type      |
|---------------------------|-----------|
| data/brownian-noise.flac  | brownian  |
| data/pink-noise.flac      | pink      |
| data/white-noise.flac     | white     |

This `.csv` can be generated in any way you want. For the most common uses cases we
recommend using <a href="https://pypi.org/project/sndls/" target="_blank">`sndls` tool</a>.

## Configuration file
The configuration file, typically named `h5pack.yaml`, is a `.yaml` file that connects your data and annotations, instructing h5pack on how to render your dataset(s). It consists of specifications for one or more datasets. In this case `h5pack.yaml` looks as follows (omitting the comments):

```yaml title="h5pack.yaml"
datasets:
  simple_dataset:
    attrs:
      author: Your name
      description: Your dataset description
      version: 0.1.0

    data:
      file: dataset.csv
      fields:
        audio:
          column: file
          parser: as_audioint16
        type:
          column: type
          parser: as_utf8str
```

From this file:

- `dataset`: Is the main mandatory key that can contain one or multiple datsets.
In this case a single dataset named `simple_dataset` is included.
- `attrs`: It is an optional key that can contain arbitrary `str` attributes to be
rendered with your data. Each key corresponds to the attribute name and each value
has to be a single `str` with the value of that specific attribute.
- `data`: Describes the data to be included in the files by relating your `.csv`
annotations file with your raw data. In this case:
    - `file` is the annotations file `dataset.csv`
    - `fields` can have one or multiple keys. Each key corresponds to the name
    of the field to be included in the dataset, and contains a `column` key with
    the column from the `.csv` to be used and a `parser` selecting the parser used
    to include that data.

`h5pack` supports the following parsers:

| Parser name       | Resulting data type             | Example `.csv` row value |
|-------------------|---------------------------------|--------------------------|
| `as_audioint16`   | Audio files stored as `int16`   | `/path/to/file.wav`      |
| `as_audiofloat32` | Audio files stored as `float32` | `/path/to/file.wav`      |
| `as_audiofloat64` | Audio files stored as `float64` | `/path/to/file.wav`      |
| `as_int8`         | Single `int8` value             | `64`                     |
| `as_int16`        | Single `int16` value            | `32767`                  |
| `as_float32`      | Single `float32` value          | `0.707`                  |
| `as_float64`      | Single `float64` value          | `3.146`                  |
| `as_listint8`     | List of `int8` values           | `[0, 127]`               |
| `as_listint16`    | List of `int16` values          | `[32767, 32767]`         |
| `as_listfloat32`  | List of `float32` values        | `[0.707, 1.414, ...]`    |
| `as_listfloat64`  | List of `float64` values        | `[0.505, 2.125, ...]`    |
| `as_utf8str`      | Single `str` value              | `hello_world`            |

In this case, there a single set of audio files from the `file` column and saved as `int16` (`as_audioint16`) in the `audio` field, and a `str` from the `type` column
and saved as `str` (`as_utf8str`).

## Rendering the dataset with `h5pack pack`
Now that all required files are ready, we can use `h5pack pack` tool to create
the actual `.h5` file. Now run
```bash
h5pack pack --config h5pack.yaml --dataset simple_dataset --output simple_dataset.h5
```

This will result in the following output:
```bash
Using root folder '/path/to/h5pack/examples/00_annotated-audio-dataset'
Validating configuration file 'h5pack.yaml' ...
Configuration file validation completed
Validating input data ...
Validating data of 'audio' field ...
Validation of 'audio' field data completed
Validating data of 'type' field ...
Validation of 'type' field data completed
Input data validation completed
Generating 1 partition spec(s) ...
Partition spec(s) completed
1 partition(s) will be created
Do you want to continue? [y/n]:
```

Once you have executed the previous required commands, you will need to confirm by typing `y` and pressing `Enter` when prompted. Upon confirmation, two files will be generated: `simple_datase.h5` and `simple_dataset.sha256`. The `simple_datase.h5` file is your dataset, now ready for use, while the `simple_dataset.sha256` file contains the checksum for `simple_dataset.h5`. You can use this checksum file later to verify the integrity of your dataset.

For more options available with the `h5pack pack` tool, you can run
```bash
h5pack pack --help
```

This tool allows customization of several aspects, including specifying the number of partitions for the output file and determining the number of workers that should be used to efficiently render the resulting files.

## Inspecting the dataset with `h5pack info`
With the file now generated, you can easily inspect its content using the `h5pack info` tool. To do so, simply run the following command:
```bash
h5pack info simple_dataset.h5
```

It will output
```bash
Input file: 'simple_dataset.h5'
File attribute(s):
  - author:        Your name
  - creation_date: 2025-10-25 11:17:37
  - description:   Your dataset description
  - producer:      h5pack 1.1.0
  - version:       0.1.0
Data group 'data':
  - 'audio' attribute(s):
    - parser:      as_audioint16
    - sample_rate: 16000
  - 'audio' data attribute(s):
    - shape: (3, 16000)
    - dtype: int16
  - 'audio__filepath' data attribute(s):
    - shape: (3,)
    - dtype: object
  - 'type' attribute(s):
    - parser: as_utf8str
  - 'type' data attribute(s):
    - shape: (3,)
    - dtype: object
```

This information allows you to swiftly verify the contents of your file. In this
example, the file contains three audio samples, each with a duration of one
second and sampled at a rate of 16 kHz, resulting in 16,000 samples per audio clip.

## Corroborating integrity with `h5pack checksum`
To verify the integrity of your file, run the following command:
```bash
h5pack checksum simple_dataset.sha256
```

The command will output:
```bash
Verifying checksum in 'simple_dataset.sha256' ...
simple_dataset.h5	cefeb61ce741bfa4a25cf54069e73d466fa9bdc2093d461ca30f381f6606eb79 [OK]
Checksum verification completed in 0.2 millisecond(s)
```

Using this tool, you can consistently check for any potentially corrupted files.

## Unpacking the dataset with `h5pack unpack`
You also have the option to convert your .h5 files back into their original constituent files
using the `h5pack unpack` tool. To do it, simply run
```bash
h5pack unpack simple_dataset.h5 --output simple_dataset
```

This process creates a `simple_dataset` folder, which includes the corresponding
data, annotations file, and configuration file.

```bash
simple_dataset
├── data
│   └── audio
│       ├── brownian-noise.flac
│       ├── pink-noise.flac
│       └── white-noise.flac
├── dataset.csv
└── h5pack.yaml
```

The generated files are structured so that you can promptly repack them into `.h5` file(s) if desired.

## Conclusion
You should now be able to manage, verify, and repack your data to ensure its integrity and flexibility for future use.
For more information on additional tools, including those not covered in this [Quickstart](quickstart.md) guide, visit the [Documentation](docs.md) section.
