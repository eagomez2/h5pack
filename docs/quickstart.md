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

`h5pack` supports parsers for audio (stored as `int16`, `float32`, `float64` or FLAC), numbers, lists of numbers, text and categorical values. See [Parsers](parsers.md) for the full list.

!!! tip
    Instead of writing the configuration file by hand, you can create it from your `.csv` file using [`h5pack init`](init.md):
    ```bash
    h5pack init dataset.csv
    ```

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
 Validated h5pack.yaml (dataset 'simple_dataset', 2 field(s), 3 row(s)) in 12.6ms
 Validated 3 row(s) (16 kHz, mono) in 5.0ms
1 partition(s) will be created. Do you want to continue? [y/n]:
```

Once you have executed the previous required commands, you will need to confirm by typing `y` and pressing `Enter` when prompted. Upon confirmation, two files will be generated:

```bash
    Packed 1 partition(s) with 1 worker(s) (3 row(s), 101.8 KiB) in 383.0ms
 + simple_dataset.h5 101.8 KiB
 + simple_dataset.sha256 checksums
```

The `simple_dataset.h5` file is your dataset, now ready for use, while the `simple_dataset.sha256` file contains the checksum for `simple_dataset.h5`. You can use this checksum file later to verify the integrity of your dataset.

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
simple_dataset.h5 (101.8 KiB)

File attributes
  Name             Value
  author           Your name
  creation_date    2025-10-25 11:17:37
  description      Your dataset description
  producer         h5pack 1.3.0
  version          0.1.0

'data' fields
  Field              Shape         Dtype     Attributes
  audio              (3, 16000)    int16     num_channels: 1
                                             parser: as_audioint16
                                             sample_rate: 16000
                                             source_dir: data
  audio__filepath    (3,)          object
  type               (3,)          object    parser: as_utf8str
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
simple_dataset.h5       f8b1e88aefe681a42fab66024d50fc8573d533be780e1690c110ee213d54dcb0 [OK]
  Verified 1 file(s) in 'simple_dataset.sha256' in 5.0ms
```

Using this tool, you can consistently check for any potentially corrupted files.

## Checking rows with `h5pack show`
To see the content of any row, or listen to its audio, run:
```bash
h5pack show simple_dataset.h5 --rows 1
```

It will output:
```bash
Row 1 of 3
 Field ┃ Type          ┃ Value
━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 audio │ audio (int16) │ 1.00s, 16 kHz, mono, peak 0.65
       │               │ pink-noise.flac
 type  │ str           │ pink
```

Add `--save <folder>` to save the audio of the selected rows as `.wav` files, or `--play` to play it. See [`h5pack show`](show.md) for more details.

## Comparing with the original files using `h5pack verify`
To make sure that the packed audio matches your original audio files, run:
```bash
h5pack verify simple_dataset.h5
```

It will output:
```bash
  Verified all 3 row(s), 'audio' (audio matches the original files) in 7.6ms
```

## Reading the dataset in Python
Your dataset can be read row by row using `h5pack.open()`:
```python
import h5pack

with h5pack.open("simple_dataset.h5") as data:
    row = data[1]
    print(row["type"], row["audio"].shape, data.sample_rate("audio"))
    # pink (16000,) 16000
```

See [Reading data in Python](reading.md) for more details, including how to use it with PyTorch.

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
