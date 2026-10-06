# Parsers

Parsers tell `h5pack pack` how to store each column of your `.csv` file. Each field of a dataset in `h5pack.yaml` selects one parser:

```yaml title="h5pack.yaml"
datasets:
  my_dataset:
    data:
      file: dataset.csv
      fields:
        audio:
          column: file
          parser: as_audioint16
```

[`h5pack init`](init.md) can guess the parsers of all columns of a `.csv` file for you.

## Available parsers

| Parser name       | Resulting data type                     | Example `.csv` row value |
|-------------------|-----------------------------------------|--------------------------|
| `as_audioint16`   | Audio files stored as `int16`           | `/path/to/file.wav`      |
| `as_audiofloat32` | Audio files stored as `float32`         | `/path/to/file.wav`      |
| `as_audiofloat64` | Audio files stored as `float64`         | `/path/to/file.wav`      |
| `as_audioflac`    | Audio files stored as FLAC (lossless)   | `/path/to/file.wav`      |
| `as_int8`         | Single `int8` value                     | `64`                     |
| `as_int16`        | Single `int16` value                    | `32767`                  |
| `as_float32`      | Single `float32` value                  | `0.707`                  |
| `as_float64`      | Single `float64` value                  | `3.146`                  |
| `as_listint8`     | List of `int8` values                   | `[0, 127]`               |
| `as_listint16`    | List of `int16` values                  | `[32767, 32767]`         |
| `as_listfloat32`  | List of `float32` values                | `[0.707, 1.414, ...]`    |
| `as_listfloat64`  | List of `float64` values                | `[0.505, 2.125, ...]`    |
| `as_utf8str`      | Single `str` value                      | `hello_world`            |
| `as_categorical`  | Value from a small set of values        | `train`                  |

All values are validated before any file is written. For example, `h5pack pack` reports values that do not fit in the selected data type (e.g. `300` with `as_int8`), empty values, or audio files with different sample rates, and shows the row where the problem was found.

## Audio parsers
Audio parsers read the audio files listed in a column and store their samples.

| Parser            | Stored as                    | Best for                                              |
|-------------------|------------------------------|-------------------------------------------------------|
| `as_audioint16`   | `int16` samples              | 16-bit audio files (most `.wav` files)                |
| `as_audiofloat32` | `float32` samples            | 24-bit or floating point audio files                  |
| `as_audiofloat64` | `float64` samples            | Floating point audio that needs full precision        |
| `as_audioflac`    | FLAC encoded bytes           | Saving space without losing any information           |

!!! warning
    Storing 16-bit audio files with `as_audiofloat32` or `as_audiofloat64` doubles or quadruples their size without adding any information. `h5pack pack` shows a warning when this happens.

### Layout
Audio is stored as follows:

| Audio                                 | Shape of the field                       |
|---------------------------------------|------------------------------------------|
| Mono, same length                     | `(num_rows, num_samples)`                |
| Multichannel, same length             | `(num_rows, num_channels, num_samples)`  |
| Different lengths                     | `(num_rows,)` with one array per row     |

When files have different lengths, multichannel audio is stored channel by channel in a single array per row and restored using the `num_channels` attribute. You don't have to deal with this yourself if you use the [Python API](reading.md), which always returns `(num_samples,)` arrays for mono audio and `(num_channels, num_samples)` arrays for multichannel audio.

All audio files of a column must have the same number of channels.

### Attributes
Each audio field stores the following attributes:

- `parser`: Parser used to pack the field.
- `sample_rate`: Sample rate in Hz.
- `num_channels`: Number of channels.
- `source_dir`: Folder of the original audio files, relative to the `.h5` file. It is used by [`h5pack verify`](verify.md).
- `codec` and `flac_subtype`: Only for `as_audioflac`.
- `resampled`: Only if the audio was resampled.

The path of each original audio file is stored in an additional `<field>__filepath` field, relative to the folder shared by all audio files of the field (e.g. `spk1/001.wav`). It is used to restore the original folder structure with [`h5pack unpack`](unpack.md). If you don't need it, use `h5pack pack --skip-filepaths`.

### FLAC
`as_audioflac` stores each audio file as FLAC, a lossless audio codec, typically reducing the size of speech and music by 40 to 60%. Audio is decoded when it is read, so reading is slower than with uncompressed parsers. See [Saving space](space.md) for more details.

FLAC files are stored as they are, while other formats are encoded as 16-bit FLAC, or 24-bit FLAC if any file of the column has more than 16 bits. FLAC supports up to 8 channels. When unpacking, audio packed with `as_audioflac` is written as `.flac` files.

### Resampling
All audio files of a column must have the same sample rate. If your files have different sample rates, or you want to store them at a different sample rate, add a `sample_rate` argument to the parser:

```yaml title="h5pack.yaml"
        audio:
          column: file
          parser: as_audiofloat32
          parser_args:
            sample_rate: 16000
```

Resampling requires the optional <a href="https://github.com/dofuuz/python-soxr" target="_blank">`soxr`</a> package, which can be installed as:

```bash
pip install "h5pack[resample]"
```

!!! note
    Resampled audio cannot match its original files, so it is skipped by [`h5pack verify`](verify.md). Since resampling produces values between the original integer levels, `as_audiofloat32` is recommended for resampled audio.

## Single value parsers
`as_int8`, `as_int16`, `as_float32` and `as_float64` store one number per row. Pick the smallest data type that can hold all your values:

| Data type | Range                                        |
|-----------|----------------------------------------------|
| `int8`    | -128 to 127                                  |
| `int16`   | -32768 to 32767                              |
| `float32` | About 7 significant digits                   |
| `float64` | About 16 significant digits (integers up to 2<sup>53</sup> are exact) |

Integers that do not fit in `int16` (e.g. ids or timestamps) can be stored with `as_float64` without losing precision.

## List parsers
`as_listint8`, `as_listint16`, `as_listfloat32` and `as_listfloat64` store a list of numbers per row, written in the `.csv` file as `[1, 2, 3]`. If all lists have the same length, they are stored as a `(num_rows, list_length)` array. Otherwise, one array per row is stored.

### Choosing the smallest data type
Lists such as token ids, frame labels or alignments can easily take more space than the audio itself. Since every value of every list is stored with the selected data type, choosing the smallest data type that can hold all values makes a big difference:

| Values                           | Parser             | Bytes per value |
|----------------------------------|--------------------|-----------------|
| Small integers (-128 to 127)     | `as_listint8`      | 1               |
| Integers (-32768 to 32767)       | `as_listint16`     | 2               |
| Decimal numbers                  | `as_listfloat32`   | 4               |
| Decimal numbers, full precision  | `as_listfloat64`   | 8               |

For example, a list of 1,000 frame labels between 0 and 50 takes 1 KB with `as_listint8` but 8 KB with `as_listfloat64`. `h5pack pack` reports values that do not fit in the selected data type, so you can safely try the smallest one first.

## Text parsers
### `as_utf8str`
Stores any text as UTF-8.

### `as_categorical`
Stores columns with a small set of repeated values, such as labels, splits or speaker ids, much more efficiently than `as_utf8str`. Each value is stored as an integer code (`uint8` for up to 256 different values) and the list of values is stored once in the `categories` attribute of the field. Categories are sorted and shared by all partitions, so codes are consistent across a whole dataset.

`as_categorical` can be used with text and integer columns. The [Python API](reading.md) returns the original values, while the codes are available for machine learning pipelines that need class indices:

```python
import h5pack

with h5pack.open("dataset.h5") as data:
    categories = data.categories("split")  # e.g. ['test', 'train', 'valid']
    code = data.read("split", 0, decode=False)  # e.g. 1
```
