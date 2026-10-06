# `h5pack init` documentation

!!! note
    If you're new to `h5pack`, please consult our [Quickstart](quickstart.md) guide.
    Please note that this page is just a quick reference to explain different tool options.

This tool creates a `h5pack.yaml` configuration file from an annotations `.csv` file, so you don't have to write it from scratch. It inspects every column and guesses a suitable [parser](parsers.md) for it.

## Basic usage

```bash
h5pack init <csv-file>
```

The output should look as follows:
```bash
   Created h5pack.yaml (dataset 'dataset', 2 field(s))
 + h5pack.yaml 481 B
```

And the created `h5pack.yaml` file:
```yaml title="h5pack.yaml"
# h5pack configuration file created with 'h5pack init'. Please
# review the parsers before running:
#   h5pack pack -c h5pack.yaml -d dataset -o dataset.h5
# All parsers: https://eagomez2.github.io/h5pack/parsers/
datasets:
  dataset:
    attrs:
      description: ""
    data:
      file: dataset.csv
      fields:
        file:
          column: file
          parser: as_audioint16  # 16 kHz, mono, PCM_16
        type:
          column: type
          parser: as_utf8str  # text
```

Parsers are guessed as follows:

| Column content                               | Parser                                                    |
|----------------------------------------------|-----------------------------------------------------------|
| Paths of existing audio files                | `as_audioint16` (`as_audiofloat32` for 24-bit or float files) |
| Lists such as `[1, 2, 3]`                    | Smallest list parser able to store all values (e.g. `as_listint8`) |
| Text with few unique values (e.g. labels)    | `as_categorical`                                          |
| Any other text                               | `as_utf8str`                                              |
| Integers                                     | `as_int8`, `as_int16` or `as_float64` depending on their range |
| Decimal numbers                              | `as_float32`                                              |

!!! note
    The guessed parsers are a starting point. Always review them before packing your data. For example, you may prefer `as_audioflac` to [save space](space.md), or `as_float64` to keep the full precision of decimal numbers.

Audio paths are resolved from the folder of the output `.yaml` file, which is also how `h5pack pack` resolves them. Please create the configuration file in the folder your audio paths are relative to.

## Advanced settings
### Output file
By default the configuration file is saved as `h5pack.yaml` in the current folder. To choose a different file use the `-o/--output` option:

```bash
h5pack init <csv-file> --output <yaml-file>
```

### Dataset name
By default the dataset is named after the `.csv` file. To choose a different name use the `-d/--dataset` option:

```bash
h5pack init <csv-file> --dataset <dataset-name>
```

### Overwrite an existing file
Existing configuration files are never replaced unless you add the `--overwrite` flag.

## Help
To see all available options, run:
```bash
h5pack init --help
```

or using aliases:
```bash
h5pack init -h
```
