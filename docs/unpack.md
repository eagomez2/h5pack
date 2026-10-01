# `h5pack unpack` documentation

!!! note
    If you're new to `h5pack`, please consult our [Quickstart](quickstart.md) guide.
    Please note that this page is just a quick reference to explain different tool options.

This tool converts `.h5` files created with `h5pack` back into their constituent files. Additionally, it automatically generates a configuration `.yaml` file and an annotations `.csv` file, enabling you to repack the data using [`h5pack pack`](pack.md).

## Basic usage

```bash
h5pack unpack <h5-file>
```

This will create an output folder with the same name as your `.h5` file. Its structure will look as follows:

```bash
<output-folder>
├── data
│   └── <field-name>
│       └── <audio-files>
├── dataset.csv
└── h5pack.yaml
```

Audio files are written to a folder named after their field. If your original audio files were stored in subfolders (e.g. `spk1/001.wav` and `spk2/001.wav`), those subfolders are preserved, so files sharing the same name do not overwrite each other.

## Advanced settings
To specify the output folder path, you can use the `-o/--output` option as follows:

```bash
h5pack unpack <h5-file> --output <output-folder>
```

or using aliases:

```bash
h5pack unpack <h5-file> -o <output-folder>
```

## Help
To see all available options, run:
```bash
h5pack unpack --help
```

or using aliases:
```bash
h5pack unpack -h
```
