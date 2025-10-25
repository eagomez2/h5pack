# `h5pack unpack` documentation

!!! note
    If you're new to `h5pack`, please consult our [Quickstart](quickstart.md) guide.
    Please note that this page is just a quick reference to explain different tool options.

This tool converts `.h5` files created with `h5pack` back into their constituent files. Additionally, it automatically generates a configuration `.yaml` file and an annotations `.csv` file, enabling you to repack the data using [`h5pack pack`](pack.md).

## Basic usage

```bash
h5pack unpack <h5-file>
```

This will create an output folder wich the same name as your `.h5` file.

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
