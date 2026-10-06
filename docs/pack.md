# `h5pack pack` documentation

!!! note
    If you're new to `h5pack`, please consult our [Quickstart](quickstart.md) guide.
    Please note that this page is just a quick reference to explain different tool options.

This tool converts raw data, annotations, and a configuration file into one or several `.h5` partitions for easy use in training or data analysis pipelines. Packing data in this format offers faster access and transfer by reducing file system overhead. The HDF5 format also maintains complex data hierarchies and metadata in one container, facilitating consistent organization, cross-language accessibility, and scalability for large datasets.

## Basic usage

```bash
h5pack pack --config <config-file> --dataset <dataset-name> --output <output-h5-file>
```

or using aliases:
```bash
h5pack pack -c <config-file> -d <dataset-name> -o <output-h5-file>
```

If your config file is named `h5pack.yaml` (the default name), you can omit the `-c`/`--config` option:
```bash
h5pack pack -d <dataset-name> -o <output-h5-file>
```

The output should look as follows:
```bash
 Validated h5pack.yaml (dataset 'simple_dataset', 2 field(s), 3 row(s)) in 12.6ms
 Validated 3 row(s) (16 kHz, mono) in 5.0ms
    Packed 1 partition(s) with 1 worker(s) (3 row(s), 101.8 KiB) in 383.0ms
 + simple_dataset.h5 101.8 KiB
 + simple_dataset.sha256 checksums
```

!!! tip
    If you don't have a configuration file yet, [`h5pack init`](init.md) can create one from your `.csv` file.

Before writing any file, `h5pack pack` validates the configuration file and all values of all columns. If something is wrong, it explains what and where, e.g.:
```bash
error: Validation of field 'emb' failed
  Caused by: List '[126,127,128]' of column 'emb' (row 126) has values that do not fit in 'int8'
```

## Advanced settings

### Create multiple partitions
You can partition your `.h5` dataset across multiple files, improving organization and potentially performance.
These partitions can be unified using a <a href="https://docs.h5py.org/en/stable/vds.html" target="_blank">Virtual Dataset (VDS)</a>, allowing you to access all the data through a single logical file.

For example, your partition files might be named `dataset.pt0.h5`, `dataset.pt1.h5`, and so on. By using VDS, you can create a single virtual file named `dataset.h5`, which seamlessly integrates the datasets from all partition files. Accessing `dataset.h5` is equivalent to accessing the combined data from `dataset.pt0.h5`, `dataset.pt1.h5`, and other partition files, providing a convenient and efficient way to work with large datasets.

Partitions can be divided by a fixed count (e.g., 4 partitions) or by the number of files per partition (e.g., 1000 files per partition).

#### Fixed number of partitions
To create a fixed number of partitions (4 in this example), run:

```bash
h5pack pack --config <config-file> --dataset <dataset-name> --output <output-h5-file> --partitions 4
```

or using aliases:
```bash
h5pack pack -c <config-file> -d <dataset-name> -o <output-h5-file> -p 4
```

#### Number of files per partition
To fit a define number of files per partition (1000 in this example), run:
```bash
h5pack pack --config <config-file> --dataset <dataset-name> --output <output-h5-file> --files-per-partition 1000
```

or using aliases:
```bash
h5pack pack -c <config-file> -d <dataset-name> -o <output-h5-file> -f 1000
```

### Number of workers
To speed up the creation of your partition files, you can increase the number of workers using the `-w/--workers` option as:

```bash
h5pack pack --config <config-file> --dataset <dataset-name> --output <output-h5-file> --partitions 4 --workers 4
```

or using aliases:
```bash
h5pack pack -c <config-file> -d <dataset-name> -o <output-h5-file> -p 4 -w 4
```

This will spawn 4 workers, each handling a single partition concurrently. Using `-w 0` will spawn one worker per CPU core.

!!! note
    Workers are always started using the `spawn` method (also on Linux), so each worker receives its own copy of the input data. If you are packing very large `.csv` files, keep the number of workers in mind to avoid running out of memory.

### Create virtual dataset
If you want to automatically create a virtual dataset file that aggregates all partitions as part of a dataset, simply add the `--create-virtual` flag as follows:
```bash
h5pack pack -c <config-file> -d <dataset-name> -o <output-h5-file> -p 4 -w 4 --create-virtual
```

In addition to generating partition files like `dataset.pt0.h5`, `dataset.pt1.h5`, and so forth, using the `--create-virtual` flag will also create a virtual dataset named `dataset.h5`. This virtual file provides unified access to all partitioned data.

!!! note
    If your datasets have already been created, please refer to the [`h5pack virtual`](virtual.md) tool for integrating them into a virtual dataset.

### Dry run
To validate your data and see the partitions that would be created without writing any file, add the `--dry-run` flag:
```bash
h5pack pack -c <config-file> -d <dataset-name> -o <output-h5-file> -p 4 --dry-run
```

### Compression
Fields can be compressed using the `--compression` option (`none`, `gzip` or `lzf`), and the `gzip` level can be set with `--compression-level` (0-9):
```bash
h5pack pack -c <config-file> -d <dataset-name> -o <output-h5-file> --compression gzip --compression-level 4
```

To store audio in a compressed format, use the `as_audioflac` [parser](parsers.md) instead. See [Saving space](space.md) for all options to make your files smaller.

### Skip file paths
By default, the path of each audio file is stored in a `<field>__filepath` field. It is used to restore your folder structure with [`h5pack unpack`](unpack.md) and shown by [`h5pack show`](show.md). If you don't need it, add the `--skip-filepaths` flag.

### Other options
- `--overwrite`: Replace existing output files.
- `-u/--unattended`: Do not ask for confirmation before creating the files.
- `--skip-validation`: Skip the validation of the data. Only use it if your data was already validated.
- `--skip-checksum`: Do not create the `.sha256` checksum file.

### Output
All `h5pack` tools accept `-q/--quiet` to print only warnings and errors, and `-v/--verbose` to print additional details. Colors are disabled when the output is not a terminal or when the `NO_COLOR` environment variable is set, and progress bars are hidden when the output is redirected to a file.

## Help
To see all available options, run:
```bash
h5pack pack --help
```

or using aliases:
```bash
h5pack pack -h
```
