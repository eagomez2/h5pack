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

This will spawn 4 workers, each handling a single partition concurrently.

### Create virtual dataset
If you want to automatically create a virtual dataset file that aggregates all partitions as part of a dataset, simply add the `--create-virtual` flag as follows:
```bash
h5pack pack -c <config-file> -d <dataset-name> -o <output-h5-file> -p 4 -w 4 --create-virtual
```

In addition to generating partition files like `dataset.pt0.h5`, `dataset.pt1.h5`, and so forth, using the `--create-virtual` flag will also create a virtual dataset named `dataset.h5`. This virtual file provides unified access to all partitioned data.

!!! note
    If your datasets have already been created, please refer to the [`h5pack virtual`](virtual.md) tool for integrating them into a virtual dataset.

## Help
To see all available options, run:
```bash
h5pack pack --help
```

or using aliases:
```bash
h5pack pack -h
```
