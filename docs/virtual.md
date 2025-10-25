# `h5pack virtual` documentation

!!! note
    If you're new to `h5pack`, please consult our [Quickstart](quickstart.md) guide.
    Please note that this page is just a quick reference to explain different tool options.

This tool takes two or more `.h5` partition files created with `h5pack` and creates a <a href="https://docs.h5py.org/en/stable/vds.html" target="_blank">Virtual Dataset (VDS)</a>, allowing you to access all the data through a single logical file.

## Basic usage

```bash
h5pack virtual <input-file-0> <input-file-1> ... --output <output-h5-file>
```

or using aliases:

```bash
h5pack virtual <input-file-0> <input-file-1> ... -o <output-h5-file>
```

This will result in a single `.h5` file that acts as a link to all the data from each input file.

!!! warning
    When using relative paths in <a href="https://docs.h5py.org/en/stable/vds.html" target="_blank">Virtual Dataset (VDS)</a>, they are typically resolved relative to the location of the VDS file itself. If you run a script from a different location and attempt to load the data, the relative paths are still anchored to the VDS file's location, not the location from which the script is executed.

    If for your specific use case using absolute paths is more convenient, you can
    use the `--force-abspath` flag. Additionally, you can use [`h5pack info`](info.md) to check the validity of your file paths.

## Advanced settings
### Adding additional attributes
You can add additional attributes to the VDS by using the `-a/--attrs` option. The arguments to this option should be key/value pairs. All values will be parsed as a string.
```bash
h5pack virtual <input-file-0> <input-file-1> --attrs key0 value0 key1 value1 ... --output <output-h5-file>
```

or using aliases:
```bash
h5pack virtual <input-file-0> <input-file-1> -a key0 value0 key1 value1 ... -o <output-h5-file>
```

### Recursive search
Oftentimes you may have folders or nested folders containing multiple `.h5` files that you want to use to create a VDS. In such cases, you can provide the parent folder as input and use the `-r/--recursive` option to make the search recursive.

```bash
h5pack virtual <input-folder> --output <output-h5-file> --recursive
```

or sing aliases:
```bash
h5pack virtual <input-folder> -o <output-h5-file> -r
```

### Selecting or filtering multiple files from a folder
You may also want to easily select or filter a subset of all files. In these cases you can use the `-s/--select` or `-f/--filter` option.

For selecting a subset of files you can run:
```bash
h5pack virtual <input-folder> --output <output-h5-file> --select <select-expression> --recursive
```

or using aliases:
```bash
h5pack virtual <input-folder> -o <output-h5-file> -s <select-expression> -r
```

For filtering out a subset of files you can run:
```bash
h5pack virtual <input-folder> --output <output-h5-file> --filter <filter-expression> --recursive
```

or using aliases:
```bash
h5pack virtual <input-folder> -o <output-h5-file> -f <filter-expression> -r
```

### Forcing absolute paths
If you want to force all the paths included in the VDS to be absolute paths regardless of how they are provided, you can use the `--force-absppath` flag as follows:

```bash
h5pack virtual <input-folder> --output <output-h5-file> --force-abspath
```

## Help
To see all available options, run:
```bash
h5pack virtual --help
```

or using aliases:
```bash
h5pack virtual -h
```