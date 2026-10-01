# `h5pack info` documentation

!!! note
    If you're new to `h5pack`, please consult our [Quickstart](quickstart.md) guide.
    Please note that this page is just a quick reference to explain different tool options.

This tool can be used to quickly inspect existing `.h5` files that have been created using `h5pack`.

## Basic usage

```bash
h5pack info <h5-file>
```

This will output the information of your `.h5` file. If the file is a <a href="https://docs.h5py.org/en/stable/vds.html" target="_blank">Virtual Dataset (VDS)</a>, it will also provide information about the linked files.

## Help
To see all available options, run:
```bash
h5pack info --help
```

or using aliases:
```bash
h5pack info -h
```
