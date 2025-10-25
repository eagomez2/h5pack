# `h5pack checksum` documentation

!!! note
    If you're new to `h5pack`, please consult our [Quickstart](quickstart.md) guide.
    Please note that this page is just a quick reference to explain different tool options.

This tool allows you to verify the integrity of your `.h5` files by checking that the stored checksum matches the actual checksum value. Additionally, you can use it to regenerate the checksum file if something has changed and you need to update its checksum value. A checksum file is a `.sha256` file stored right next to your `.h5` file. This file contains a list of files and their corresponding checksum values.

## Basic usage
### Verifying existing checkpoints

```bash
h5pack checksum <sha256-file>
```

The output should look as follows:
```bash
Verifying checksum in 'dataset.sha256' ...
dataset.h5	8258bb92f49c2ed032bbe6f1e3bc86132cbc8b7bc0c0e512c4bc6b9888f9aabe [OK]
Checksum verification completed in 0.2 millisecond(s)
```

### Calculating checksum

```bash
h5pack checksum <h5-file>
```

If the input has a `.h5` extension or is a folder, the checksum of all `.h5` files found will be calculated and printed to the screen as follows:

```bash
Calculating checksum for .h5 files in 'dataset.h5' ...
dataset.h5	8258bb92f49c2ed032bbe6f1e3bc86132cbc8b7bc0c0e512c4bc6b9888f9aabe
Checksum calculation completed in 0.1 millisecond(s)
```

Additionally, you can use the `--save` to save the calculated checksum to a `.sha256` file.

```bash
h5pack checksum <h5-file> --save <output-sha256-file>
```

## Advanced settings
### Recursive search
To calculate the checksum of multiple `.h5` files, possibly within nested folders, you can use the `-r/--recursive` flag. Please note that this option is only available for checksum calculations.

```bash
h5pack checksum <input-folder-with-h5-files> --recursive
```

or using aliases:
```bash
h5pack checksum <input-folder-with-h5-files> -r
```

## Help
To see all available options, run:
```bash
h5pack checksum --help
```

or using aliases:
```bash
h5pack checksum -h
```