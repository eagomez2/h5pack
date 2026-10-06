# `h5pack verify` documentation

!!! note
    If you're new to `h5pack`, please consult our [Quickstart](quickstart.md) guide.
    Please note that this page is just a quick reference to explain different tool options.

This tool compares the audio stored in a `.h5` file with its original audio files, sample by sample. It is useful to make sure your data was packed correctly before deleting or archiving the original files.

!!! note
    [`h5pack checksum`](checksum.md) checks that a `.h5` file has not changed since it was created, while `h5pack verify` checks that its content matches the original audio files.

## Basic usage

```bash
h5pack verify <h5-file>
```

The output should look as follows:
```bash
  Verified 20 of 402 random row(s), 'audio' (audio matches the original files) in 327.2ms
```

By default, 20 random rows are verified. If any audio does not match its original file, every mismatch is reported and the command finishes with a non-zero exit code, so it can be safely used in scripts:

```bash
error: Row 5 'audio' does not match
  Caused by: /path/to/data/spk1/002.wav: 1 sample(s) differ
error: 1 of 20 audio file(s) do not match their original audio files
```

Audio is compared using the data type of the parser:

- `as_audioint16`, `as_audiofloat32` and `as_audiofloat64`: The original file is read with the same data type, so audio must match exactly.
- `as_audioflac`: The stored FLAC data is decoded and must match exactly for 8, 16 and 24-bit audio files. Audio files stored as floating point are quantized when they are stored as FLAC, so they must match within the resolution of the FLAC file.

Fields that were [resampled](parsers.md#resampling) are skipped, since they cannot match their original files.

## Advanced settings
### Number of rows
To change the number of random rows use the `-n/--num-rows` option, or use `-a/--all` to verify all rows:

```bash
h5pack verify <h5-file> --num-rows 100
h5pack verify <h5-file> --all
```

Random rows are selected using a fixed seed, so running the command twice verifies the same rows. Use `--seed` to select different rows.

### Location of the original files
`h5pack pack` stores the folder of the original audio files in the `source_dir` attribute of each audio field, relative to the `.h5` file. If you moved the `.h5` file or the original audio files, use the `--source` option to tell `h5pack verify` where they are:

```bash
h5pack verify <h5-file> --source <audio-folder>
```

`<audio-folder>` is the folder shared by all audio files of the field (i.e. the folder that the paths shown by [`h5pack show`](show.md) are relative to).

!!! note
    Files created with `h5pack` 1.2.0 can be verified using `--source`. Files created with older versions, or packed using `--skip-filepaths`, do not store the information needed to find the original files.

## Help
To see all available options, run:
```bash
h5pack verify --help
```

or using aliases:
```bash
h5pack verify -h
```
