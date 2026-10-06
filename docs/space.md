# Saving space

Audio datasets can be large, and `h5pack` offers several ways to make them smaller. All of them are lossless, so no information is lost.

!!! tip
    Use `h5pack pack --dry-run` to validate your data and check the planned partitions before writing any file, and [`h5pack verify`](verify.md) to check that the packed audio matches your original files.

## Store audio as FLAC
`as_audioflac` stores each audio file as FLAC, a lossless audio codec. This is the most effective option for audio, typically reducing speech and music to roughly half of their uncompressed size (the exact ratio depends on the content; noise-like signals compress much less).

```yaml title="h5pack.yaml"
        audio:
          column: file
          parser: as_audioflac
```

The trade-off is that audio has to be decoded when it is read. Decoding FLAC is fast, but if reading speed is critical (e.g. many short files read in a tight training loop) you may prefer an uncompressed parser. See [Parsers](parsers.md#flac) for more details.

## Use the smallest data type
- Store 16-bit audio files with `as_audioint16`. Storing them with `as_audiofloat32` doubles their size without adding any information (`h5pack pack` shows a warning when this happens).
- Store labels, splits, speaker ids and other repeated values with `as_categorical` instead of `as_utf8str`. Each value takes 1 byte for up to 256 different values.
- Store lists with the smallest data type able to hold their values (e.g. `as_listint8` for small integers). See [Choosing the smallest data type](parsers.md#choosing-the-smallest-data-type).

## Compress fields
`h5pack pack` can compress fields using HDF5's built-in compression filters with the `--compression` option:

```bash
h5pack pack -c h5pack.yaml -d my_dataset -o my_dataset.h5 --compression gzip
```

| Option   | Description                                                                  |
|----------|------------------------------------------------------------------------------|
| `none`   | No compression (default).                                                    |
| `gzip`   | Good compression, slower. The level can be set with `--compression-level` (0-9, default 4). Readable by any HDF5 library. |
| `lzf`    | Fast compression and decompression, less effective. Only available in `h5py`. |

Compressed fields are split into chunks (one row per chunk for audio) and the bytes of each value are shuffled before compression, which usually improves compression of numeric data.

!!! note
    HDF5 compression only applies to fields where all rows have the same length (e.g. fixed length audio, numbers, categorical values or lists of the same length). Fields with rows of different lengths and FLAC audio are stored uncompressed, since HDF5 cannot compress them. For audio, `as_audioflac` is usually much more effective than `--compression`.

## Skip file paths
By default, `h5pack pack` stores the path of each original audio file, which is needed to restore your folder structure with [`h5pack unpack`](unpack.md) and to run [`h5pack verify`](verify.md). For datasets with many short audio files, these paths can take a noticeable amount of space. If you don't need them, add `--skip-filepaths`:

```bash
h5pack pack -c h5pack.yaml -d my_dataset -o my_dataset.h5 --skip-filepaths
```

When unpacking a file packed this way, audio files are named after their row index (e.g. `0042.wav`).
