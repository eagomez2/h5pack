# `h5pack show` documentation

!!! note
    If you're new to `h5pack`, please consult our [Quickstart](quickstart.md) guide.
    Please note that this page is just a quick reference to explain different tool options.

This tool shows the content of one or more rows of a `.h5` file. It can also save their audio to `.wav` files or play it, so you can check by ear that your data was packed correctly.

## Basic usage

```bash
h5pack show <h5-file> --rows <rows>
```

The output should look as follows:
```bash
Row 1 of 3
 Field ┃ Type          ┃ Value
━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 audio │ audio (int16) │ 1.00s, 16 kHz, mono, peak 0.65
       │               │ pink-noise.flac
 type  │ str           │ pink
```

For audio fields, the duration, sample rate, number of channels and peak level are shown, together with the path of the original audio file.

Rows can be selected using `-r/--rows` as:

| Value   | Selected rows             |
|---------|---------------------------|
| `42`    | Row 42                    |
| `-1`    | Last row                  |
| `10:20` | Rows 10 to 19             |
| `:5`    | First 5 rows              |
| `-3:`   | Last 3 rows               |

If `-r/--rows` is not given, the first row is shown. Virtual datasets are supported, so you can inspect any row of a dataset split into several partitions.

## Advanced settings
### Select fields
To show only some fields use the `-f/--fields` option:

```bash
h5pack show <h5-file> -r 0:3 --fields audio type
```

### Save audio
To save the audio of the selected rows as `.wav` files use the `-s/--save` option:

```bash
h5pack show <h5-file> -r 0:3 --save <output-folder>
```

Files are named after their row and field, e.g. `row0_audio.wav`.

### Play audio
To play the audio of the selected rows use the `-p/--play` flag:

```bash
h5pack show <h5-file> -r 0:3 --play
```

Press `Ctrl+C` to stop playback. Playing audio requires the optional <a href="https://python-sounddevice.readthedocs.io/" target="_blank">`sounddevice`</a> package, which can be installed as:

```bash
pip install "h5pack[play]"
```

## Help
To see all available options, run:
```bash
h5pack show --help
```

or using aliases:
```bash
h5pack show -h
```
