import os
import json
import h5py
import polars as pl
from ..core.io import write_audio


def _add_csv_column(output_csv: str, series: pl.Series) -> None:
    """Adds a column to the output `dataset.csv` file.

    Args:
        output_csv (str): Output `dataset.csv` file.
        series (pl.Series): Column to add.
    """
    if os.path.getsize(output_csv) > 0:  # Polars cannot read empty .csv
        # NOTE: Columns are read as str so values written by previous fields
        # are kept exactly as they are (e.g. '007' is not converted to 7)
        df = pl.read_csv(output_csv, infer_schema=False)
        df = df.with_columns(series)

    else:
        df = series.to_frame()

    df.write_csv(output_csv)


def _from_audiodtype(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Extracts audio of any data type and renders it to a folder.
    
    Args:
        output_csv (str): Output `dataset.csv` file.
        output_yaml (str): Output `h5pack.yaml` file.
        output_dir (str): Output folder.
        dataset_name (str): Name of exctracted dataset.
        field_name (str): Field name to extract data from.
        data (h5py.Dataset): h5py.Dataset object from which the data will be
            extracted.
        attrs (h5py.AttributeManager): Attributes associated to the audio data.
        ctx (dict): Extraxction context.
    """
    # Add fields to yaml
    output_yaml["datasets"][dataset_name]["data"]["fields"].update(
        {
            field_name: {
                "column": f"{field_name}__filepath",
                "parser": attrs["parser"]
            }
        }
    )

    # Keep sample rate if audio was resampled
    if attrs.get("resampled", False):
        output_yaml["datasets"][dataset_name]["data"]["fields"][field_name][
            "parser_args"
        ] = {"sample_rate": int(attrs["sample_rate"])}

    # Make output folder if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    is_flac = attrs.get("codec", None) == "flac"
    num_rows = data[field_name].shape[0]
 
    # Get file path and sample rate
    if f"{field_name}__filepath" in data:
        filenames = [
            s.decode("utf-8") for s in data[f"{field_name}__filepath"]
        ]

    elif f"{field_name}_filepaths" in data:  # Legacy (< 1.0.1)
        filenames = [
            s.decode("utf-8") for s in data[f"{field_name}_filepaths"]
        ]

    else:
        # NOTE: Paths are not stored when packing with --skip-filepaths
        ext = ".flac" if is_flac else ".wav"
        filenames = [
            f"{str(idx).zfill(len(str(num_rows)))}{ext}"
            for idx in range(num_rows)
        ]

    if is_flac:
        # NOTE: Files must keep the .flac extension since bytes are written
        # as they are stored
        filenames = [
            f"{os.path.splitext(f)[0]}.flac" for f in filenames
        ]

    fs = attrs["sample_rate"]
    num_channels = int(attrs.get("num_channels", 1))

    # Write paths to csv
    _add_csv_column(
        output_csv,
        pl.Series(
            name=f"{field_name}__filepath",
            values=[os.path.join("data", field_name, f) for f in filenames],
            dtype=pl.String
        )
    )

    # Get progress bar
    progress_bar = ctx["progress_bar"]

    with progress_bar:
        # Add task
        task = progress_bar.add_task(
            "Unpacking",
            total=len(filenames)
        )

        for row_idx, filename in enumerate(filenames):
            # Make step
            progress_bar.advance(task)

            # Extract file
            file = os.path.join(output_dir, filename)
            os.makedirs(os.path.dirname(file), exist_ok=True)
            audio = data[field_name][row_idx]

            if is_flac:
                # Write FLAC bytes as they are
                with open(file, "wb") as f:
                    f.write(audio.tobytes())

                continue

            if data[field_name].ndim == 1 and num_channels > 1:
                # Multichannel vlen audio is stored channel by channel
                audio = audio.reshape(num_channels, -1)

            write_audio(audio, file=file, fs=int(fs))


def from_audioint16(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for audio data as `int16`."""
    return _from_audiodtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_audiofloat32(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for audio data as `float32`."""
    return _from_audiodtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_audiofloat64(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for audio data as `float64`."""
    return _from_audiodtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def _from_dtype(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        field_name: str,
        dataset_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Extracts any single value data type and renders it to a `.csv` file.
    
    Args:
        output_csv (str): Output `dataset.csv` file.
        output_yaml (str): Output `h5pack.yaml` file.
        output_dir (str): Output folder.
        field_name (str): Field name to extract data from.
        dataset_name (str): Name of exctracted dataset.
        data (h5py.Dataset): h5py.Dataset object from which the data will be
            extracted.
        attrs (h5py.AttributeManager): Attributes associated to the audio data.
        ctx (dict): Extraxction context.
    """
    # Update .yaml
    output_yaml["datasets"][dataset_name]["data"]["fields"].update(
        {
            field_name: {
                "column": field_name,
                "parser": attrs["parser"]
            }
        }
    )

    # Write data to .csv
    _add_csv_column(
        output_csv,
        pl.Series(field_name, data[field_name][:].tolist())
    )


def from_int8(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for single value data as `int8`."""
    return _from_dtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_int16(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for single value data as `int16`."""
    return _from_dtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_float32(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for single value data as `float32`."""
    return _from_dtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_float64(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for single value data as `float64`."""
    return _from_dtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_utf8str(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for single value data as `str`."""
    # Update .yaml
    output_yaml["datasets"][dataset_name]["data"]["fields"].update(
        {
            field_name: {
                "column": field_name,
                "parser": attrs["parser"]
            }
        }
    )

    # Write data to .csv
    decoded_data = [
        i.decode("utf-8") if isinstance(i, bytes)
        else i for i in data[field_name]
    ]
    _add_csv_column(output_csv, pl.Series(field_name, decoded_data, pl.Utf8))


def _from_listdtype(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Extracts any list of numeric data types and renders it to a `.csv` file.
    
    Args:
        output_csv (str): Output `dataset.csv` file.
        output_yaml (str): Output `h5pack.yaml` file.
        output_dir (str): Output folder.
        dataset_name (str): Name of exctracted dataset.
        field_name (str): Field name to extract data from.
        data (h5py.Dataset): h5py.Dataset object from which the data will be
            extracted.
        attrs (h5py.AttributeManager): Attributes associated to the audio data.
        ctx (dict): Extraxction context.
    """
    # Update .yaml
    output_yaml["datasets"][dataset_name]["data"]["fields"].update(
        {
            field_name: {
                "column": field_name,
                "parser": attrs["parser"]
            }
        }
    )
    # Write data to .csv
    _add_csv_column(
        output_csv,
        pl.Series(
            field_name,
            [str(r) for r in data[field_name][:].tolist()],
            pl.String
        )
    )


def from_listint8(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for lists of data as `int8`."""
    return _from_listdtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_listint16(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for lists of data as `int16`."""
    return _from_listdtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_listfloat32(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for lists of data as `float32`."""
    return _from_listdtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_listfloat64(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for lists of data as `float64`."""
    return _from_listdtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_audioflac(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Alias of generic extractor for audio data stored as FLAC."""
    return _from_audiodtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs,
        ctx=ctx
    )


def from_categorical(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        ctx: dict
) -> None:
    """Extracts categorical data and renders it to a `.csv` file.

    Args:
        output_csv (str): Output `dataset.csv` file.
        output_yaml (str): Output `h5pack.yaml` file.
        output_dir (str): Output folder.
        dataset_name (str): Name of exctracted dataset.
        field_name (str): Field name to extract data from.
        data (h5py.Dataset): h5py.Dataset object from which the data will be
            extracted.
        attrs (h5py.AttributeManager): Attributes associated to the data.
        ctx (dict): Extraction context.
    """
    # Update .yaml
    output_yaml["datasets"][dataset_name]["data"]["fields"].update(
        {
            field_name: {
                "column": field_name,
                "parser": attrs["parser"]
            }
        }
    )

    # Write data to .csv
    categories = json.loads(attrs["categories"])
    values = [categories[code] for code in data[field_name][:]]
    _add_csv_column(output_csv, pl.Series(field_name, values))
