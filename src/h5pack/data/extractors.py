import os
import h5py
import polars as pl
from tqdm import tqdm
from ..core.io import write_audio


def _from_audiodtype(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Extracts audio of any data type and renders it to a folder.
    
    Args:
        output_dir (str): Output folder.
        field_name (str): Field name to extract data from.
        data (h5py.Dataset): Data from which the data will be extracted.
        attrs (h5py.AttributeManager): Attributes associated to the audio data.
    """
    # Add fields to yaml
    output_yaml["datasets"][dataset_name]["data"]["fields"].update(
        {
            field_name: {
                "column": f"{field_name}_filepath",
                "parser": attrs["parser"]
            }
        }
    )

    # Make output folder if it does not exist
    os.makedirs(output_dir, exist_ok=True)
 
    # Get file path and sample rate
    filenames = [s.decode("utf-8") for s in data[f"{field_name}_filepath"]]
    fs = attrs["sample_rate"]

    # Write paths to csv
    if os.path.getsize(output_csv) > 0:  # Polars cannot read empty .csv
        df = pl.read_csv(output_csv, n_rows=0)
    
    else:
        df = pl.DataFrame()

    df = df.with_columns(
        pl.Series(
            name=f"{field_name}_filepath",
            values=[os.path.join("data", field_name, f) for f in filenames],
            dtype=pl.String
        )
    )
    df.write_csv(output_csv)

    if data[field_name].ndim == 2:  # Fixed length audio
        for row_idx, filename in tqdm(
            zip(range(data[field_name].shape[0]), filenames),
            total=len(filenames),
            desc=f"Extracting '{field_name}'",
            colour="green",
            leave=False,
        ):
            os.makedirs(
                os.path.join(output_dir, os.path.dirname(filename)),
                exist_ok=True
            )
            audio = data[field_name][row_idx, :]
            write_audio(
                audio,
                file=os.path.join(output_dir, filename),
                fs=int(fs)
            )
    
    elif data[field_name].ndim == 1:  # vlen audio
        for row_idx, filename in tqdm(
            zip(range(data[field_name].shape[0]), filenames),
            total=len(filenames),
            desc=f"Extracting '{field_name}'",
            colour="green",
            leave=False
        ):
            os.makedirs(
                os.path.join(output_dir, os.path.dirname(filename)),
                exist_ok=True
            )
            audio = data[field_name][row_idx]
            write_audio(
                audio,
                file=os.path.join(output_dir, filename),
                fs=int(fs)
            ) 


def from_audioint16(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for audio data as `int16`."""
    return _from_audiodtype(
        output_csv=output_csv,
        output_yaml=output_yaml,
        output_dir=output_dir,
        dataset_name=dataset_name,
        field_name=field_name,
        data=data,
        attrs=attrs
    )


def from_audiofloat32(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for audio data as `float32`."""
    return _from_audiodtype(
        output_csv=output_csv,
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs,
    )


def from_audiofloat64(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for audio data as `float64`."""
    return _from_audiodtype(
        output_csv=output_csv,
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs
    )


def _from_dtype(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Extracts any single value data type and renders it to a `.csv` file.
    
    Args:
        output_dir (str): Output folder.
        field_name (str): Field name to extract data from.
        data (h5py.Dataset): Data from which the data will be extracted.
        attrs (h5py.AttributeManager): Attributes associated to the data.
    """
    os.makedirs(output_dir, exist_ok=True)
    df = pl.DataFrame({field_name: list(data[field_name])})
    df.write_csv(os.path.join(output_dir, f"{field_name}.csv"))


def from_int8(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for single value data as `int8`."""
    return _from_dtype(
        output_csv=output_csv,
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs
    )


def from_int16(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for single value data as `int16`."""
    return _from_dtype(
        output_csv=output_csv,
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs
    )


def from_float32(
        output_csv: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for single value data as `float32`."""
    return _from_dtype(
        output_csv=output_csv,
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs
    )


def from_float64(
        output_csv: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for single value data as `float64`."""
    return _from_dtype(
        output_csv=output_csv,
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs,
    )


def from_utf8str(
        output_csv: str,
        output_yaml: str,
        output_dir: str,
        dataset_name: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
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
    df = pl.read_csv(output_csv)
    decoded_data = [
        i.decode("utf-8") if isinstance(i, bytes)
        else i for i in data[field_name]
    ]
    df = df.with_columns(pl.Series(field_name, decoded_data, pl.Utf8))
    df.write_csv(output_csv)


def _from_listdtype(
        output_csv: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Extracts any list of numeric data types and renders it to a `.csv` file.
    
    Args:
        output_dir (str): Output folder.
        field_name (str): Field name to extract data from.
        data (h5py.Dataset): Data from which the data will be extracted.
        attrs (h5py.AttributeManager): Attributes associated to the data.
    """
    os.makedirs(output_dir, exist_ok=True)
    df = pl.DataFrame(
        {field_name: [str(r) for r in data[field_name][:].tolist()]}
    )
    df.write_csv(os.path.join(output_dir, f"{field_name}.csv"))


def from_listint8(
        output_csv: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for lists of data as `int8`."""
    return _from_listdtype(
        output_csv=output_csv,
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs
    )


def from_listint16(
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for lists of data as `int16`."""
    return _from_listdtype(
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs
    )


def from_listfloat32(
        output_csv: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for lists of data as `float32`."""
    return _from_listdtype(
        output_csv=output_csv,
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs
    )


def from_listfloat64(
        output_csv: str,
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager
) -> None:
    """Alias of generic extractor for lists of data as `float64`."""
    return _from_listdtype(
        output_csv=output_csv,
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs
    )
