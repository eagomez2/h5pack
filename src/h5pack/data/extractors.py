import os
import h5py
import polars as pl
from tqdm import tqdm
from ..core.io import write_audio


def _from_audiodtype(
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        verbose: bool = False
) -> None:
    os.makedirs(output_dir, exist_ok=True)
    filenames = [s.decode("utf-8") for s in data[f"{field_name}_filepaths"]]
    fs = attrs["sample_rate"]

    if data[field_name].ndim == 2:
        for row_idx, filename in tqdm(
            zip(range(data[field_name].shape[0]), filenames),
            total=len(filenames),
            desc=f"Extracting '{field_name}'",
            colour="green",
            leave=False,
            disable=not verbose
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
    
    elif data[field_name].ndim == 1:  # vlen
        for row_idx, filename in tqdm(
            zip(range(data[field_name].shape[0]), filenames),
            total=len(filenames),
            desc=f"Extracting '{field_name}'",
            colour="green",
            leave=False,
            disable=not verbose
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
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        verbose: bool = False
) -> None:
    return _from_audiodtype(
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs,
        verbose=verbose
    )


def from_audiofloat32(
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        verbose: bool = False
) -> None:
    return _from_audiodtype(
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs,
        verbose=verbose
    )


def from_audiofloat64(
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        verbose: bool = False
) -> None:
    return _from_audiodtype(
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs,
        verbose=verbose
    )


def _from_dtype(
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        verbose: bool = False
) -> None:
    os.makedirs(output_dir, exist_ok=True)
    df = pl.DataFrame({field_name: list(data[field_name])})
    df.write_csv(os.path.join(output_dir, f"{field_name}.csv"))


def from_float32(
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        verbose: bool = False
) -> None:
    return _from_dtype(
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs,
        verbose=verbose
    )


def from_float64(
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        verbose: bool = False
) -> None:
    return _from_dtype(
        output_dir=output_dir,
        field_name=field_name,
        data=data,
        attrs=attrs,
        verbose=verbose
    )


def from_utf8_str(
        output_dir: str,
        field_name: str,
        data: h5py.Dataset,
        attrs: h5py.AttributeManager,
        verbose: bool = False
) -> None:
    os.makedirs(output_dir, exist_ok=True)
    decoded_data = [
        i.decode("utf-8") if isinstance(i, bytes)
        else i for i in data[field_name]
    ]
    df = pl.DataFrame({field_name: decoded_data})
    df.write_csv(os.path.join(output_dir, f"{field_name}.csv"))
