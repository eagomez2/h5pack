import io
import os
import json
import h5py
import numpy as np
import soundfile as sf
from typing import Any


# ---- FIELD DECODING ---------------------------------------------------------
def is_audio_field(attrs: h5py.AttributeManager | dict) -> bool:
    """Returns `True` if a field was packed using an audio parser.

    Args:
        attrs (h5py.AttributeManager | dict): Attributes of the field.

    Returns:
        (bool): `True` if the field contains audio data.
    """
    return str(attrs.get("parser", "")).startswith("as_audio")


def is_aux_field(name: str) -> bool:
    """Returns `True` if a dataset stores auxiliary data of another field
    (e.g. the original file paths of an audio field).

    Args:
        name (str): Dataset name.

    Returns:
        (bool): `True` if the dataset is auxiliary.
    """
    return name.endswith("__filepath") or name.endswith("_filepaths")


def decode_audio(
        value: np.ndarray,
        attrs: h5py.AttributeManager | dict,
        dtype: str = "float32"
) -> np.ndarray:
    """Decodes the audio of a single row.

    Args:
        value (np.ndarray): Raw value stored in the `.h5` file.
        attrs (h5py.AttributeManager | dict): Attributes of the field.
        dtype (str): Data type used to decode audio stored as FLAC. Audio
            stored as `int16`, `float32` or `float64` keeps its data type.

    Returns:
        (np.ndarray): Audio data with shape `(num_samples,)` for mono audio
            or `(num_channels, num_samples)` for multichannel audio.
    """
    num_channels = int(attrs.get("num_channels", 1))

    if attrs.get("codec", None) == "flac":
        audio, _ = sf.read(
            io.BytesIO(np.asarray(value, dtype=np.uint8).tobytes()),
            dtype=dtype,
            always_2d=True
        )
        audio = audio.T
        return audio[0] if num_channels == 1 else audio

    if value.ndim == 1 and num_channels > 1:
        # NOTE: Multichannel audio of different lengths is stored channel by
        # channel
        return value.reshape(num_channels, -1)

    return value


def decode_value(
        value: Any,
        attrs: h5py.AttributeManager | dict,
        audio_dtype: str = "float32"
) -> Any:
    """Decodes the value of a single row of any field.

    Args:
        value (Any): Raw value stored in the `.h5` file.
        attrs (h5py.AttributeManager | dict): Attributes of the field.
        audio_dtype (str): Data type used to decode audio stored as FLAC.

    Returns:
        (Any): Decoded value. Audio and lists are returned as `np.ndarray`,
            text and categories as `str` and scalars as `numpy` scalars.
    """
    parser = str(attrs.get("parser", ""))

    if is_audio_field(attrs):
        return decode_audio(value, attrs=attrs, dtype=audio_dtype)

    if parser == "as_categorical":
        return _get_categories(attrs)[int(value)]

    if isinstance(value, bytes):
        return value.decode("utf-8")

    return value


def _get_categories(attrs: h5py.AttributeManager | dict) -> list:
    """Returns the categories of a categorical field.

    Args:
        attrs (h5py.AttributeManager | dict): Attributes of the field.

    Returns:
        (list): Categories, where the index of each category is its code.
    """
    return json.loads(attrs["categories"])


# ---- READER -----------------------------------------------------------------
class H5PackFile:
    """Reads files created with `h5pack` row by row.

    The underlying `.h5` file is opened lazily on first access and reopened
    in each process, so a single `H5PackFile` can be shared with
    `torch.utils.data.DataLoader` workers or `multiprocessing` pools.

    Args:
        file (str): `.h5` file (partition or virtual dataset).
        fields (list[str] | None): Fields to read. If `None`, all fields are
            read.
        audio_dtype (str): Data type used to decode audio stored as FLAC.

    Example:
        ```python
        import h5pack

        with h5pack.open("dataset.h5") as data:
            row = data[0]
            print(row["audio"].shape, data.sample_rate("audio"))
        ```
    """
    def __init__(
            self,
            file: str,
            fields: list[str] | None = None,
            audio_dtype: str = "float32"
    ) -> None:
        if not os.path.isfile(file):
            raise FileNotFoundError(f"File '{file}' not found")

        self.file = file
        self.audio_dtype = audio_dtype
        self._h5_file = None
        self._pid = None

        # Read structure once and close the file, so the object can be
        # pickled before any data is read
        with h5py.File(file, "r") as h5_file:
            if "data" not in h5_file:
                raise ValueError(
                    f"File '{file}' does not contain a 'data' group"
                )

            self.attrs = {
                k: (v.decode("utf-8") if isinstance(v, bytes) else v)
                for k, v in h5_file.attrs.items()
            }
            self._field_attrs = {
                name: dict(dataset.attrs)
                for name, dataset in h5_file["data"].items()
                if not is_aux_field(name)
            }
            self._len = (
                min(h5_file["data"][f].shape[0] for f in self._field_attrs)
                if len(self._field_attrs) > 0 else 0
            )
            self._aux_fields = [
                name for name in h5_file["data"] if is_aux_field(name)
            ]

        if fields is not None:
            for field in fields:
                if field not in self._field_attrs:
                    raise KeyError(
                        f"Field '{field}' not found. Available fields: "
                        f"{', '.join(self._field_attrs)}"
                    )

        self._fields = (
            list(fields) if fields is not None else list(self._field_attrs)
        )

    @property
    def fields(self) -> list[str]:
        """Returns the fields read by `__getitem__`."""
        return self._fields

    @property
    def data(self) -> h5py.Group:
        """Returns the `data` group of the underlying `.h5` file."""
        return self._get_file()["data"]

    def _get_file(self) -> h5py.File:
        """Returns the opened `.h5` file, opening it if necessary.

        Returns:
            (h5py.File): Opened `.h5` file.
        """
        # NOTE: HDF5 file handles must not be shared between processes, so
        # each process opens its own handle
        if self._h5_file is None or self._pid != os.getpid():
            self._h5_file = h5py.File(self.file, "r")
            self._pid = os.getpid()

        return self._h5_file

    def field_attrs(self, field: str) -> dict:
        """Returns the attributes of a field.

        Args:
            field (str): Field name.

        Returns:
            (dict): Field attributes.
        """
        return self._field_attrs[field]

    def is_audio(self, field: str) -> bool:
        """Returns `True` if a field contains audio data.

        Args:
            field (str): Field name.

        Returns:
            (bool): `True` if the field contains audio data.
        """
        return is_audio_field(self._field_attrs[field])

    def sample_rate(self, field: str) -> int:
        """Returns the sample rate of an audio field.

        Args:
            field (str): Audio field name.

        Returns:
            (int): Sample rate in Hz.
        """
        if not self.is_audio(field):
            raise ValueError(f"Field '{field}' does not contain audio data")

        return int(self._field_attrs[field]["sample_rate"])

    def categories(self, field: str) -> list:
        """Returns the categories of a field packed with `as_categorical`.

        Args:
            field (str): Field name.

        Returns:
            (list): Categories, where the index of each category is its code.
        """
        if self._field_attrs[field].get("parser") != "as_categorical":
            raise ValueError(f"Field '{field}' is not categorical")

        return _get_categories(self._field_attrs[field])

    def filepath(self, field: str, idx: int) -> str | None:
        """Returns the original path of the audio file of a given row.

        Args:
            field (str): Audio field name.
            idx (int): Row index.

        Returns:
            (str | None): Path relative to the folder shared by all audio
                files of the field, or `None` if paths were not stored.
        """
        for name in (f"{field}__filepath", f"{field}_filepaths"):
            if name in self._aux_fields:
                value = self.data[name][self._check_idx(idx)]
                return (
                    value.decode("utf-8") if isinstance(value, bytes)
                    else str(value)
                )

        return None

    def _check_idx(self, idx: int) -> int:
        """Validates a row index and converts negative indices.

        Args:
            idx (int): Row index.

        Returns:
            (int): Non-negative row index.
        """
        idx = int(idx)

        if idx < 0:
            idx += self._len

        if not 0 <= idx < self._len:
            raise IndexError(
                f"Row index out of range (file has {self._len} rows)"
            )

        return idx

    def read(self, field: str, idx: int, decode: bool = True) -> Any:
        """Reads the value of a single field of a given row.

        Args:
            field (str): Field name.
            idx (int): Row index.
            decode (bool): If `True`, the value is decoded (e.g. FLAC audio
                is decoded and categories are converted to `str`).

        Returns:
            (Any): Field value.
        """
        value = self.data[field][self._check_idx(idx)]

        if not decode:
            return value

        return decode_value(
            value,
            attrs=self._field_attrs[field],
            audio_dtype=self.audio_dtype
        )

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, idx: int) -> dict:
        idx = self._check_idx(idx)
        return {field: self.read(field, idx) for field in self._fields}

    def __iter__(self):
        for idx in range(self._len):
            yield self[idx]

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        state["_h5_file"] = None
        state["_pid"] = None
        return state

    def __enter__(self) -> "H5PackFile":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"H5PackFile(file='{self.file}', rows={self._len}, "
            f"fields={self._fields})"
        )

    def close(self) -> None:
        """Closes the underlying `.h5` file."""
        if self._h5_file is not None and self._pid == os.getpid():
            self._h5_file.close()

        self._h5_file = None
        self._pid = None


def open_file(
        file: str,
        fields: list[str] | None = None,
        audio_dtype: str = "float32"
) -> H5PackFile:
    """Opens a file created with `h5pack` for reading. Available as
    `h5pack.open()`.

    Args:
        file (str): `.h5` file (partition or virtual dataset).
        fields (list[str] | None): Fields to read. If `None`, all fields are
            read.
        audio_dtype (str): Data type used to decode audio stored as FLAC.

    Returns:
        (H5PackFile): Reader object.
    """
    return H5PackFile(file, fields=fields, audio_dtype=audio_dtype)
