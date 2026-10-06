# Reading data in Python

`h5pack` includes a small Python API to read files created with `h5pack pack`, so you don't need to deal with the details of how each field is stored (e.g. FLAC audio, categorical values or multichannel audio of different lengths).

## Basic usage

```python
import h5pack

with h5pack.open("dataset.h5") as data:
    print(len(data))  # Number of rows
    print(data.fields)  # e.g. ['audio', 'speaker', 'split']

    row = data[0]  # Dict with the value of each field
    audio = row["audio"]  # np.ndarray
    fs = data.sample_rate("audio")  # e.g. 16000
```

`h5pack.open()` works with partitions and virtual datasets. Each row is returned as a `dict` where:

- Audio is returned as an `np.ndarray` with shape `(num_samples,)` for mono audio or `(num_channels, num_samples)` for multichannel audio. Audio keeps the data type of its parser (e.g. `int16` for `as_audioint16`), while audio packed with `as_audioflac` is decoded as `float32` (see `audio_dtype` below).
- Lists are returned as `np.ndarray`.
- Text and categorical values are returned as `str` (or the original integer for categorical integer columns).
- Single values are returned as `numpy` scalars.

## Reference

| Method / property                         | Description                                                   |
|-------------------------------------------|---------------------------------------------------------------|
| `h5pack.open(file, fields=None, audio_dtype="float32")` | Opens a file. `fields` selects the fields returned for each row. `audio_dtype` is the data type used to decode FLAC audio. |
| `len(data)`                               | Number of rows.                                               |
| `data[idx]`                               | Row `idx` as a `dict`. Negative indices are supported.        |
| `for row in data`                         | Iterates over all rows.                                       |
| `data.fields`                             | Fields returned for each row.                                 |
| `data.read(field, idx, decode=True)`      | Value of a single field. With `decode=False`, the raw stored value is returned (e.g. FLAC bytes or categorical codes). |
| `data.sample_rate(field)`                 | Sample rate of an audio field.                                |
| `data.categories(field)`                  | Categories of a field packed with `as_categorical`.           |
| `data.filepath(field, idx)`               | Path of the original audio file of a row.                     |
| `data.field_attrs(field)`                 | Attributes of a field.                                        |
| `data.attrs`                              | Attributes of the file (e.g. `producer` or your own `attrs`). |
| `data.data`                               | Underlying `h5py.Group` for direct access.                    |
| `data.close()`                            | Closes the file (also done when leaving a `with` block).      |

## Using it with PyTorch
The file is opened lazily the first time a row is read, and reopened in each process. This means a single `h5pack.open()` object can be shared with all workers of a `torch.utils.data.DataLoader`, which is the safe way to read HDF5 files from multiple processes.

The example below trains on random 1-second crops of the audio of each row, and uses a categorical `speaker` field as the target class:

```python
import h5pack
import torch
from torch.utils.data import Dataset, DataLoader


class AudioDataset(Dataset):
    def __init__(self, file: str, crop_len: int = 16000):
        self.data = h5pack.open(file, fields=["audio", "speaker"])
        self.speakers = self.data.categories("speaker")
        self.crop_len = crop_len

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, int]:
        row = self.data[idx]
        audio = torch.from_numpy(row["audio"]).float()

        # int16 audio is scaled to [-1, 1)
        if row["audio"].dtype.kind == "i":
            audio = audio / 32768.0

        # Random crop (or zero padding) to a fixed length
        if audio.shape[-1] >= self.crop_len:
            start = torch.randint(audio.shape[-1] - self.crop_len + 1, ())
            audio = audio[..., start:start + self.crop_len]

        else:
            audio = torch.nn.functional.pad(
                audio,
                (0, self.crop_len - audio.shape[-1])
            )

        label = self.speakers.index(row["speaker"])

        return audio, label


dataset = AudioDataset("dataset.h5")
loader = DataLoader(dataset, batch_size=32, shuffle=True, num_workers=4)

for audio, label in loader:
    ...  # audio: (32, 16000), label: (32,)
```

!!! tip
    - Create the `h5pack.open()` object in `__init__` as shown above. It does not keep the file open until the first row is read, so it is safely copied to each worker.
    - If your rows have different lengths and you need the full audio, use a custom `collate_fn` that pads each batch to its longest row instead of cropping.
    - On macOS and Windows, `DataLoader` workers are started using `spawn`, so the code that creates the `DataLoader` must be inside an `if __name__ == "__main__":` block when it is run as a script.
    - Reading from a [virtual dataset](virtual.md) is as fast as reading from its partitions, so you can train on a dataset split across many files.

## Using h5py directly
Files created with `h5pack` are regular HDF5 files, so they can be read with any HDF5 library. All fields are stored in the `data` group, and their attributes describe how they were stored (see [Parsers](parsers.md)):

```python
import h5py

with h5py.File("dataset.h5", "r") as f:
    audio = f["data"]["audio"][0]
    fs = int(f["data"]["audio"].attrs["sample_rate"])
```
