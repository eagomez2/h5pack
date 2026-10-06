import os
import numpy as np
import soundfile as sf
from glob import glob
from collections.abc import Callable
from .guards import is_file_or_error
from .exceptions import (
    FolderNotFoundError,
    UnsupportedShapeError
)
from .utils import make_list


def add_extension(file: str, ext: str) -> str:
    """Adds an extension to a file ``str`` if it does not have it yet.
    
    Args:
        file (str): File that will be appended with an extension.
        ext (str): File extension to append.
    
    Returns:
        str: ``file`` but with the newly added extension ``str``.
    """
    return f"{file}{ext}" if not file.endswith(ext) else file


def change_extension(file: str, new_ext: str) -> str:
    file, _ = os.path.splitext(file)
    return f"{file}{new_ext}"


def add_suffix(file: str, suffix: str) -> str:
    """Adds a suffix between a filename and its extension.
    
    Args:
        file (str): File ``str``.
        suffix (str): Suffix to be appended to ``file``.
    
    Returns:
        str: ``file`` but with the new added suffix.
    """
    filename, ext = os.path.splitext(file)
    return f"{filename}{suffix}{ext}"


def get_dir_files(
        dir: str | list[str],
        ext: str | list[str] = ".wav",
        recursive: bool = True,
        key: Callable | None = None,
) -> list[str]:
    """Returns a `list` with all the files inside folder with extension `ext`.
    It supports a recursive search and searching in more than one root folder
    at a time if `recursive=True` and `dir` is a `list` of `str`,
    respectively.

    Args:
        dir (str | list[str]): Folder(s) to be searched.
        ext (str | tuple[str]): File extensions to be considered. Accepts
            `.*` as a wild card.
        recursive (bool): If `True`, the search inside each folder will be
            recursive.
        key (Callable | None): Key function to sort the results. If it is
            not provided, files will be sorted alphabetically.

    Returns:
        `list` of `str` with the path to each retrieved file.

    Raises:
        FileNotFoundError: If one of the folder(s) cannot be found.
    """
    dir = make_list(dir)
    ext = make_list(ext)

    # Check dirs exist before fetching content
    for dir_ in dir:
        if not os.path.isdir(dir_):
            raise FolderNotFoundError(f"Folder not found: '{dir_}'")

    all_files = []

    # Search dirs
    for dir_ in dir:
        for ext_ in ext:
            if recursive:
                all_files.extend(
                    list(
                        glob(os.path.join(dir_, "**", f"*{ext_}"),
                             recursive=True)
                    )
                )
            else:
                all_files.extend(list(glob(os.path.join(dir_, f"*{ext_}"))))
    
    # Filter out f olders with file-like names (e.g. ending in .wav extension)
    flagged_files = []

    for file in all_files:
        if not os.path.isfile(file):
            flagged_files.append(file)
    
    for flagged_file in flagged_files:
        all_files.remove(flagged_file)

    return sorted(all_files, key=key)


def read_audio_metadata(file: str) -> dict:
    """Reads the metadata block of an audio file.
    
    Args:
        file (str): Audio file.
    
    Returns:
        dict: Metadata of the audio file including sample rate (``fs``),
            number of channels (``num_channels``), number of samples per
            channel (``num_samples_per_channel``), duration in seconds
            (``duration_seconds``), audio format (``fmt``), and audio subtype
            (``subtype``).
    """
    info = sf.info(file, verbose=False)

    return {
        "fs": info.samplerate,
        "num_channels": info.channels,
        "num_samples_per_channel": info.frames,
        "duration_seconds": info.duration,
        "fmt": info.format,
        "subtype": info.subtype
    }


def read_audio(
        file: str,
        start: int = 0,
        frames: int | None = -1,
        stop: int | None = None,
        dtype: str = "float32",
) -> tuple[np.ndarray, int]:
    """Reads an audio file or audio file chunk and returns it as a 
    `np.ndarray`.

    Args:
        file (str): Audio file.
        start (int): Start frame for reading partial frames of the file.
        frames int | None: Number of frames to read.
        stop (int | None): End frame index for reading partial frames of the
            file.
        dtype (str): Data type used to represent the data.
    
    Returns:
        (tuple[np.ndarray, int]): `np.ndarray` representing the audio data and
            and sample rate `tuple`.
    """
    is_file_or_error(file)

    # Read audio in (num_channels, num_samples) format
    data, fs_ = sf.read(
        file,
        dtype=dtype,
        always_2d=True,
        start=start,
        stop=stop,
        frames=frames
    )

    return data.transpose(), fs_


def write_audio(
        audio: np.ndarray,
        file: str,
        fs: int,
        subtype: str | None = None,
        fmt: str | None = None,
) -> None:
    """Writes an audio tensor to a file.
    
    Args:
        audio (np.ndarray): Audio array with shape
            ``(1, num_channels, num_samples)``, ``(num_channel, num_samples)``
            or ``(num_samples)``.
        file (str): Output file.
        fs (int): Sample rate used to write the audio file.
        subtype (str | None): Subtype used to write the audio file.
        fmt (str | None): Format used to write the audio file.
    """
    # Only mono, stereo or multichannel audios are supported as 2D tensors
    if audio.ndim not in [1, 2]:
        raise UnsupportedShapeError(
            "Only 1D or 2D arrays can be written to disk as audio. Found "
            f"{audio.shape=}"
        )

    # NOTE: Assumes layout (num_channels, num_samples)
    if audio.ndim == 1:
        audio = np.expand_dims(audio, axis=-1)
    
    elif audio.ndim == 2:
            audio = audio.swapaxes(0, 1)

    else:
        raise AssertionError

    sf.write(file=file, data=audio, samplerate=fs, subtype=subtype, format=fmt)


def resample_audio(
        audio: np.ndarray,
        fs: int,
        target_fs: int,
        num_samples: int | None = None
) -> np.ndarray:
    """Resamples an audio signal using `soxr`.

    Args:
        audio (np.ndarray): Audio data with shape
            `(num_channels, num_samples)`.
        fs (int): Original sample rate.
        target_fs (int): Target sample rate.
        num_samples (int | None): If provided, the output is trimmed or zero
            padded to have exactly this number of samples per channel.

    Returns:
        (np.ndarray): Resampled audio with shape
            `(num_channels, num_samples)` and the same data type as `audio`.

    Raises:
        ModuleNotFoundError: If `soxr` is not installed.
    """
    try:
        import soxr

    except ModuleNotFoundError:
        raise ModuleNotFoundError(
            "Resampling requires 'soxr'. Install it with "
            "'pip install h5pack[resample]'"
        ) from None

    # NOTE: soxr expects (num_samples, num_channels)
    resampled = soxr.resample(audio.T, fs, target_fs).T

    if num_samples is not None:
        if resampled.shape[-1] > num_samples:
            resampled = resampled[..., :num_samples]

        elif resampled.shape[-1] < num_samples:
            pad = num_samples - resampled.shape[-1]
            resampled = np.pad(resampled, ((0, 0), (0, pad)))

    return np.ascontiguousarray(resampled, dtype=audio.dtype)
