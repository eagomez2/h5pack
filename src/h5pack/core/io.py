import os
import soundfile as sf
from glob import glob
from typing import (
    Callable,
    List,
    Optional,
    Union
)
from .exceptions import FolderNotFoundError
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
        dir: Union[str, List[str]],
        ext: Union[str, List[str]] = ".wav",
        recursive: bool = True,
        key: Optional[Callable] = None,
) -> List[str]:
    """Returns a `list` with all the files inside folder with extension `ext`.
    It supports a recursive search and searching in more than one root folder
    at a time if `recursive=True` and `dir` is a `list` of `str`,
    respectively.

    Args:
        dir (Union[str, List[str]]): Folder(s) to be searched.
        ext (Union[str, Tuple[str]]): File extensions to be considered. Accepts
            `.*` as a wild card.
        recursive (bool): If `True`, the search inside each folder will be
            recursive.
        key (Optional[Callable]): Key function to sort the results. If it is
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


def read_audio_info(file: str) -> dict:
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
