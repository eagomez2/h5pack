from typing import Any


class __Singleton__(type):
    """A singleton class to be used as `metaclass`."""
    _instances = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class __Config__(metaclass=__Singleton__):
    """Internal package configuration singleton.

    !!! warning
        This singleton is not supposed to be accessed directly. Please use the
        respective setters and getters for each configuration.
    """
    def __init__(self) -> None:
        super().__init__()

        self._DEFAULT_AUDIO_IO_DTYPE = "float32"
        self._DEFAULT_AUDIO_SUBTYPE = "FLOAT"
        self._ALLOWED_AUDIO_EXTENSIONS = [
            ".aif",
            ".aiff",
            ".mp3",
            ".flac",
            ".ogg",
            ".wav",
            ".wave"
        ]


def get_allowed_audio_extensions() -> list[str]:
    """Returns the list of allowed audio file extensions.
    
    Returns:
        list[str]: List of allowed audio extensions.
    """
    return __Config__()._ALLOWED_AUDIO_EXTENSIONS
