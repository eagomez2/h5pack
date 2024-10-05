class __Singleton__(type):
    """A singleton class to be used as ``metaclass``. """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class __Config__(metaclass=__Singleton__):
    """Internal package configuration singleton.

    !!! warning
        This singleton is not supposed to be accessed directly. Please use the
        respective setters and getters for each configuration.
    """
    def __init__(self):
        super().__init__()

        self._TEXT_COLORS = {
            "red": "\033[91m",
            "green": "\033[92m",
            "blue": "\033[94m",
            "cyan": "\033[96m",
            "magenta": "\033[35m",
            "yellow": "\033[93m",
            "end_color": "\033[0m",
        }

        self._TEXT_DECORATORS = {
            "bold": "\033[1m",
            "italic": "\033[3m",
            "underline": "\033[4m",
            "end_decoration": "\033[0m",
        }

        self._TEXT_COLOR_TAGS = {
            "<error>": self._TEXT_COLORS["red"],
            "</error>": self._TEXT_COLORS["end_color"],
            "<warning>": self._TEXT_COLORS["yellow"],
            "</warning>": self._TEXT_COLORS["end_color"],
            "<green>": self._TEXT_COLORS["green"],
            "</green>": self._TEXT_COLORS["end_color"],
            "<magenta>": self._TEXT_COLORS["magenta"],
            "</magenta>": self._TEXT_COLORS["end_color"]
        }

        self._TEXT_DECORATOR_TAGS = {
            "<b>": self._TEXT_DECORATORS["bold"],
            "</b>": self._TEXT_DECORATORS["end_decoration"],
            "<i>": self._TEXT_DECORATORS["italic"],
            "</i>": self._TEXT_DECORATORS["end_decoration"],
            "<u>": self._TEXT_DECORATORS["underline"],
            "</u>": self._TEXT_DECORATORS["end_decoration"]
        }

        self._DEFAULT_AUDIO_IO_DTYPE = "float32"
        self._DEFAULT_AUDIO_SUBTYPE = "FLOAT"
        self._SINGLE_PROCESS_PROGRESS_BAR_COLOR = "green"
        self._MULTI_PROCESS_PROGRESS_BAR_COLOR = "cyan"


def _get_text_color_tags() -> dict:
    """Returns all available text color tags.
    
    Returns:
        dict: Text color tags.
    """
    return __Config__()._TEXT_COLOR_TAGS


def _get_text_decorator_tags() -> dict:
    """Returns all available decorator tags.
    
    Returns:
        dict: Decorator tags.
    """
    return __Config__()._TEXT_DECORATOR_TAGS


def _get_sppbar_color() -> str:
    """Returns the default single process progress bar color.
    
    Returns:
        str: Single process progress bar color.
    """
    return __Config__()._SINGLE_PROCESS_PROGRESS_BAR_COLOR
