import polars as pl


def validate_attrs(data: dict) -> None:
    for k, v in data.items():
        if not isinstance(v, str):
            raise TypeError(
                f"Only str keys are supported. Found key '{k}' of type "
                f"'{v.__class__.__name__}'"
            )

def validate_file_as_audiofloat32(df: pl.DataFrame, col: str) -> None:
    ...

def validate_file_as_audiofloat64() -> bool:
    ...

