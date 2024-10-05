from typing import (
    Any,
    Tuple
)
from argparse import Namespace
from abc import (
    ABC,
    abstractmethod
)


class DatasetBuilder(ABC):
    def __init__(self) -> None:
        super().__init__()
    
    @abstractmethod
    def collect_data(self, *args, **kwargs) -> Any:
        ...
    
    @abstractmethod
    def validate_data(self, *args, **kwargs) -> None:
        ...
    
    @abstractmethod
    def create_partition_specs(self, *args, **kwargs) -> dict:
        ...
    
    @abstractmethod
    def create_partition_from_specs(self, *args, **kwargs) -> Tuple[int, str]:
        ...
    
    @abstractmethod
    def create_partitions(self, args: Namespace) -> None:
        ...
