import pandas as pd

from abc import abstractmethod

from dataclasses import dataclass
from typing import Dict, Union, Tuple, Generic, TypeVar
import joblib

from alexflow.core import Output


T_co = TypeVar("T_co", covariant=True)


class ArrayLike(Generic[T_co]):
    """An abstract class represents data
    """

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def __getitem__(self, index) -> T_co:
        raise NotImplementedError

    @property
    @abstractmethod
    def index(self) -> Union[pd.Index, pd.MultiIndex]:
        raise NotImplementedError

    @property
    @abstractmethod
    def shape(self) -> Tuple[int, ...]:
        raise NotImplementedError


Partition = Union[pd.Series, pd.DataFrame, ArrayLike]


@dataclass(frozen=True)
class Dataset:
    X: Union[Partition, Tuple[Partition, ...], Dict[str, Partition]]
    Y: Union[Partition, Tuple[Partition, ...], Dict[str, Partition]]

    @property
    def index(self):
        if isinstance(self.X, (tuple, list)):
            return self.X[0].index
        if isinstance(self.X, dict):
            return list(self.X.values())[0].index
        return self.X.index

    @property
    def input_shape(self):
        if isinstance(self.X, (list, tuple)):
            return [x.shape[1:] for x in self.X]
        if isinstance(self.X, dict):
            return {k: v.shape[1:] for k, v in self.X.items()}
        return self.X.shape[1:]


@dataclass(frozen=True)
class DatasetOutput(Output):
    def store(self, data: Dataset):
        raise NotImplementedError

    def load(self) -> Dataset:
        raise NotImplementedError


@dataclass(frozen=True)
class BinaryDatasetOutput(DatasetOutput):
    def store(self, data: Dataset):
        if not isinstance(data, Dataset):
            raise ValueError("DatasetOutput expects alexflow.coreml.Dataset")
        assert self.storage is not None, "storage must be given"
        with self.storage.path(self.key, mode="w") as path:
            joblib.dump(data, path)

    def load(self) -> Dataset:
        assert self.storage is not None, "storage must be given"
        with self.storage.path(self.key, mode="r") as path:
            return joblib.load(path)
