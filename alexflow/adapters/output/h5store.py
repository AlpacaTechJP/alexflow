from dataclasses import dataclass
from contextlib import contextmanager

import pandas as pd

from alexflow.core import Output


@dataclass(frozen=True)
class H5FileOutput(Output):
    def store(self, data):
        raise NotImplementedError("store API is not supported. Use #open API instead.")

    @contextmanager
    def open(self, complevel: int = 1, complib: str = "blosc:zstd") -> pd.HDFStore:
        assert self.storage is not None, f"storage must be given for {self.key}"
        with self.storage.path(self.key, mode="w") as path:
            with pd.HDFStore(path, mode="w", complevel=complevel, complib=complib) as s:
                yield s

    @contextmanager
    def load(self) -> pd.HDFStore:
        assert self.storage is not None, f"storage must be given for {self.key}"
        with self.storage.path(self.key, mode="r") as path:
            with pd.HDFStore(path) as store:
                yield store
