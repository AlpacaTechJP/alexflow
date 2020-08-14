import os
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from glob import glob
from logging import getLogger
from pathlib import Path
from typing import Optional, List, Iterator
from uuid import uuid4

from .core import Storage, File, NotFound

logger = getLogger(__name__)


@dataclass(frozen=True)
class LocalStorage(Storage):
    """Local file system based storage class.

    Attrs:
        base_path(str): Path to the directory used as local storage.
    """

    base_path: str

    def list(self, path: Optional[str] = None) -> List["File"]:
        d = self.base_path

        if path is not None:
            d = str(Path(d).joinpath(path))

        files = list(glob(f"{d}/**", recursive=True))

        return [
            File(path=file.replace(f"{d}/", ""))
            for file in files
            if os.path.isfile(file)
        ]

    def remove(self, path: str) -> None:
        os.remove(self._namespaced_path(path))

    def get(self, path: str) -> "File":
        return File(path=path)

    def exists(self, path: str) -> bool:
        file = self._namespaced_path(path)
        return os.path.exists(file) and os.path.isfile(file)

    def makedirs(self, path, exist_ok: bool = False) -> None:
        os.makedirs(self._namespaced_path(path), exist_ok=exist_ok)

    def namespace(self, path: str) -> "LocalStorage":
        return LocalStorage(base_path=self._namespaced_path(path))

    def _namespaced_path(self, path: str) -> str:
        return str(Path(self.base_path).joinpath(path))

    @contextmanager
    def path(self, path, mode="r") -> Iterator[str]:
        assert mode in ("r", "w")

        path = str(Path(self.base_path).joinpath(path))

        if mode == "r":

            if not os.path.exists(path):
                raise NotFound(path)

            yield path

            return

        with tempfile.TemporaryDirectory() as d:

            fname = path.split("/")[-1]

            temp_path = f"{d}/{fname}"

            yield temp_path

            if os.path.isfile(temp_path) or os.path.isdir(temp_path):

                os.makedirs(os.path.dirname(path), exist_ok=True)

                temp_id = str(uuid4())
                # First move file to the path + random suffix file, then next move them to desired path
                # To avoid the issue of non-atomic move operation when they are in different filesystem.
                shutil.move(temp_path, path + "." + temp_id)
                shutil.move(path + "." + temp_id, path)

    def copy(self, path: str, target_storage: Storage):
        # If copy between local storages, then try hard link first.
        if isinstance(target_storage, LocalStorage):
            try:
                src_path = os.path.realpath(self._namespaced_path(path))

                dst_path = target_storage._namespaced_path(path)

                os.symlink(src_path, dst_path)
                return
            except OSError as e:
                logger.debug(
                    f"hard link failed ({str(e)}) "
                    "fall back to the original copy implementation"
                )
        return super().copy(path, target_storage)
