from typing import Optional, Iterator, List
from dataclasses import dataclass
from contextlib import contextmanager

from .core import Storage, File, StorageError, NotFound


class ReadOnlyAccess(StorageError):
    pass


@dataclass(frozen=True)
class CompositeStorage(Storage):
    """Composite Storage with primary read only strage and secondary read / write storage.

    Attrs:
        read_only: Storage class used as read only operation
        read_write: Primary storage class used as read/write operation
    """

    read_only: Storage
    read_write: Storage

    def list(self, path: Optional[str] = None) -> List["File"]:
        return list(
            sorted(
                set(self.read_only.list(path) + self.read_write.list(path)),
                key=lambda x: x.path,
            )
        )

    def remove(self, path: str) -> None:
        if self.read_write.exists(path):
            self.read_write.remove(path)

        if self.read_only.exists(path):
            raise ReadOnlyAccess(path)

    def get(self, path: str) -> "File":
        return File(path=path)

    def exists(self, path: str) -> bool:
        if self.read_only.exists(path):
            return True
        if self.read_write.exists(path):
            return True
        return False

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        if self.read_only.exists(path):
            return
        if self.read_write.exists(path):
            return
        self.read_write.makedirs(path, exist_ok=exist_ok)

    def namespace(self, path: str) -> "CompositeStorage":
        return CompositeStorage(
            read_write=self.read_write.namespace(path),
            read_only=self.read_only.namespace(path),
        )

    @contextmanager
    def path(self, path: str, mode="r") -> Iterator[str]:
        assert mode in ("r", "w")

        if mode == "r":

            if self.read_only.exists(path):
                with self.read_only.path(path, mode="r") as _path:
                    yield _path
                return

            if self.read_write.exists(path):
                with self.read_write.path(path, mode="r") as _path:
                    yield _path
                return

            raise NotFound(path)

        if self.read_only.exists(path):
            raise ReadOnlyAccess(path)

        with self.read_write.path(path, mode="w") as _path:
            yield _path
