# flake8: noqa
import shutil
from ...core import Storage, Dir, File, StorageError, NotFound


def copy_file(file: File, src: Storage, dst: Storage, path=None):
    if path is None:
        path = file.path

    if dst.exists(path):
        return

    with src.path(file.path, mode="r") as src_path, dst.path(
        path, mode="w"
    ) as dst_path:
        shutil.copy(src_path, dst_path)
