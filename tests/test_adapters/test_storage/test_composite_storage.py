import pytest
import tempfile

from alexflow.adapters.storage.local_storage import (
    LocalStorage,
    NotFound,
    File,
)
from alexflow.adapters.storage.composite_storage import (
    CompositeStorage,
    ReadOnlyAccess,
)


@pytest.fixture
def temp_path():
    with tempfile.TemporaryDirectory() as path:
        yield path


def test_local_storage(temp_path):

    storage1 = LocalStorage(base_path=temp_path + "/dir1")
    storage2 = LocalStorage(base_path=temp_path + "/dir2")

    storage = CompositeStorage(read_only=storage1, read_write=storage2)

    assert storage.get("item.txt") == File(path="item.txt")

    assert len(storage.list()) == 0

    # Case not found
    with pytest.raises(NotFound):
        with storage.path("mypath.txt", mode="r") as path:
            pass

    # Case write
    with storage.path("mypath.txt", mode="w") as path:
        with open(path, mode="w") as f:
            f.write("ok")

    with storage.path("mypath.txt", mode="r") as path:
        with open(path) as f:
            assert f.read() == "ok"

    assert len(storage1.list()) == 0
    assert len(storage2.list()) == 1
    assert len(storage.list()) == 1
    assert storage.list()[0].path == "mypath.txt"
    assert storage.exists("mypath.txt")

    # Case remove
    storage.remove("mypath.txt")
    assert not storage.exists("mypath.txt")

    # Case - read read only
    # parepared file beforehand
    with storage1.path("readonly.txt", mode="w") as path:
        with open(path, mode="w") as f:
            f.write("ok")

    assert len(storage.list()) == 1
    assert storage.exists("readonly.txt")

    # can be readable
    with storage.path("readonly.txt", mode="r") as path:
        with open(path) as f:
            assert f.read() == "ok"

    # should not be deletable
    with pytest.raises(ReadOnlyAccess):
        storage.remove("readonly.txt")

    # should not be editable
    with pytest.raises(ReadOnlyAccess):
        with storage.path("readonly.txt", mode="w") as path:
            with open(path) as f:
                assert f.read() == "ok"

    # Case - mkdirs will create dir in read_write storage
    storage.makedirs("my/path")

    # Case - namespace
    assert storage.namespace("myname").read_only == storage1.namespace("myname")
    assert storage.namespace("myname").read_write == storage2.namespace("myname")
