import pytest
import tempfile

from alexflow.adapters.storage.local_storage import LocalStorage, NotFound


@pytest.fixture
def temp_path():
    with tempfile.TemporaryDirectory() as path:
        yield path


def test_local_storage(temp_path):

    storage = LocalStorage(base_path=temp_path + "/dir1")

    assert len(storage.list()) == 0

    with pytest.raises(NotFound):
        with storage.path("mypath.txt", mode="r") as path:
            pass

    with storage.path("mypath.txt", mode="w") as path:
        with open(path, mode="w") as f:
            f.write("ok")

    with storage.path("mypath.txt", mode="r") as path:
        with open(path) as f:
            assert f.read() == "ok"

    assert len(storage.list()) == 1

    assert storage.list()[0].path == "mypath.txt"

    with storage.path("mypath/2nd", mode="w") as path:
        with open(path, mode="w") as f:
            f.write("ok")

    with storage.path("mypath/3nd/deep", mode="w") as path:
        with open(path, mode="w") as f:
            f.write("ok")

    assert len(storage.list()) == 3

    assert storage.exists("mypath/2nd") is True
    assert storage.exists("mypath/3rd") is False
    assert storage.exists("mypath") is False

    # Check if sub directory works.
    with storage.path("dir-creation", mode="w") as path:
        storage.makedirs(path, exist_ok=True)
        with open(path + "/subfile", mode="w") as f:
            f.write("ok")

    assert storage.exists("dir-creation/subfile") is True

    # Check if namespace works

    ns = storage.namespace("dir-creation")
    assert ns.exists("subfile")
