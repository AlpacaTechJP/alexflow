import tempfile

from alexflow.adapters.storage import LocalStorage
from alexflow.adapters.luigi import run_job
from alexflow.helper import is_completed, generate_task
from alexflow.testing.tasks import Task1, Task2, DynamicTask1, WriteValue


def test_run_job():

    task = Task2(resource_spec=None, parent=Task1(resource_spec=None).output())

    with tempfile.TemporaryDirectory() as dirname:

        storage = LocalStorage(dirname)

        assert not is_completed(task.input().src_task, storage)
        assert not is_completed(task, storage)

        run_job(task, storage)

        assert is_completed(task.input().src_task, storage)
        assert is_completed(task, storage)


def test_run_dynamic_task_job():
    dynamic = DynamicTask1(
        parent=Task1(name="test-dynamic-task", resource_spec=None).output()
    )

    # Case - case independently executed

    with tempfile.TemporaryDirectory() as dirname:
        storage = LocalStorage(dirname)

        run_job(dynamic.parent.src_task, storage)

        task = generate_task(dynamic, storage)

        assert isinstance(task, WriteValue)
        assert task.value_to_write == "test-dynamic-task"

        run_job(task, storage)

        assert is_completed(task, storage)

        assert is_completed(dynamic, storage)

    assert not is_completed(dynamic, storage)

    # Case - executed at the same time
    with tempfile.TemporaryDirectory() as dirname:

        run_job(dynamic, storage)

        assert is_completed(dynamic, storage)
