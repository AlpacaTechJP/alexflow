from typing import List, Optional
from dataclasses import field, dataclass
from dataclass_serializer import no_default, NoDefaultVar

import pytest

import time

from alexflow import Task, BinaryOutput
from alexflow.adapters.storage import LocalStorage
from alexflow.adapters.executor.alexflow import run_job
from alexflow.helper import is_completed, generate_task
from alexflow.testing.tasks import Task1, Task2, DynamicTask1, WriteValue


@pytest.fixture
def storage(tmp_path):
    yield LocalStorage(tmp_path)


@pytest.mark.parametrize("n_jobs", [1, 2])
def test_run_job(n_jobs: int, storage):

    task = Task2(resource_spec=None, parent=Task1(resource_spec=None).output())

    assert not is_completed(task.input().src_task, storage)
    assert not is_completed(task, storage)

    run_job(task, storage, n_jobs=n_jobs)

    assert is_completed(task.input().src_task, storage)
    assert is_completed(task, storage)


@dataclass(frozen=True)
class Tagged(Task):
    resources: List[str] = field(default_factory=list, compare=False)
    value: NoDefaultVar[str] = no_default
    parents: Optional[List["Tagged"]] = None

    def input(self):
        if self.parents:
            return tuple([item.output() for item in self.parents])
        return None

    def output(self):
        return self.build_output(output_class=BinaryOutput, key="output.pkl")

    @property
    def tags(self):
        return set(self.resources)

    def run(self, input, output):

        time.sleep(3)

        output.store(self.value)


@pytest.mark.parametrize(
    "concurrency, expect, comparator",
    [
        # With 1 concurrency, expected to have more than sequential time consumption.
        (1, 13.0, "gt"),
        # With 3 concurrency, at least less than 2 sequential time consumption.
        (3, 13.0 / 3.0 * 2, "lt"),
    ],
)
def test_run_with_resources(concurrency, expect, comparator, storage):

    items_limited = [
        Tagged(resources=["resource-1"], value="op1"),
        Tagged(resources=["resource-1"], value="op2"),
        Tagged(resources=["resource-1", "resource-2"], value="op3"),
    ]

    main = Tagged(parents=items_limited, value="op4")

    t = time.time()

    run_job(main, storage, resources={"resource-1": concurrency}, n_jobs=3)

    sec = time.time() - t

    if comparator == "gt":
        assert sec > expect
    elif comparator == "lt":
        assert sec < expect
    else:
        raise NotImplementedError


@pytest.mark.parametrize("n_jobs", [1, 2])
def test_run_dynamic_task_job(n_jobs, storage):
    dynamic = DynamicTask1(
        parent=Task1(name="test-dynamic-task", resource_spec=None).output()
    )

    # Case - case independently executed
    run_job(dynamic.parent.src_task, storage, n_jobs=n_jobs)

    task = generate_task(dynamic, storage)

    assert isinstance(task, WriteValue)
    assert task.value_to_write == "test-dynamic-task"

    run_job(task, storage)

    assert is_completed(task, storage)

    assert is_completed(dynamic, storage)

    _flush(storage)

    # Case - executed at the same time
    assert not is_completed(dynamic, storage), "confirm initialization of storage"

    run_job(dynamic, storage, n_jobs=n_jobs)

    assert is_completed(dynamic, storage)


@dataclass(frozen=True)
class ComplexInputTask(Task):
    def input(self):
        return {
            "value": Task1(name="task1").output(),
            "array": [Task1(name="task2").output()],
            "dict": {"task3": Task1(name="task3").output(),},
        }

    def output(self):
        return self.build_output(BinaryOutput, key="output.pkl")

    def run(self, input, output):

        assert input["value"].load()["name"] == "task1"
        assert input["array"][0].load()["name"] == "task2"

        assert input["dict"]["task3"].load()["name"] == "task3"

        output.store("value")


def test_complex_input_task(storage):
    task = ComplexInputTask()

    run_job(task, storage, n_jobs=1)

    assert is_completed(task, storage)


def _flush(storage: LocalStorage):
    for item in storage.list():
        storage.remove(item.path)
    assert len(storage.list()) == 0
