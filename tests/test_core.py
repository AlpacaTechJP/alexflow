from dataclasses import dataclass
from typing import Optional

from dataclass_serializer import deserialize

from alexflow import Task, no_default, NoDefaultVar


def test_task():
    task = Task()
    assert task.resource_spec is None


def test_task_id_for_no_default():
    """
    Task with no_default default variable instantiated with None should be equivalent to
    the Task have optional arg instantiated with no additional parameter.
    """

    @dataclass(frozen=True)
    class MyTask(Task):
        arg: Optional[NoDefaultVar[str]] = no_default

    task_id = MyTask(arg=None).task_id

    @dataclass(frozen=True)
    class MyTask(Task):
        arg: Optional[str] = None

    task_id_optional = MyTask().task_id

    assert task_id == task_id_optional

    @dataclass(frozen=True)
    class MyTask(Task):
        pass

    task_id_no_args = MyTask().task_id

    assert task_id == task_id_no_args


@dataclass(frozen=True)
class MyTask(Task):
    arg: Optional[NoDefaultVar[str]] = no_default


def test_task_id_migration_to_v100():
    new_task = MyTask(arg=None)
    assert new_task._task_spec == "1.0.0"
    assert (
        new_task.task_id == "test_core.MyTask.bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f"
    )

    old_task = deserialize(
        {"resource_spec": None, "arg": None, "__ser__": "test_core:MyTask"}
    )
    assert old_task._task_spec is None

    assert (
        old_task.task_id == "test_core.MyTask.0871e69fa5e3a73f77e3ea440a8726bd66646b14"
    )
