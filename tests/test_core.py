from typing import Optional

from dataclasses import dataclass
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
