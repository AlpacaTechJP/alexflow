from dataclasses import dataclass, field
from typing import Optional
import pytest

from dataclass_serializer import deserialize

from alexflow import Task, no_default, NoDefaultVar, ResourceSpec


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


class TestTaskIDIntegrity:
    def test_task_id_integrity(self):
        task = MyTask(arg=None)
        assert task._task_spec == "1.0.0"
        assert (
            task.task_id == "test_core.MyTask.bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f"
        )

    def test_if_arg_given_task_id_should_be_modified(self):
        task = MyTask(arg="test")
        assert (
            task.task_id == "test_core.MyTask.ce12fb848e4a73c2a1f34a24c58f27cf307e123e"
        )

    def test_resource_spec_should_not_affect_to_task_id(self):
        task = MyTask(arg=None, resource_spec=ResourceSpec(gpu=1))
        assert (
            task.task_id == "test_core.MyTask.bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f"
        )

    def test_if_none_comparable_field_should_not_affect_to_task_id(self):
        @dataclass(frozen=True)
        class TaskForNoneComparableField(Task):
            insignificant_field: str = field(default=no_default, compare=False)

        assert (
            TaskForNoneComparableField(insignificant_field="value1").task_id
            == TaskForNoneComparableField(insignificant_field="value2").task_id
        )


@pytest.mark.parametrize(
    "task_json",
    [
        {"resource_spec": None, "arg": None, "__ser__": "test_core:MyTask"},
        {"resource_spec": None, "__ser__": "test_core:MyTask"},
    ],
)
def test_task_id_migration_to_v100(task_json):
    # Task with none versioned should have the same task_id
    old_task = deserialize(task_json)
    assert old_task._task_spec is None
    assert (
        old_task.task_id == "test_core.MyTask.0871e69fa5e3a73f77e3ea440a8726bd66646b14"
    )
