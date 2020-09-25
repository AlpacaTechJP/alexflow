from unittest.mock import MagicMock

from alexflow.adapters.executor._reference_manager import ReferenceManager
from alexflow.testing.tasks import Task1, Task2


def test_if_reference_manager_can_handle_mixed_ephemeral_condition():
    """
    For the case 1 output is represented as both ephemeral and none-ephemeral, the output
    object is finally none-ephemeral and should not be deleted.
    """
    storage = MagicMock()

    base = Task1()

    variant1 = Task2(parent=base.output(), name="variant1")
    variant2 = Task2(parent=base.output().as_ephemeral(), name="variant2")

    tasks = [variant1, variant2]

    manager = ReferenceManager({task.task_id: task for task in tasks}, storage)

    assert manager._ephemeral_map[base.output().key] is False

    manager.remove(base)
    manager.remove(variant1)
    manager.remove(variant2)

    storage.remove.assert_not_called()
