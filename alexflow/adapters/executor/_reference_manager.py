from typing import Dict, Set
from collections import defaultdict

from alexflow.core import AbstractTask, Storage
from alexflow.helper import flatten


class ReferenceManager:
    """Manages reference count of Output object and purge once all referenced tasks are resolved.
    """

    def __init__(self, tasks: Dict[str, AbstractTask], storage: Storage):
        self._refcount: Dict[str, Set[str]] = _to_ref_map(tasks)
        self._storage: Storage = storage

    def add(self, task: AbstractTask) -> None:
        """Add new task to reference manager in case you have additional task with DynamicTask"""
        inputs = flatten(task.output())

        for inp in inputs:

            self._refcount[inp.output_id].add(task.task_id)

    def remove(self, task: AbstractTask):
        inputs = flatten(task.input())

        for inp in inputs:

            self._refcount[inp.output_id].remove(task.task_id)

            if len(self._refcount[inp.output_id]) > 0:
                continue

            if inp.ephemeral:

                inp.assign_storage(self._storage).remove()


def _to_ref_map(tasks: Dict[str, AbstractTask]) -> Dict[str, Set[str]]:
    """Get reference count dictionary
    """

    # key = output_id, value set of task_ids who uses the output
    ref: Dict[str, Set[str]] = defaultdict(set)

    while len(tasks) > 0:

        next_tasks: Dict[str, AbstractTask] = {}

        for task_id, task in tasks.items():

            inputs = flatten(task.input())

            for inp in inputs:
                ref[inp.output_id].add(task_id)

            dependent_tasks = {inp.src_task.task_id: inp.src_task for inp in inputs}

            for dependent_task_id, task in dependent_tasks.items():
                next_tasks[dependent_task_id] = task

        tasks = next_tasks

    return ref
