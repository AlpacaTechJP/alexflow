from typing import Dict, Set, List, Tuple
from collections import defaultdict, OrderedDict

from alexflow.core import AbstractTask, Storage, Output
from alexflow.helper import flatten

from logging import getLogger


logger = getLogger(__name__)


class ReferenceManager:
    """Manages reference count of Output object and purge once all referenced tasks are resolved.
    """

    def __init__(self, tasks: Dict[str, AbstractTask], storage: Storage):
        self._refcount, self._ephemeral_map = _to_ref_map(tasks)
        self._storage: Storage = storage

    def add(self, task: AbstractTask) -> None:
        """Add new task to reference manager in case you have additional task with DynamicTask"""
        inputs = _uniq(flatten(task.output()))

        for inp in inputs:

            self._refcount[inp.key].add(task.task_id)

            self._ephemeral_map[inp.key] = (
                self._ephemeral_map[inp.key] and inp.ephemeral
            )

    def remove(self, task: AbstractTask):
        inputs = _uniq(flatten(task.input()))

        for inp in inputs:

            self._refcount[inp.key].remove(task.task_id)

            _recursive_purge_if_ephemeral(
                inp,
                storage=self._storage,
                refcount=self._refcount,
                ephemeral_map=self._ephemeral_map,
            )


def _recursive_purge_if_ephemeral(
    output: Output,
    storage: Storage,
    refcount: Dict[str, Set[str]],
    ephemeral_map: Dict[str, bool],
):
    """Recursively purge the output who marked as ephemeral.
    """
    assert (
        output.key in ephemeral_map
    ), f"Output(key={output.key}) must be registered in reference count"

    if len(refcount[output.key]) > 0:
        return

    output = output.assign_storage(storage)

    # Case when sub-graph is already purged.
    if not output.exists():
        return

    if ephemeral_map[output.key]:
        logger.debug(f"Purging Output(key={output.key})")
        output.remove()

    for item in flatten(output.src_task.input()):
        _recursive_purge_if_ephemeral(
            item, storage=storage, refcount=refcount, ephemeral_map=ephemeral_map
        )


def _uniq(items: List[Output]):
    o = OrderedDict([(item.key, item) for item in items])
    return list(o.values())


def _to_ref_map(
    tasks: Dict[str, AbstractTask]
) -> Tuple[Dict[str, Set[str]], Dict[str, bool]]:
    """Get reference count dictionary
    """

    # key = Output.key, value set of task_ids who uses the output
    ref: Dict[str, Set[str]] = defaultdict(set)

    # key = Output.key, value where an output is all ephemeral or not.
    ephemeral_map: Dict[str, bool] = defaultdict(lambda: True)

    while len(tasks) > 0:

        next_tasks: Dict[str, AbstractTask] = {}

        for task_id, task in tasks.items():

            inputs = flatten(task.input())

            for inp in inputs:
                ref[inp.key].add(task_id)
                ephemeral_map[inp.key] = ephemeral_map[inp.key] and inp.ephemeral

            dependent_tasks = {inp.src_task.task_id: inp.src_task for inp in inputs}

            for dependent_task_id, task in dependent_tasks.items():
                next_tasks[dependent_task_id] = task

        tasks = next_tasks

    return ref, ephemeral_map
