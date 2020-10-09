from typing import Dict, Set, List, Tuple, Optional
from collections import defaultdict, OrderedDict

from alexflow.core import AbstractTask, Storage, Output
from alexflow.helper import flatten, is_completed

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

        refcount, ephemeral_map = _to_ref_map(
            {task.task_id: task}, storage=self._storage
        )

        for key, value in refcount.items():

            self._refcount[key].update(value)

        for key, value in ephemeral_map.items():

            self._ephemeral_map[key] = self._ephemeral_map[key] and value

    def remove(self, task: AbstractTask):
        inputs = _uniq(flatten(task.input()))

        # Reduce reference count first to cover the case:
        #     One input depends on the other in a same list
        for inp in inputs:
            self._refcount[inp.key].remove(task.task_id)

        for inp in inputs:
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
    tasks: Dict[str, AbstractTask],
    only_incomplete: bool = False,
    storage: Optional[Storage] = None,
) -> Tuple[Dict[str, Set[str]], Dict[str, bool]]:
    """Get reference count dictionary
    """

    if only_incomplete:
        assert storage is not None, "storage must be given when only_incomplete = True"

    # key = Output.key, value set of task_ids who uses the output
    ref: Dict[str, Set[str]] = defaultdict(set)

    # key = Output.key, value where an output is all ephemeral or not.
    ephemeral_map: Dict[str, bool] = defaultdict(lambda: True)

    while len(tasks) > 0:

        next_tasks: Dict[str, AbstractTask] = {}

        for task_id, task in tasks.items():

            if only_incomplete:
                if is_completed(task, storage):
                    continue

            inputs = flatten(task.input())

            for inp in inputs:
                ref[inp.key].add(task_id)
                ephemeral_map[inp.key] = ephemeral_map[inp.key] and inp.ephemeral

            dependent_tasks = {inp.src_task.task_id: inp.src_task for inp in inputs}

            for dependent_task_id, task in dependent_tasks.items():
                next_tasks[dependent_task_id] = task

        tasks = next_tasks

    return ref, ephemeral_map
