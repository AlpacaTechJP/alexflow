import luigi
from typing import Union, List
from collections import Mapping

from dataclass_serializer import deserialize

from ...core import Task, DynamicTask, Workflow, Storage
from ...helper import is_completed, run_task, generate_task

from logging import getLogger

logger = getLogger(__name__)


def make_dict(x):
    """
        Transforms an instance of the DictParameter value
     (_FrozenOrderedDict) into a python dictionary.
    """

    def _recursively_standardize(value):
        """
            Recursively walks ``Mapping``s and ``list``s and converts all
             ``Mapping`` to python dictionaries.
        """
        if isinstance(value, Mapping):
            return dict(((k, _recursively_standardize(v)) for k, v in value.items()))

        elif isinstance(value, list) or isinstance(value, tuple):
            return [_recursively_standardize(v) for v in value]

        return value

    return _recursively_standardize(x)


def run_workflow(workflow: Workflow, n_jobs: int = 1):
    """Run workflow through luigi
    """
    run_job(
        list(workflow.tasks.values()), storage=workflow.storage, n_jobs=n_jobs,
    )


def run_job(
    task: Union[Task, List[Task]], storage: Storage, n_jobs: int = 1, log_level="ERROR"
):
    """Run pipeline task through luigi.
    """
    assert n_jobs > 0

    tasks: List[Task]

    if isinstance(task, list):
        tasks = [to_luigi(task, storage) for task in task]
    else:
        tasks = [to_luigi(task, storage)]

    luigi.build(tasks, workers=n_jobs, log_level=log_level, local_scheduler=True)

    for task in tasks:
        if not task.complete():
            raise RuntimeError("task is not completed")


def to_luigi(task: Task, storage: Storage) -> luigi.Task:
    return SerializableTask(
        params=make_dict(task.serialize()), storage=storage.serialize()
    )


class SerializableTask(luigi.Task):
    storage = luigi.DictParameter()
    params = luigi.DictParameter()

    def complete(self):
        storage = deserialize(make_dict(self.storage))
        task = deserialize(make_dict(self.params))
        return is_completed(task, storage)

    def run(self):
        storage = deserialize(make_dict(self.storage))
        task = deserialize(make_dict(self.params))

        tasks = _find_uniq_tasks(task.input())

        deps = [to_luigi(task, storage=storage) for task in tasks]

        yield deps

        if isinstance(task, DynamicTask):
            tasks = generate_task(task, storage=storage)

            if not isinstance(tasks, list):
                tasks = [tasks]

            yield [to_luigi(value, storage=storage) for value in tasks]
        else:
            try:
                run_task(task, storage)
            except Exception as e:
                logger.error(
                    f"class = {task.__class__.__name__}, task_id = {task.task_id}"
                )
                logger.exception(e)
                raise e


def _find_uniq_tasks(x):
    out = []
    if x is None:
        pass
    elif isinstance(x, (list, tuple)):
        for xi in x:
            if isinstance(xi, (list, tuple, dict)):
                out += _find_uniq_tasks(xi)
            else:
                out += [xi.src_task]
    elif isinstance(x, dict):
        for _, xi in x.items():
            if isinstance(xi, (list, tuple, dict)):
                out += _find_uniq_tasks(xi)
            else:
                out += [xi.src_task]
    else:
        out = [x.src_task]

    task_map = {}

    for task in out:
        task_map[task.task_id] = task

    return list(task_map.values())


def _flatten(x):
    out = []
    if x is None:
        pass
    elif isinstance(x, (list, tuple)):
        for xi in x:
            if isinstance(xi, (list, tuple, dict)):
                out += _flatten(xi)
            else:
                out += [SerializableTask(params=xi.serialize())]
    elif isinstance(x, dict):
        for _, xi in x.items():
            if isinstance(xi, (list, tuple, dict)):
                out += _flatten(xi)
            else:
                out += [SerializableTask(params=xi.serialize())]
    else:
        out = [SerializableTask(params=x.serialize())]
    return out
