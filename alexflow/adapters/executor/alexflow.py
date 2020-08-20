from dataclasses import dataclass
from typing import Union, List, Dict, Optional
from collections import OrderedDict

import enum
import os
import signal

from multiprocess import Process, Manager, Queue

import queue
import traceback
import time

from ...core import Task, DynamicTask, Workflow, AbstractTask, Storage
from ...helper import is_completed, run_task, generate_task, exists_output

from ...helper import flatten


from logging import getLogger


logger = getLogger(__name__)


class Termination(Exception):
    pass


class QueueSet:
    q_in: Queue
    q_out: Queue
    q_err: Queue

    def __init__(self, manager: Manager):
        self.q_in = manager.Queue()
        self.q_out = manager.Queue()
        self.q_err = manager.Queue()


class Kind(enum.Enum):
    DONE = 1
    GENERATED = 2
    RUN = 3
    RAISE = 4


@dataclass
class Message:
    # Cases = done, generated, run
    kind: Kind
    content: Dict


class ResourceManager:
    def __init__(self, resources: Dict[str, int]):
        self._resources: Dict[str, int] = resources
        self._running: Dict[str, int] = {}

    def add(self, task: Task):
        for tag in task.tags:
            self._running[tag] = self._running.get(tag, 0) + 1

    def remove(self, task: Task):
        for tag in task.tags:
            self._running[tag] = self._running.get(tag, 0) - 1
            assert self._running[tag] >= 0

    def is_runnable(self, task: Task):
        out = []

        for tag in task.tags:

            if tag not in self._resources:
                out.append(True)
                continue

            max_concurrency = self._resources[tag]

            next_concurrency = self._running.get(tag, 0) + 1

            out.append(next_concurrency <= max_concurrency)

        return all(out)


def _execute(workflow: Workflow, workers: int, resources: Dict[str, int]):  # noqa
    buffer = 100

    manager = Manager()

    resource_manager = ResourceManager(resources)

    q_set = QueueSet(manager)

    tasks = {task.task_id: task for task in workflow.tasks.values()}

    running = []

    try:

        # started workers
        ws: List[Worker] = []
        for _ in range(workers):
            w = Worker(q_set, workflow.storage)
            w.run()
            ws.append(w)

        try:

            while len(tasks) > 0 or len(running) > 0:

                for worker in ws:
                    if not worker.is_alive():
                        raise Termination("Detected unexpectedly dead worker ")

                try:
                    while True:
                        msg: Message = q_set.q_out.get_nowait()

                        assert msg.kind in (Kind.DONE, Kind.GENERATED)

                        if msg.kind == Kind.DONE:
                            running.remove(msg.content["task"].task_id)
                            resource_manager.remove(msg.content["task"])
                        elif msg.kind == Kind.GENERATED:
                            running.remove(msg.content["task"].task_id)
                            resource_manager.remove(msg.content["task"])

                            new_tasks: Dict[str, Task] = msg.content["tasks"]
                            for task_id, task in new_tasks.items():
                                if task_id not in running:
                                    tasks[task_id] = task
                except queue.Empty:
                    pass

                try:
                    msg = q_set.q_err.get_nowait()
                    logger.error("raise[task_id={}]".format(msg.content["task_id"]))
                    print(msg.content["trace"])
                    raise Termination(
                        "Raised error on task_id={}".format(msg.content["task_id"])
                        + "trace:\n"
                        + msg.content["trace"]
                    )
                except queue.Empty:
                    pass

                if q_set.q_in.qsize() >= buffer:
                    time.sleep(0.2)
                    continue

                next_tasks = {}

                for task in tasks.values():
                    if task.task_id in running:
                        continue

                    if is_completed(task, workflow.storage):
                        continue

                    inputs = flatten(task.input())

                    dependent_tasks_to_execute = OrderedDict()

                    for inp in inputs:

                        if exists_output(inp, workflow.storage):
                            continue

                        dependent_tasks_to_execute[inp.src_task.task_id] = inp.src_task

                    # Case if there is any in-complete task
                    if len(dependent_tasks_to_execute) > 0:
                        for key, value in dependent_tasks_to_execute.items():
                            next_tasks[key] = value
                        next_tasks[task.task_id] = task
                        continue

                    if not resource_manager.is_runnable(task):
                        continue

                    q_set.q_in.put(Message(kind=Kind.RUN, content={"task": task}))

                    running.append(task.task_id)

                    resource_manager.add(task)

                tasks = next_tasks

                time.sleep(0.2)

        finally:
            time.sleep(1)
            shutdown_all(ws)
    finally:
        manager.shutdown()


def _sequential_execute(workflow: Workflow, workers: int):  # noqa
    tasks = {task.task_id: task for task in workflow.tasks.values()}

    while len(tasks) > 0:

        next_tasks = {}

        for task in tasks.values():
            if is_completed(task, workflow.storage):
                continue

            inputs = flatten(task.input())

            dependent_tasks_to_execute = OrderedDict()

            for inp in inputs:

                if exists_output(inp, workflow.storage):
                    continue

                dependent_tasks_to_execute[inp.src_task.task_id] = inp.src_task

            # Case if there is any in-complete task
            if len(dependent_tasks_to_execute) > 0:
                for key, value in dependent_tasks_to_execute.items():
                    next_tasks[key] = value
                next_tasks[task.task_id] = task
                continue

            msg: Message = _process_a_job(
                Message(kind=Kind.RUN, content={"task": task}), workflow.storage
            )

            if msg.kind == Kind.DONE:
                continue

            if msg.kind == Kind.GENERATED:
                new_tasks: Dict[str, Task] = msg.content["tasks"]

                for task_id, new_task in new_tasks.items():
                    if task_id not in tasks:
                        next_tasks[task_id] = new_task

        tasks = next_tasks


def shutdown_all(workers):
    for w in workers:
        w.kill()

    for w in workers:
        w.process.join()


def _process_a_job(msg: Message, storage: Storage) -> Message:
    if isinstance(msg.content["task"], DynamicTask):
        tasks = generate_task(msg.content["task"], storage)

        if isinstance(tasks, AbstractTask):
            tasks = [tasks]

        out = Message(
            kind=Kind.GENERATED,
            content={
                "task": msg.content["task"],
                "tasks": {task.task_id: task for task in tasks},
            },
        )

    else:
        task_id = msg.content["task"].task_id

        logger.debug("run[task_id={}]".format(task_id))

        run_task(msg.content["task"], storage)

        logger.debug("ack[task_id={}]".format(task_id))

        out = Message(kind=Kind.DONE, content={"task": msg.content["task"]})

    return out


def jobfunc(q_set: QueueSet, storage: Storage):
    """Task execution process.
    """

    try:
        # Completes every some completes to avoid the memory leaks.
        for _ in range(30):
            msg: Message = q_set.q_in.get()
            assert msg.kind == Kind.RUN
            try:
                q_set.q_out.put(_process_a_job(msg, storage))
            except Exception as e:
                trace_msg = traceback.format_exc()
                q_set.q_err.put(
                    Message(
                        kind=Kind.RAISE,
                        content={
                            "error": str(e),
                            "trace": trace_msg,
                            "task_id": msg.content["task"].task_id,
                        },
                    )
                )

    except KeyboardInterrupt:
        return


def procgen(q_set: QueueSet, storage: Storage):
    """Task generation process manager.

    Keep generate task execution process, with periodic process termination
    to avoid memory leaks.
    """

    try:
        while True:
            sub_process = Process(target=jobfunc, args=(q_set, storage))
            sub_process.start()
            sub_process.join()

            # Case when subprocess dies unexpectedly, due to the termination signal, OOMs.
            if sub_process.exitcode != 0:
                raise Termination(
                    "Detected unexpected exit (exitcode = {})".format(
                        sub_process.exitcode
                    )
                )

    except KeyboardInterrupt:
        if sub_process.is_alive():
            os.kill(sub_process.pid, signal.SIGINT)


class Worker:
    def __init__(self, q_set: QueueSet, storage: Storage):
        self.q_set = q_set
        self.storage = storage
        self.process = None

    def run(self):

        self.process = Process(target=procgen, args=(self.q_set, self.storage))

        self.process.start()

    def kill(self):
        if self.is_alive():
            os.kill(self.process.pid, signal.SIGINT)

    def is_alive(self) -> bool:
        return self.process is not None and self.process.is_alive()


def run_job(
    task: Union[Task, List[Task]],
    storage: Storage,
    n_jobs: int = 1,
    resources: Optional[Dict[str, int]] = None,
):
    """Run pipeline task through luigi.
    """
    tasks: List[Task]
    if isinstance(task, list):
        tasks = task
    else:
        tasks = [task]

    run_workflow(
        Workflow(tasks={task.task_id: task for task in tasks}, storage=storage),
        n_jobs=n_jobs,
        resources=resources,
    )


def run_workflow(
    workflow: Workflow, n_jobs: int = 1, resources: Optional[Dict[str, int]] = None
):
    logger.debug(f"start running alexflow_executor with workers = {n_jobs}")

    if n_jobs == 1:
        _sequential_execute(workflow, workers=1)
    else:
        if resources is None:
            resources = {}
        _execute(workflow, workers=n_jobs, resources=resources)
