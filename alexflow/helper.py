from typing import List, Dict, Optional, Union
from dataclasses import replace

from .adapters.storage import Storage, NotFound
from .core import Output, Task, InOut, Workflow, DynamicTask, AbstractTask


def assign_storage_to_output(output: InOut, storage: Storage):
    if isinstance(output, Output):
        output = replace(output, storage=storage)
    elif isinstance(output, dict):
        output = {key: replace(value, storage=storage) for key, value in output.items()}
    elif isinstance(output, (list, tuple)):
        # Case of namedtuple
        if hasattr(output, "_fields"):
            return output.__class__(
                **{
                    key: replace(value, storage=storage)
                    for key, value in zip(output._fields, output)
                }
            )
        output = output.__class__(replace(value, storage=storage) for value in output)
    else:
        assert output is None
    return output


def flatten(inout: Optional[InOut]) -> List[Output]:
    """Make inout into list of outputs
    """
    output_list: List[Output] = []
    if inout is None:
        pass
    if isinstance(inout, (list, tuple)):
        output_list += list(inout)
    elif isinstance(inout, dict):
        output_list += list(inout.values())
    elif isinstance(inout, Output):
        output_list += [inout]
    return output_list


def is_completed(task: Task, storage: Storage) -> bool:
    """Respond the completition status of the task.

    Notes:
        If there is no outputs given, then the task will
        always executed.
    """
    try:
        outputs = task.output()

        # Case if task is dynamic and output is not defined, then try to check all the generated task's complete status.
        if isinstance(task, DynamicTask) and outputs is None:
            inputs = assign_storage_to_output(task.input(), storage)

            tasks = task.generate(inputs, None)
            if not isinstance(tasks, (list, tuple)):
                assert isinstance(tasks, AbstractTask)
                tasks = [tasks]

            return all([is_completed(task, storage=storage) for task in tasks])

        if outputs is None:
            return False

        output_list: List[Output] = flatten(outputs)

        return all(
            [
                assign_storage_to_output(output, storage).exists()
                for output in output_list
            ]
        )
    except NotFound:
        return False


def run_task(task: Task, storage: Storage):

    input = assign_storage_to_output(task.input(), storage)

    output = assign_storage_to_output(task.output(), storage)

    return task.run(input, output)


def generate_task(task: DynamicTask, storage: Storage) -> Union[Task, List[Task]]:
    """Generate task from DynamicTask
    """

    input = assign_storage_to_output(task.input(), storage)

    output = assign_storage_to_output(task.output(), storage)

    return task.generate(input, output)


def remove_output(output: Output, storage: Storage):
    output = assign_storage_to_output(output, storage)
    return output.remove()


def load_output(output: Output, storage: Storage):
    output = assign_storage_to_output(output, storage)
    return output.load()


def exists_output(output: Output, storage: Storage) -> bool:
    output = assign_storage_to_output(output, storage)
    return output.exists()

