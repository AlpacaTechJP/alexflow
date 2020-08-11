import hashlib
import json
import shutil
import types
from abc import abstractmethod
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Union, List, Optional, Dict, Type, Tuple, TypeVar

import joblib
from cached_property import cached_property
from dataclass_serializer import Serializable, deserialize

from alexflow.misc import gjson

T = TypeVar("T")

InOut = Union[None, "Output", List["Output"], Tuple["Output", ...], Dict[str, "Output"]]


class Storage(Serializable):
    @abstractmethod
    def list(self, path: Optional[str] = None) -> Union[List["File"]]:
        raise NotImplementedError

    @abstractmethod
    def remove(self, path: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, path: str) -> "File":
        raise NotImplementedError

    @abstractmethod
    def exists(self, path: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def namespace(self, path: str) -> "Storage":
        raise NotImplementedError

    @abstractmethod
    def makedirs(self, path: str, exist_ok: bool = False):
        raise NotImplementedError

    @abstractmethod
    def path(self, path: str, mode: str = "r"):
        raise NotImplementedError

    def copy(self, path: str, target_storage: "Storage"):
        with self.path(path, mode="r") as src_path:
            with target_storage.path(path, mode="w") as dst_path:
                shutil.copy(src_path, dst_path)


@dataclass(frozen=True)
class Dir(Serializable):
    path: str


@dataclass(frozen=True)
class File(Serializable):
    path: str


@dataclass(frozen=True)
class ResourceSpec(Serializable):
    """Defines the machine resources to execute the task.

    On distributed execution across the cluster, this fields
    will be used to allocate resources for the specific job.
    """

    cpu_requests: Optional[str] = None
    cpu_limits: Optional[str] = None
    memory_requests: Optional[str] = None
    memory_limits: Optional[str] = None
    gpu: Optional[int] = None


@dataclass(frozen=True)
class AbstractTask(Serializable):
    def input(self) -> InOut:
        """Defines the dependent output items for the task.

        Only represents the placeholder for the resource, and not expected to be used
        to get the data within `Task#run` method. Instead, InOut objects after associated
        to the real `Storage` object will be given by its arguments.
        """
        return None

    def output(self) -> InOut:
        """Defines the outputs of this task.

        Only represents the placeholder for the resource, and not expected to be used
        to get the data within `Task#run` method. Instead, InOut objects after associated
        to the real `Storage` object will be given by its arguments.
        """
        return None

    @cached_property
    def task_id(self) -> str:
        """Unique id associated to the task.
        """
        return _create_task_id(self)

    def build_output(
        self, output_class: Type[T], key: str, storage: Optional[Storage] = None,
    ) -> T:
        """Create the Output class with prefix of task_id.
        """
        key = self.task_id + "." + key
        return output_class(src_task=self, key=key, storage=storage)


@dataclass(frozen=True)
class Task(AbstractTask):
    resource_spec: Optional[ResourceSpec] = field(repr=False)

    def __init__(self,  resource_spec: Optional[ResourceSpec] = None):
        object.__setattr__(self, 'resource_spec', resource_spec)


    def run(self, input: InOut, output: InOut):
        """
        Args:
            input : `Output` associated to the storage object to be used in this Task.
            output: `Output` associated to the storage object, used to store the output data.
        """
        pass


@dataclass(frozen=True)
class DynamicTask(AbstractTask):
    """
    Concept:
        We don't know how the workflow looks like, but at least we know its input / output.

    Notes:
        if output is not given, then it will use all the generated tasks to confirm if completed.
    """

    def generate(self, input: InOut, output: InOut) -> Union[Task, List[Task]]:
        """Transform DynamicTask to Task.

        Args:
            input : `Output` associated to the storage object to be used in this Task.
            output: `Output` associated to the storage object, used to store the output data.
        """
        pass


@dataclass(frozen=True)
class WrapperTask(AbstractTask):
    task: Task

    def input(self) -> InOut:
        return self.task.input()

    def output(self) -> InOut:
        return self.task.output()

    @property
    def task_id(self) -> str:
        return self.task.task_id

    def complete(self) -> bool:
        return self.task.complete()

    def run(self):
        return self.task.run()

    def build_output(
        self,
        output_class: Type[T],
        key: str,
        storage: Optional[Storage] = None,
        metadata: Optional[Dict] = None,
    ) -> T:
        return self.task.build_output(
            output_class=output_class, key=key, storage=storage, metadata=metadata,
        )


@dataclass(frozen=True)
class Output(Serializable):
    """
    Attrs:
        metadata: Additional character of the output have no effect to the identity of the output
    """

    src_task: AbstractTask
    key: str
    storage: Optional[Storage] = field(
        default=None, compare=False, repr=False,
    )

    @cached_property
    def output_id(self) -> str:
        return self.src_task.task_id + "." + self.key

    def store(self, data):
        raise NotImplementedError

    def exists(self) -> bool:
        assert self.storage is not None, f"storage must be given for {self.key}"
        return self.storage.exists(self.key)

    def remove(self):
        assert self.storage is not None, f"storage must be given for {self.key}"
        if self.storage.exists(self.key):
            self.storage.remove(self.key)

    def load(self):
        raise NotImplementedError


@dataclass(frozen=True)
class BinaryOutput(Output):
    def store(self, data):
        assert self.storage is not None, f"storage must be given for {self.key}"
        with self.storage.path(self.key, mode="w") as path:
            joblib.dump(data, path)

    def load(self):
        assert self.storage is not None, f"storage must be given for {self.key}"
        with self.storage.path(self.key, mode="r") as path:
            return joblib.load(path)


@dataclass(frozen=True)
class JSONOutput(Output):
    def store(self, data):
        assert self.storage is not None, f"storage must be given for {self.key}"
        with self.storage.path(self.key, mode="w") as path:
            gjson.dump(data, path)

    def load(self):
        assert self.storage is not None, f"storage must be given for {self.key}"
        with self.storage.path(self.key, mode="r") as path:
            return gjson.load(path)


@dataclass(frozen=True)
class SerializableOutput(Output):
    def store(self, data: Serializable):
        assert self.storage is not None, f"storage must be given for {self.key}"
        with self.storage.path(self.key, mode="w") as path:
            gjson.dump(data.serialize(), path)

    def load(self) -> Serializable:
        assert self.storage is not None, f"storage must be given for {self.key}"
        with self.storage.path(self.key, mode="r") as path:
            return deserialize(gjson.load(path))


@dataclass(frozen=True)
class Workflow(Serializable):
    storage: Storage
    tasks: Dict[str, AbstractTask]
    artifacts: Dict[str, Output] = field(default=dict)


def _create_task_id(obj):
    """Calculates the unique_id for the Task.

    Notes:
        As far as it response back the same unique_id, it is the same component
        and have exact same behaviors.
        We need to calculate dependent serializable's task_id recursively to ignore
        field compare == False.
    """

    if not isinstance(obj, Serializable):

        if isinstance(obj, (list, tuple)):
            return obj.__class__([_create_task_id(item) for item in obj])

        if isinstance(obj, dict):
            return {key: _create_task_id(item) for key, item in obj.items()}

        return _serialize(obj)

    if isinstance(obj, Output):
        return obj.output_id

    o = {}

    value_map = obj.to_dict()

    for _field in fields(obj):

        # If field.compare is false then it is not expected to be used for eq, gt.
        if not _field.compare:
            continue

        value = value_map[_field.name]

        # If default value is None and actual value is None, then we'll skip them from the
        # calculation of unique id. This is for the keep the backward compatibility of
        # unique_id after adding new field into the class. We expect for new field to have None
        # by default.
        if (value_map[_field.name] is None) and (_field.default is None):
            continue

        if isinstance(value, AbstractTask):
            # We expect Task and WrapperTask to have the same task_id, and Task#task_id and WrapperTask#task_id
            # designed for that purpose. And here we need to use it instead of calculate it here. Otherwise additional
            # wrapper layer's field information is also encoded and different task_id will be produced.
            o[_field.name] = value.task_id
            continue

        o[_field.name] = _create_task_id(value)

    basename = f"{obj.__class__.__module__}.{obj.__class__.__name__}"

    return (
        basename
        + "."
        + hashlib.sha1(json.dumps(o, sort_keys=True).encode("utf-8")).hexdigest()
    )


def _serialize(x):
    if isinstance(x, (type, types.FunctionType)):
        return f"{x.__module__}:{x.__name__}"
    if isinstance(x, types.ModuleType):
        return f"{x.__name__}"
    if isinstance(x, datetime):
        return x.isoformat()
    return x
