from alexflow.core import Task, BinaryOutput, Output, DynamicTask
from dataclasses import dataclass


@dataclass(frozen=True)
class Task1(Task):
    name: str = "task1"

    def output(self):
        return self.build_output(output_class=BinaryOutput, key="output.pkl")

    def run(self, input, output):
        output.store({"name": self.name})


@dataclass(frozen=True)
class Task2(Task):
    parent: Output
    name: str = "task2"

    def input(self):
        return self.parent

    def output(self):
        return self.build_output(output_class=BinaryOutput, key="output.pkl")

    def run(self, input, output):
        output.store({"name": self.name})


@dataclass(frozen=True)
class WriteValue(Task):
    value_to_write: str
    target: Output

    def output(self):
        return self.target

    def run(self, input, output):
        output.store({"name": self.value_to_write})


@dataclass(frozen=True)
class DynamicTask1(DynamicTask):
    parent: Output

    def input(self):
        return self.parent

    def output(self):
        return self.build_output(output_class=BinaryOutput, key="output.pkl")

    def generate(self, input, output):
        value = input.load()["name"]
        return WriteValue(value_to_write=value, target=output, resource_spec=None)
