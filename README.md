# alexflow
ALEXFlow is a python workflow library built for reproducible complex workflow, mainly for machine learning training.


## Get Started

For the installation from pypi, simply install via pip.

`pip install alexflow` 


## Remarks

##### Support of type hints with `dataclasses`

luigi does not work well with type hints, which makes it difficult to build workflow when it is complex. With use of dataclasses, we'd like to gain benefit of type hints.

##### Build workflow by composition, rather than parameter bucket relies.

Parameter bucket rely finally build a huge global state at the entrypoint of workflow, which is pretty difficult to maintain in general as it is works similarly with global variables... Instead, we've decided to compose workflow with compositions. With this architecture we can gain the benefit of divide and conquer strategy.

##### Focus of reproducibility with immutability tasks

Task class is designed to be a immutable dataclass object, for distributed execution, strong consistency, and reproducibility. And also those `Task` objects can be serialized as json object, and you can easily trace the exact parameters used to generate the `Output`. 

##### Dependency via Outputs, rather than Tasks

Description of workflow dependency by `Output` makes it easy to run partially graph.

## A exmaple of Task construction

Also you can see the example workflow at `examples/workflow.py`.

```python
from typing import Tuple
from sklearn import linear_model
from dataclasses import dataclass, field
from alexflow import Task, no_default, NoDefaultVar, Output, BinaryOutput


@dataclass(frozen=True)
class Train(Task):
    # Here you can write parameter of task as dataclass fields. Task's unique id will be 
    # generated from given parameters' and each task is executed at once while the entire
    # graph computation.
    X: NoDefaultVar[Output] = no_default
    y: NoDefaultVar[Output] = no_default
    model_type: NoDefaultVar[str] = no_default
    # Here you can describe in-significant parameter with compare=False, with following
    # dataclass' object equality. Even you changed those variables, Task's unique id is
    # consistent.
    verbose: bool = field(default=True, compare=False)

    def input(self):
        """Here describes the dependent output of your task"""
        return self.X, self.y

    def output(self):
        """Here describes the dependent output of your task"""
        return self.build_output(BinaryOutput, key="model.pkl")

    def run(self, input: Tuple[BinaryOutput, BinaryOutput], output: BinaryOutput):
        # Dependent output you defined in `input()` method is available as input variable.
        X = input[0].load()
        y = input[1].load()

        model_class = getattr(linear_model, self.model_type)

        cls = model_class().fit(X, y)
        
        # And you can store what you want to output in following manner.
        output.store(cls)
```