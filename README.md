# alexflow
ALEXFlow is a python workflow library  built for reproducible complex workflow, mainly for machine learning training.


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
