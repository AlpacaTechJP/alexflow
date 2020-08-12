# alexflow

Workflow library built for the compositional experiment workflow.

## Remarks

##### Support of type hints with `dataclasses`

luigi does not work well with type hints, which makes it difficult to build workflow when it is complex. With use of dataclasses, we'd like to gain benefit of type hints.

##### Build workflow by composition, rather than parameter bucket relies.

Parameter bucket rely finally build a huge global state at the entrypoint of workflow, which is pretty difficult to maintain in general as it is works similarly with global variables... Instead, we've decided to compose workflow with compositions. With this architecture we can gain the benefit of divide and conquer strategy.

##### Immutability of Task

Immutability is for distributed execution and for the strong consistency for integrity.

##### Dependency via Outputs, rather than Tasks

Description of workflow dependency by `Output` makes it easy to run partially graph.
