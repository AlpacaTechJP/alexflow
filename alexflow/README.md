Workflow Engine

## Requirements

- I want to run workflow in local development environment and distributed environment, without having any difference in usage.
- I want to use a task as inputs for the other task, so that we don't have bucket relay of parameters.
  - This is the one of the most hardest experience I have with luigi.
- I want to build flexible workflow.
  - As far as its interface is compatible, I'd like to make it pluggable, like having feature selection module or not can be abstracted and must be possible without any effort.
- I want to package them into a single container.
  - Once have the storage abstraction, we can download all the dependent artifacts for the inference and package them into a single file or single docker image.
  - So that package all dependencies and deploy it to the production.
  - So that does not need to be dependent on different docker images.
- I want to distribute task to the cluster
  - All Task must be serializable.
  - [optional] To be compatible argo workflow style?
    - Output need to have task, and Task need to depends on previous artifact or task.
- I want to use the local network storage while training and upload only some of them as artifacts
  - To optimize transfer cost between s3 and our cluster, we'd like to change the storage depends on the condition.
  - Output for the inferrence
  - Output for the analysis
- I want to use the same inferrence workflow in production environment
