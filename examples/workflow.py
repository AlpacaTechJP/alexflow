from dataclasses import dataclass

from alexflow import Task, Output, BinaryOutput, no_default, NoDefaultVar


@dataclass(frozen=True)
class Dataset(Task):
    def output(self):
        return self.build_output(BinaryOutput, key="dataset.pkl")

    def run(self, input, output):
        output.store("dataset")


@dataclass(frozen=True)
class Train(Task):
    dataset: NoDefaultVar[Output] = no_default

    def input(self):
        return self.dataset

    def output(self):
        return self.build_output(BinaryOutput, key="model.pkl")

    def run(self, input, output):
        output.store("model")


@dataclass(frozen=True)
class Predict(Task):
    model: NoDefaultVar[Output] = no_default

    def input(self):
        return self.model

    def output(self):
        return self.build_output(BinaryOutput, key="model.pkl")

    def run(self, input, output):
        output.store("Generated!")


if __name__ == "__main__":
    from alexflow.adapters.alexflow_executor import run_job
    from alexflow.adapters.storage import LocalStorage
    from alexflow.helper import load_output

    import tempfile

    # Here start the construction of workflow, with description of dependency of tasks in composition.
    dataset = Dataset()

    train = Train(dataset=dataset.output())

    predict = Predict(model=train.output())

    # Then here start the execution of the workflow. We can run a workflow over a storage interface. Here we use
    # LocalStorage with temporary directory.
    with tempfile.TemporaryDirectory() as tempdir:
        storage = LocalStorage(base_path=tempdir)

        run_job(task=predict, storage=storage)

        # Here how you can load the output of the task.
        prediction = load_output(predict.output(), storage)

    print(f"Generated prediction = {prediction}")
