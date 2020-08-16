"""Example simple ML workflow

Notes:
    Additionally requires sklearn.
"""
from typing import Tuple
from dataclasses import dataclass, field
from sklearn import datasets, linear_model

from alexflow import Task, Output, BinaryOutput, no_default, NoDefaultVar


@dataclass(frozen=True)
class Dataset(Task):
    def output(self):
        return (
            self.build_output(BinaryOutput, key="X.pkl"),
            self.build_output(BinaryOutput, key="y.pkl"),
        )

    def run(self, input, output: Tuple[BinaryOutput, BinaryOutput]):
        X, y = datasets.load_iris(return_X_y=True)
        output[0].store(X)
        output[1].store(y)


@dataclass(frozen=True)
class Train(Task):
    X: NoDefaultVar[Output] = no_default
    y: NoDefaultVar[Output] = no_default
    model_type: NoDefaultVar[str] = field(
        default=no_default,
        metadata={"contract": lambda x: x in ("LogisticRegression", "Lasso", "Ridge")},
    )

    def input(self):
        return self.X, self.y

    def output(self):
        return self.build_output(BinaryOutput, key="model.pkl")

    def run(self, input: Tuple[BinaryOutput, BinaryOutput], output: BinaryOutput):
        X = input[0].load()
        y = input[1].load()

        model_class = getattr(linear_model, self.model_type)

        cls = model_class().fit(X, y)

        output.store(cls)


@dataclass(frozen=True)
class Predict(Task):
    model: NoDefaultVar[Output] = no_default
    X: NoDefaultVar[Output] = no_default

    def input(self):
        return self.model, self.X

    def output(self):
        return self.build_output(BinaryOutput, key="model.pkl")

    def run(self, input: BinaryOutput, output: BinaryOutput):

        clf = input[0].load()

        X = input[1].load()

        y_hat = clf.predict(X)

        output.store(y_hat)


if __name__ == "__main__":
    from alexflow.adapters.executor.alexflow import run_job
    from alexflow.adapters.storage import LocalStorage
    from alexflow.helper import load_output
    import json

    import tempfile

    # Here start the construction of workflow, with using Task and Output objects.
    dataset = Dataset()

    X, y = dataset.output()

    train = Train(X=X, y=y, model_type="Lasso")

    predict = Predict(model=train.output(), X=X)

    print("Task:")
    print(json.dumps(predict.serialize(), indent=4))

    # Then here start the execution of the workflow. We can run a workflow over a storage interface. Here we use
    # LocalStorage with temporary directory.
    with tempfile.TemporaryDirectory() as tempdir:
        storage = LocalStorage(base_path=tempdir)

        # With run_job, all the dependent task will be resolved as DAG, and executed.
        run_job(task=predict, storage=storage)

        # Here how you can load the output of the task.
        prediction = load_output(predict.output(), storage)

    print(f"Generated Prediction:")
    print(prediction)
