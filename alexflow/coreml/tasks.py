from typing import NamedTuple
from dataclasses import dataclass

from alexflow.core import Task, JSONOutput
from .dataset import (
    BinaryDatasetOutput,
    DatasetOutput,
)
from .prediction import (
    PredictionOutput,
    BinaryPredictionOutput,
)


class BuildDatasetOpOutput(NamedTuple):
    main: DatasetOutput
    metadata: JSONOutput


@dataclass(frozen=True)
class BuildDatasetOp(Task):
    def output(self) -> BuildDatasetOpOutput:
        return BuildDatasetOpOutput(
            main=self.build_output(output_class=BinaryDatasetOutput, key="dataset.pkl"),
            metadata=self.build_output(output_class=JSONOutput, key="metadata.json.gz"),
        )


class PredictOpOutput(NamedTuple):
    main: PredictionOutput
    metadata: JSONOutput


@dataclass(frozen=True)
class PredictOp(Task):
    def output(self) -> PredictOpOutput:
        return PredictOpOutput(
            main=self.build_output(
                output_class=BinaryPredictionOutput, key="prediction.pkl"
            ),
            metadata=self.build_output(output_class=JSONOutput, key="metadata.json.gz"),
        )
