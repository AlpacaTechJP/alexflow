from typing import Tuple, Any
import joblib

from dataclasses import dataclass
from alexflow.core import Output


@dataclass(frozen=True)
class Prediction:
    """Abstract class represents an output of a model.
    """

    value: Any
    kind: str


@dataclass(frozen=True)
class ReconstructionLoss(Prediction):
    kind: str = "recon_loss"


@dataclass(frozen=True)
class ClassProb(Prediction):
    kind: str = "class_prob"


@dataclass(frozen=True)
class PredictionSamples(Prediction):
    kind: str = "prediction_samples"


@dataclass(frozen=True)
class PredictionValue(Prediction):
    kind: str = "prediction_value"


@dataclass(frozen=True)
class ConfidenceInterval(Prediction):
    kind: str = "confidence_interval"


@dataclass(frozen=True)
class Contributions(Prediction):
    kind: str = "contributions"


@dataclass(frozen=True)
class Coefficients(Prediction):
    kind: str = "coefficients"


@dataclass(frozen=True)
class TValues(Prediction):
    kind: str = "t_values"


@dataclass(frozen=True)
class Bias(Prediction):
    kind: str = "bias"


@dataclass(frozen=True)
class Importances(Prediction):
    kind: str = "importances"


@dataclass
class PredictionSet:
    components: Tuple[Prediction, ...]

    def __post_init__(self):
        assert len(self.components) == len(
            set([component.kind for component in self.components])
        ), "prediction must have unique set of kinds in list"

    def get(self, kind: str) -> Any:
        for item in self.components:
            if item.kind == kind:
                return item.value
        return None


@dataclass(frozen=True)
class PredictionOutput(Output):
    def load(self) -> PredictionSet:
        raise NotImplementedError


@dataclass(frozen=True)
class BinaryPredictionOutput(PredictionOutput):
    def store(self, data: PredictionSet):
        assert self.storage is not None
        if not isinstance(data, PredictionSet):
            raise ValueError("PredictionSet expects alexflow.coreml.PredictionSet")

        with self.storage.path(self.key, mode="w") as path:
            joblib.dump(data, path)

    def load(self) -> PredictionSet:
        assert self.storage is not None
        with self.storage.path(self.key, mode="r") as path:
            return joblib.load(path)
