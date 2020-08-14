import gzip
import json

from datetime import date
import os
from os.path import dirname

import numpy as np
import pandas as pd


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()

        if isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")

        if isinstance(obj, pd.Timedelta):
            return str(obj)

        if isinstance(obj, np.float32) or isinstance(obj, np.float64):
            return float(obj)

        if isinstance(obj, np.int32) or isinstance(obj, np.int64):
            return int(obj)

        return json.JSONEncoder.default(self, obj)


def dump(obj, path):
    os.makedirs(dirname(path), exist_ok=True)
    with gzip.open(path, "wb") as f:
        f.write(json.dumps(obj, cls=Encoder).encode("utf-8"))


def load(path):
    with gzip.open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))
