from pathlib import Path
from datetime import datetime
import json

import pandas as pd


class LandingWriter:

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def write(
        self,
        data,
        source: str,
        dataset: str,
        timestamp: datetime | None = None
    ):

        timestamp = timestamp or datetime.utcnow()

        folder = (
            self.base_path
            / source
            / timestamp.strftime("%Y")
            / timestamp.strftime("%m")
            / timestamp.strftime("%d")
        )

        folder.mkdir(parents=True, exist_ok=True)

        filename = timestamp.strftime("%H%M%S")

        if isinstance(data, dict):

            path = folder / f"{dataset}_{filename}.json"

            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

        elif isinstance(data, pd.DataFrame):

            path = folder / f"{dataset}_{filename}.parquet"

            data.to_parquet(path, index=False)

        else:
            raise TypeError(
                f"Tipo no soportado: {type(data)}"
            )

        return path