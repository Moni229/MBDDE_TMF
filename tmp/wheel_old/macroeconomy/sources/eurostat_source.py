"""Fuente HTTP de Eurostat para conjuntos de datos estadísticos."""

import requests
from datetime import datetime

from macroeconomy.sources.datasource import DataSource
from macroeconomy.utils.constants import EUROSTAT_CONFIG_KEY


class EurostatSource(DataSource):
    """Obtiene conjuntos de datos de Eurostat por HTTP.

    Se soportan tres maneras de filtrar por fecha:
    - `time`: lista de periodos (p. ej. ["2025-09", "2025-10"]).
    - `start_date` y `end_date`: ambos deben proporcionarse y generan
      la lista mensual entre las dos fechas (inclusive), por ejemplo
      start_date="2025-09", end_date="2025-12" ->
      time=["2025-09","2025-10","2025-11","2025-12"].

    `time` y (`start_date`/`end_date`) son mutuamente excluyentes.
    """

    def __init__(
        self,
        base_url: str = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data",
        time: list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        self.base_url = base_url

        # Validaciones básicas en el constructor: `time` es excluyente con
        # start/end y start/end deben aparecer ambos si se proporcionan.
        if time is not None and (start_date is not None or end_date is not None):
            raise ValueError("`time` is mutually exclusive with `start_date`/`end_date`")

        if (start_date is None) ^ (end_date is None):
            # XOR: sólo uno está presente
            raise ValueError("Both `start_date` and `end_date` must be provided together")

        self.time = time
        self.start_date = start_date
        self.end_date = end_date

    @property
    def config_key(self):
        return EUROSTAT_CONFIG_KEY

    @staticmethod
    def _months_range(start: str, end: str) -> list[str]:
        """Genera una lista de periodos mensuales entre `start` y `end` inclusive.

        Ambos parámetros deben tener formato "YYYY-MM".
        """
        fmt = "%Y-%m"
        try:
            start_dt = datetime.strptime(start, fmt)
            end_dt = datetime.strptime(end, fmt)
        except ValueError as exc:
            raise ValueError("start_date and end_date must have format YYYY-MM") from exc

        if start_dt > end_dt:
            raise ValueError("start_date must be <= end_date")

        months = []
        year = start_dt.year
        month = start_dt.month
        while (year, month) <= (end_dt.year, end_dt.month):
            months.append(f"{year:04d}-{month:02d}")
            # incrementar mes
            if month == 12:
                month = 1
                year += 1
            else:
                month += 1

        return months

    def read(self, dataset, **params):
        url = f"{self.base_url}/{dataset}"
        request_params = params.copy() if params else {}

        # Si la instancia tiene `time` o start/end definidas, estas sirven
        # como valores por defecto que pueden ser sobreescritos por `params`.
        # Primero comprobamos conflictos y presencia en `params`.
        param_has_time = "time" in request_params and request_params["time"] is not None
        param_has_start = "start_date" in request_params and request_params["start_date"] is not None
        param_has_end = "end_date" in request_params and request_params["end_date"] is not None

        # Conflictos entre formas de expresar el periodo
        if (self.time is not None or (self.start_date is not None and self.end_date is not None)) and param_has_time:
            raise ValueError("`time` provided both in constructor and in read params")

        if param_has_time and (param_has_start or param_has_end):
            raise ValueError("`time` is mutually exclusive with `start_date`/`end_date` in params")

        # Si params incluye start/end, validamos que estén ambas
        if param_has_start ^ param_has_end:
            raise ValueError("Both `start_date` and `end_date` must be provided together in params")

        # Prioridad: params > instance attributes
        if param_has_time:
            # se deja tal cual (se espera lista o string acorde a la API)
            pass
        elif param_has_start and param_has_end:
            # Generar lista mensual y sustituir en request_params
            request_params["time"] = self._months_range(request_params.pop("start_date"), request_params.pop("end_date"))
        elif self.time is not None:
            request_params["time"] = self.time
        elif self.start_date is not None and self.end_date is not None:
            request_params["time"] = self._months_range(self.start_date, self.end_date)

        response = requests.get(
            url,
            params=request_params,
            timeout=60,
        )

        response.raise_for_status()
        return response.json()
