"""Cargadores de configuración para YAML y ficheros de propiedades de Confluent."""

from pathlib import Path

import yaml

from macroeconomy.utils.constants import (
    CONFIG_INGESTION_FILE,
    DEFAULT_CONFLUENT_CONFIG_PATH,
)

CONFIG_DIR = Path(__file__).parent.parent / "config"


def load_configs(config_file: str = CONFIG_INGESTION_FILE) -> dict:
    """Carga un fichero YAML desde el directorio de configuración del paquete."""
    with open(CONFIG_DIR / config_file, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_confluent_config(
    config_path: str = DEFAULT_CONFLUENT_CONFIG_PATH,
) -> dict:
    """Carga un fichero de configuración de Confluent con formato clave=valor."""
    config = {}
    with open(config_path) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                parameter, value = line.split("=", 1)
                config[parameter] = value.strip()
    return config
