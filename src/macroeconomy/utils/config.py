from pathlib import Path
import yaml

CONFIG_DIR = Path(__file__).parent.parent / "config"

def load_sources():
    with open(CONFIG_DIR / "sources.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_confluent_config(
    config_path: str = "/dbfs/FileStore/client_properties",
) -> dict:
    config = {}
    with open(config_path) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                config[parameter] = value.strip()
    return config
