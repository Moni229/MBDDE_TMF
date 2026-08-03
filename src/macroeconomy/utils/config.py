from pathlib import Path
import yaml

CONFIG_DIR = Path(__file__).parent.parent / "config"

def load_sources():
    with open(CONFIG_DIR / "sources.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)