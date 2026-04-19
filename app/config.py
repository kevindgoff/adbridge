import os
import yaml

_CONFIG_PATH = os.environ.get("ADBRIDGE_CONFIG_PATH", "config.yml")


def load_config() -> dict:
    try:
        with open(_CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


def get_enabled_apis() -> dict:
    config = load_config()
    apis = config.get("apis", {})
    # Default all to True if not specified
    return {
        "basis": apis.get("basis", True),
        "dv360": apis.get("dv360", True),
        "triton": apis.get("triton", True),
        "freewheel": apis.get("freewheel", True),
        "hivestack": apis.get("hivestack", True),
    }
