import os
import yaml

_CONFIG_PATH = os.environ.get("ADBRIDGE_CONFIG_PATH", "config.yml")


def load_config() -> dict:
    try:
        with open(_CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


def get_hivestack_settings() -> dict:
    """Hivestack seed mode. Env vars HIVESTACK_SEED / HIVESTACK_SAMPLE_DIR override config.yml.

    seed: "sample"    — load captured live records from sample_dir, synthesize the rest
          "synthetic" — generated data only
    """
    hs = load_config().get("hivestack", {}) or {}
    return {
        "seed": os.environ.get("HIVESTACK_SEED", hs.get("seed", "synthetic")).lower(),
        "sample_dir": os.environ.get("HIVESTACK_SAMPLE_DIR", hs.get("sample_dir", "data/hivestack/sample")),
    }


def get_enabled_apis() -> dict:
    config = load_config()
    apis = config.get("apis", {})
    # Default all to True if not specified
    return {
        "basiswodh": apis.get("basiswodh", True),
        "dv360": apis.get("dv360", True),
        "triton": apis.get("triton", True),
        "freewheel": apis.get("freewheel", True),
        "hivestack": apis.get("hivestack", True),
        "adswizz": apis.get("adswizz", True),
        "thetradedesk": apis.get("thetradedesk", True),
        "gam": apis.get("gam", True),
        "basisuil": apis.get("basisuil", True),
        "radioworkflow": apis.get("radioworkflow", True),
        "stackadapt": apis.get("stackadapt", True),
    }
