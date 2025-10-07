import os, yaml, datetime, re
from typing import Dict

def load_yaml(path: str) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def stamp(fmt="%Y%m%d_%H%M%S") -> str:
    return datetime.datetime.now().strftime(fmt)

def fill_vars(s: str, **vars_) -> str:
    # replace {timestamp} and any provided vars like {download_timestamp}
    for k, v in vars_.items():
        s = s.replace("{"+k+"}", str(v))
    return s

def sanitize_path(p: str) -> str:
    return re.sub(r"[\\]+", "/", p)
