import logging, logging.config, os, pathlib, yaml

def setup_logging(cfg_path: str):
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)
    # ensure master log dir exists
    for handler in cfg.get("handlers", {}).values():
        if "filename" in handler:
            pathlib.Path(handler["filename"]).parent.mkdir(parents=True, exist_ok=True)
    logging.config.dictConfig(cfg)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
