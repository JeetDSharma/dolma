#!/usr/bin/env python3
import argparse, concurrent.futures
from pathlib import Path

from dolma.scripts.utils.config_loader import load_yaml, stamp, fill_vars
from dolma.scripts.utils.log_utils import setup_logging, get_logger
from dolma.scripts.utils.io_utils import ensure_dir, decompress_gz, decompress_zst


def decompress_file(src: Path, out_dir: Path, logger, delete_src: bool):
    if src.suffix not in {".zst", ".gz"}:
        logger.info(f"skip (unknown ext): {src.name}")
        return
    # build output name
    base = src.name
    if base.endswith(".json.jsonl"):      # 
        logger.info(f"skip (looks decompressed): {src.name}")
        return
    if base.endswith(".jsonl.zst"):
        dst = out_dir / base.replace(".jsonl.zst", ".json.jsonl")
    elif base.endswith(".jsonl.gz"):
        dst = out_dir / base.replace(".jsonl.gz", ".json.jsonl")
    elif base.endswith(".json.zst"):
        dst = out_dir / base.replace(".json.zst", ".json.jsonl")
    elif base.endswith(".json.gz"):
        dst = out_dir / base.replace(".json.gz", ".json.jsonl")
    else:
        # fallback
        dst = out_dir / (src.stem + ".jsonl")

    if dst.exists():
        logger.info(f"skip existing: {dst.name}")
        return

    try:
        if src.suffix == ".zst":
            decompress_zst(src, dst)
        else:
            decompress_gz(src, dst)
        logger.info(f"decompressed: {src.name} -> {dst.name}")
        if delete_src:
            try:
                src.unlink()
                logger.info(f"deleted source: {src.name}")
            except Exception as e:
                logger.warning(f"could not delete {src.name}: {e}")
    except Exception as e:
        logger.error(f"failed {src.name}: {e}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="path to unzip_config.yaml")
    ap.add_argument("--logging", default="dolma/configs/logging_config.yaml")
    ap.add_argument("--vars", nargs="*", default=[], help="override vars like download_timestamp=20251007_120654_download")
    args = ap.parse_args()

    setup_logging(args.logging)
    logger = get_logger("DolmaExtractor")
    cfg = load_yaml(args.config)

    kv = dict(v.split("=",1) for v in args.vars) if args.vars else {}
    ts = stamp()
    input_dir = Path(fill_vars(cfg["input_dir"], timestamp=ts, **kv))
    output_dir = Path(fill_vars(cfg["output_dir"], timestamp=ts, **kv))
    logs_dir = Path(fill_vars(cfg["logs_dir"], timestamp=ts, **kv))
    ensure_dir(output_dir); ensure_dir(logs_dir)

    files = [p for p in input_dir.iterdir() if p.is_file() and p.suffix in {".zst", ".gz"}]
    if not files:
        logger.error(f"no compressed files in {input_dir}")
        return

    workers = int(cfg.get("workers", 4))
    delete_src = bool(cfg.get("delete_compressed", False))
    logger.info(f"extract {len(files)} files -> {output_dir}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(lambda p: decompress_file(p, output_dir, logger, delete_src), files))

    logger.info("extract complete.")

if __name__ == "__main__":
    main()
