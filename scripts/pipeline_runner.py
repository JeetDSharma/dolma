#!/usr/bin/env python3
import argparse, subprocess, sys
from dolma.scripts.utils.config_loader import stamp

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=["download","unzip","both"], default="both")
    ap.add_argument("--download_cfg", default="dolma/configs/download_config.yaml")
    ap.add_argument("--unzip_cfg", default="dolma/configs/unzip_config.yaml")
    args = ap.parse_args()

    dl_ts = stamp()
    if args.stage in ("download","both"):
        subprocess.check_call([
            sys.executable, "dolma/scripts/download.py",
            "--config", args.download_cfg
        ])
    if args.stage in ("unzip","both"):
        # pass the download timestamp to unzip if needed
        subprocess.check_call([
            sys.executable, "dolma/scripts/unzip.py",
            "--config", args.unzip_cfg,
            "--vars", f"download_timestamp={dl_ts}_download"
        ])

if __name__ == "__main__":
    main()
