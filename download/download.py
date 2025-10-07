#!/usr/bin/env python3
"""
dolma_downloader.py
--------------------
Professional-grade dataset downloader for Dolma v1.7 or similar large-scale corpora.

Features:
 - Parallel multi-threaded downloads with retries
 - Automatic logging to both console and file
 - Progress bars with tqdm
 - Skips already downloaded files

Usage:
    python dolma_downloader.py --urls urls.txt --output ./dolma_data --workers 6
"""

import argparse
import concurrent.futures
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
from tqdm import tqdm

# ---------- Configuration ----------
RETRY_LIMIT = 5
CHUNK_SIZE = 1024 * 1024  # 1 MB per chunk


# ---------- Logging Setup ----------
def setup_logger() -> logging.Logger:
    """Create a logger that writes to both console and a log file."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"dolma_download_{timestamp}.log"

    logger = logging.getLogger("DolmaDownloader")
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("[%(levelname)s] %(message)s")
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logging initialized. Log file: {log_file.resolve()}")
    return logger


# ---------- Utility Functions ----------
def safe_filename(url: str) -> str:
    """Extract clean filename from a URL."""
    return os.path.basename(urlparse(url).path)


def download_file(url: str, output_dir: Path, logger: logging.Logger) -> None:
    """Download a single file with retries and progress tracking."""
    filename = safe_filename(url)
    filepath = output_dir / filename

    if filepath.exists() and filepath.stat().st_size > 0:
        logger.info(f"Skipping existing file: {filename}")
        return

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                desc = f"{filename} (Attempt {attempt}/{RETRY_LIMIT})"

                with tqdm(total=total, unit="B", unit_scale=True, desc=desc, leave=False) as pbar:
                    with open(filepath, "wb") as f:
                        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
            logger.info(f"Completed: {filename}")
            return

        except Exception as e:
            logger.warning(f"Error downloading {filename} (Attempt {attempt}): {e}")
            time.sleep(2 * attempt)

    logger.error(f"Failed after {RETRY_LIMIT} attempts: {filename}")


def load_urls(url_input: str):
    """Load URLs from a text file or comma-separated list."""
    if os.path.isfile(url_input):
        with open(url_input, "r") as f:
            urls = [line.strip() for line in f if line.strip()]
    else:
        urls = [u.strip() for u in url_input.split(",") if u.strip()]
    return urls


# ---------- Main Function ----------
def main():
    parser = argparse.ArgumentParser(description="Download Dolma dataset shards with logging.")
    parser.add_argument("--urls", required=True, help="Path to text file containing URLs or comma-separated URLs.")
    parser.add_argument("--output", required=True, help="Output directory to save files.")
    parser.add_argument("--workers", type=int, default=4, help="Number of concurrent download threads.")
    args = parser.parse_args()

    logger = setup_logger()
    urls = load_urls(args.urls)

    if not urls:
        logger.error("No URLs provided. Exiting.")
        sys.exit(1)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting download of {len(urls)} files to {output_dir.resolve()} using {args.workers} workers.\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        list(
            tqdm(
                executor.map(lambda u: download_file(u, output_dir, logger), urls),
                total=len(urls),
                desc="Overall progress",
            )
        )

    logger.info("✅ All downloads complete.")


if __name__ == "__main__":
    main()
