from pathlib import Path
import gzip, zstandard as zstd

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def count_lines(path: str, chunk_size=1<<20) -> int:
    # fast-ish line count
    cnt = 0
    with open(path, "rb") as f:
        while True:
            buf = f.read(chunk_size)
            if not buf: break
            cnt += buf.count(b"\n")
    return cnt

def decompress_zst(inp: Path, outp: Path, chunk=1<<14):
    dctx = zstd.ZstdDecompressor(max_window_size=2**31)
    with open(inp, "rb") as comp, open(outp, "wb") as dst:
        with dctx.stream_reader(comp) as reader:
            while True:
                block = reader.read(chunk)
                if not block: break
                dst.write(block)

def decompress_gz(inp: Path, outp: Path, chunk=1<<14):
    with gzip.open(inp, "rb") as src, open(outp, "wb") as dst:
        while True:
            block = src.read(chunk)
            if not block: break
            dst.write(block)
