__all__ = ["hash_to_float"]

from hashlib import sha256
from zlib import adler32


def _bytes_to_float(b: bytes) -> float:
    """Computes an Adler-32 checksum of data which is consistent across different python versions and platforms."""
    return float(adler32(b) & 0xFFFFFFFF) / 2 ** 32


def hash_to_float(s: str) -> float:
    encoding = "utf-8"
    b = s.encode(encoding)
    return _bytes_to_float(sha256(b).digest())
