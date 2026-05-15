from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import BinaryIO


UPLOAD_CHUNK_SIZE = 1024 * 1024


class EmptyUploadError(Exception):
    pass


class UploadTooLargeError(Exception):
    pass


def stream_fileobj_to_path(
    source: BinaryIO,
    target_path: Path,
    *,
    max_bytes: int,
    chunk_size: int = UPLOAD_CHUNK_SIZE,
) -> int:
    try:
        source.seek(0)
    except (AttributeError, OSError):
        pass

    target_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{target_path.name}.",
        suffix=".upload",
        dir=target_path.parent,
    )
    temp_path = Path(temp_name)
    total_bytes = 0

    try:
        with os.fdopen(fd, "wb") as handle:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > max_bytes:
                    raise UploadTooLargeError()
                handle.write(chunk)

        if total_bytes <= 0:
            raise EmptyUploadError()

        os.replace(temp_path, target_path)
        return total_bytes
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
