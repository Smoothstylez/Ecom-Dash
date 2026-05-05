from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import ensure_runtime_dirs
from app.services.source_sync import sync_all_sources


def main() -> None:
    parser = argparse.ArgumentParser(description="Synchronize source databases into Dashboard-Combined.")
    parser.add_argument("--force", action="store_true", help="Force copy even when files look up-to-date.")
    parser.add_argument(
        "--skip-documents",
        action="store_true",
        help="Skip bookkeeping documents sync.",
    )
    parser.add_argument(
        "--include-bookkeeping-bootstrap",
        action="store_true",
        help="Explicitly copy bookkeeping bootstrap DB/documents into runtime paths.",
    )
    args = parser.parse_args()

    ensure_runtime_dirs()
    result = sync_all_sources(
        force=bool(args.force),
        include_documents=not bool(args.skip_documents),
        include_bookkeeping_bootstrap=bool(args.include_bookkeeping_bootstrap),
    )
    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
