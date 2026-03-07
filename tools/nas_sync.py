import argparse
import os
import sys

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from config import load_config  # noqa: E402
from app.core import sync_local_spool_to_nas_if_possible  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Sync local _spool/good_and_bad to NAS if available")
    ap.add_argument("--base-dir", default=BASE_DIR)
    args = ap.parse_args()

    load_dotenv()
    cfg = load_config(args.base_dir)

    report = sync_local_spool_to_nas_if_possible(cfg)
    print(report)

    return 0 if report.get("nas_available") else 2


if __name__ == "__main__":
    raise SystemExit(main())