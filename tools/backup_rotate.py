import argparse
import os
import shutil
import zipfile
from datetime import datetime, timedelta


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def zip_dir(zipf: zipfile.ZipFile, src_dir: str, arc_prefix: str) -> None:
    src_dir = os.path.abspath(src_dir)
    if not os.path.exists(src_dir):
        return
    for root, _, files in os.walk(src_dir):
        for fn in files:
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, src_dir)
            arc = os.path.join(arc_prefix, rel).replace("\\", "/")
            zipf.write(full, arcname=arc)


def rotate_old(backup_dir: str, keep_days: int) -> list[str]:
    removed: list[str] = []
    cutoff = datetime.now() - timedelta(days=keep_days)

    for name in os.listdir(backup_dir):
        p = os.path.join(backup_dir, name)
        if not os.path.isdir(p) and not name.lower().endswith(".zip"):
            continue

        ymd = ""
        if len(name) >= 8 and name[:8].isdigit():
            ymd = name[:8]
        else:
            for part in name.split("_"):
                if len(part) >= 8 and part[:8].isdigit():
                    ymd = part[:8]
                    break

        if not ymd:
            continue
        try:
            dt = datetime.strptime(ymd, "%Y%m%d")
        except Exception:
            continue
        if dt < cutoff:
            try:
                if os.path.isdir(p):
                    shutil.rmtree(p)
                else:
                    os.remove(p)
                removed.append(p)
            except Exception:
                pass

    return removed


def main() -> int:
    ap = argparse.ArgumentParser(description="Backup users/ and _spool/ into dated zip, and rotate generations")
    ap.add_argument("--base-dir", default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))), help="project base directory")
    ap.add_argument("--backup-dir", default=None, help="backup directory (default: <base>/_backup)")
    ap.add_argument("--keep-days", type=int, default=30, help="keep days")
    ap.add_argument("--no-zip", action="store_true", help="create folder backup only")

    args = ap.parse_args()

    base_dir = os.path.abspath(args.base_dir)
    backup_dir = os.path.abspath(args.backup_dir or os.path.join(base_dir, "_backup"))

    users_dir = os.path.join(base_dir, "users")
    spool_dir = os.path.join(base_dir, "_spool")

    ensure_dir(backup_dir)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.no_zip:
        out_dir = os.path.join(backup_dir, stamp)
        ensure_dir(out_dir)
        if os.path.exists(users_dir):
            shutil.copytree(users_dir, os.path.join(out_dir, "users"), dirs_exist_ok=True)
        if os.path.exists(spool_dir):
            shutil.copytree(spool_dir, os.path.join(out_dir, "spool"), dirs_exist_ok=True)
        print(f"OK: folder backup created: {out_dir}")
    else:
        zip_path = os.path.join(backup_dir, f"backup_{stamp}.zip")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            zip_dir(z, users_dir, "users")
            zip_dir(z, spool_dir, "spool")
        print(f"OK: zip backup created: {zip_path}")

    removed = rotate_old(backup_dir, args.keep_days)
    if removed:
        print("Rotated:")
        for p in removed:
            print(f"  - {p}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())