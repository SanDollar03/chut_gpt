import os
import re
import csv
import json
import uuid
import io
import time
import shutil
from threading import Lock
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, Any, Iterable, Optional, List, Tuple, Set

import requests
from dotenv import load_dotenv
from flask import (
    Flask, render_template, request, jsonify, session, Response,
    redirect, url_for, g, stream_with_context
)

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USERS_DIR = os.path.join(BASE_DIR, "users")

FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")
DIFY_API_BASE = (os.environ.get("DIFY_API_BASE") or "http://161.93.108.55:8890/v1").rstrip("/")
DEFAULT_DIFY_API_KEY = (os.environ.get("DIFY_API_KEY") or "").strip()

ID7_RE = re.compile(r"^\d{7}$")
DEFAULT_MODEL_KEY = "seisan"

MODELS = {
    "seisan":   {"label": "生産モデル 1.04", "api_key_env": "DIFY_API_KEY_SEISAN"},
    "hozen":    {"label": "保全モデル 1.04", "api_key_env": "DIFY_API_KEY_HOZEN"},
    "sefety":   {"label": "安全モデル 1.01", "api_key_env": "DIFY_API_KEY_SEFETY"},
    "ems":      {"label": "環境/EMSモデル 1.02", "api_key_env": "DIFY_API_KEY_EMS"},
    "genka":    {"label": "原価・経営モデル 1.01", "api_key_env": "DIFY_API_KEY_GENKA"},
    "jinji":    {"label": "人事制度モデル 1.03", "api_key_env": "DIFY_API_KEY_JINJI"},
    "iatf":     {"label": "IATFモデル 1.04", "api_key_env": "DIFY_API_KEY_IATF"},
    "security": {"label": "情報セキュリティーモデル 1.02", "api_key_env": "DIFY_API_KEY_SECURITY"},
    "miyoshi_try": {"label": "三好工場トライモデル 1.00", "api_key_env": "DIFY_API_KEY_MIYOSHI_TRY"},
}

NOTICE_PATH = os.path.join(BASE_DIR, "notice.txt")

USER_FIELDS = ["user_id", "password", "model_key", "created_at"]
HISTORY_FIELDS = ["timestamp", "role", "model_key", "thread_id", "dify_conversation_id", "content"]
THREAD_FIELDS = ["thread_id", "name", "preview", "created_at", "updated_at"]
MAP_FIELDS = ["thread_id", "model_key", "dify_conversation_id", "updated_at"]

FEEDBACK_DIR_NAS = os.environ.get("FEEDBACK_DIR_NAS") or r"\\172.27.23.54\disk1\Chuppy\good_and_bad"
FEEDBACK_DIR_LOCAL = os.path.join(BASE_DIR, "_spool", "good_and_bad")
FEEDBACK_STATE_NAME = "feedback_state.csv"
FEEDBACK_FIELDS = ["user_id", "model_key", "thread_id", "bot_ts", "kind", "saved_at", "question", "answer"]

NAS_CHECK_TTL_SEC = 5
_path_lock = Lock()
_nas_ok_cache: Optional[bool] = None
_nas_ok_checked_at: float = 0.0

MD_REBUILD_COOLDOWN_SEC = 10
_md_lock = Lock()
_last_md_rebuild_at: Dict[str, float] = {}
_dirty_months_by_model: Dict[str, Set[str]] = {}

BACKUP_DIR = os.environ.get("BACKUP_DIR") or os.path.join(BASE_DIR, "_backup")
BACKUP_KEEP_DAYS = int(os.environ.get("BACKUP_KEEP_DAYS") or "30")
MAINTENANCE_TTL_SEC = int(os.environ.get("MAINTENANCE_TTL_SEC") or "300")
_maintenance_lock = Lock()
_last_maintenance_at: float = 0.0

_file_locks: Dict[str, Lock] = {}
_file_locks_guard = Lock()


def _lock_for_path(path: str) -> Lock:
    p = os.path.abspath(path)
    with _file_locks_guard:
        lk = _file_locks.get(p)
        if lk is None:
            lk = Lock()
            _file_locks[p] = lk
        return lk


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def ensure_notice_file() -> None:
    if os.path.exists(NOTICE_PATH):
        return
    try:
        with open(NOTICE_PATH, "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass


def _csv_cache() -> Dict[str, Any]:
    c = getattr(g, "_csv_cache", None)
    if c is None:
        c = {}
        g._csv_cache = c
    return c


def csv_read_dicts_cached(path: str, fieldnames: List[str]) -> List[Dict[str, str]]:
    cache = _csv_cache()
    key = f"read::{path}"
    if key in cache:
        return cache[key]

    lk = _lock_for_path(path)
    with lk:
        if not os.path.exists(path):
            ensure_dir(os.path.dirname(path))
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()

        out: List[Dict[str, str]] = []
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                out.append({k: row.get(k, "") for k in fieldnames})

    cache[key] = out
    return out


def csv_write_dicts_atomic(path: str, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    lk = _lock_for_path(path)
    with lk:
        ensure_dir(os.path.dirname(path))
        tmp = path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in fieldnames})
        os.replace(tmp, path)

    _csv_cache().pop(f"read::{path}", None)


def csv_append_row(path: str, row: List[str]) -> None:
    lk = _lock_for_path(path)
    with lk:
        ensure_dir(os.path.dirname(path))
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(row)
    _csv_cache().pop(f"read::{path}", None)


def is_dir_writable(path: str) -> bool:
    try:
        ensure_dir(path)
        probe = os.path.join(path, ".write_test.tmp")
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(probe)
        return True
    except Exception:
        return False


def is_nas_available_cached() -> bool:
    global _nas_ok_cache, _nas_ok_checked_at
    now = time.time()
    with _path_lock:
        if _nas_ok_cache is not None and (now - _nas_ok_checked_at) < NAS_CHECK_TTL_SEC:
            return _nas_ok_cache
        ok = is_dir_writable(FEEDBACK_DIR_NAS)
        _nas_ok_cache = ok
        _nas_ok_checked_at = now
        return ok


def active_feedback_dir() -> str:
    return FEEDBACK_DIR_NAS if is_nas_available_cached() else FEEDBACK_DIR_LOCAL


def user_dir(user_id: str) -> str:
    return os.path.join(USERS_DIR, user_id)


def user_csv_path(user_id: str) -> str:
    return os.path.join(user_dir(user_id), "user.csv")


def history_csv_path(user_id: str) -> str:
    return os.path.join(user_dir(user_id), "history.csv")


def threads_csv_path(user_id: str) -> str:
    return os.path.join(user_dir(user_id), "threads.csv")


def map_csv_path(user_id: str) -> str:
    return os.path.join(user_dir(user_id), "thread_map.csv")


def ensure_csv(path: str, fieldnames: List[str]) -> None:
    if os.path.exists(path):
        return
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()


def ensure_all_user_csv(user_id: str) -> None:
    ensure_dir(user_dir(user_id))
    ensure_csv(history_csv_path(user_id), HISTORY_FIELDS)
    ensure_csv(threads_csv_path(user_id), THREAD_FIELDS)
    ensure_csv(map_csv_path(user_id), MAP_FIELDS)


def user_exists(user_id: str) -> bool:
    return os.path.exists(user_csv_path(user_id))


def load_user(user_id: str) -> Optional[Dict[str, str]]:
    p = user_csv_path(user_id)
    if not os.path.exists(p):
        return None
    ensure_all_user_csv(user_id)
    with open(p, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        row = next(r, None)
    if not row:
        return None

    mk = (row.get("model_key") or DEFAULT_MODEL_KEY).strip() or DEFAULT_MODEL_KEY
    if mk not in MODELS:
        mk = DEFAULT_MODEL_KEY

    return {
        "user_id": (row.get("user_id") or user_id).strip() or user_id,
        "password": row.get("password") or "",
        "model_key": mk,
        "created_at": row.get("created_at") or datetime.now().isoformat(timespec="seconds"),
    }


def save_user(u: Dict[str, str]) -> None:
    ensure_all_user_csv(u["user_id"])
    p = user_csv_path(u["user_id"])
    lk = _lock_for_path(p)
    with lk:
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=USER_FIELDS)
            w.writeheader()
            w.writerow({
                "user_id": u["user_id"],
                "password": u.get("password", ""),
                "model_key": u.get("model_key", DEFAULT_MODEL_KEY),
                "created_at": u.get("created_at", ""),
            })


def verify_user(user_id: str, password: str) -> bool:
    u = load_user(user_id)
    return bool(u and u.get("password", "") == password)


def create_user_files(user_id: str, password: str) -> None:
    ensure_all_user_csv(user_id)
    p = user_csv_path(user_id)
    lk = _lock_for_path(p)
    with lk:
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=USER_FIELDS)
            w.writeheader()
            w.writerow({
                "user_id": user_id,
                "password": password,
                "model_key": DEFAULT_MODEL_KEY,
                "created_at": datetime.now().isoformat(timespec="seconds"),
            })


def resolve_api_key(model_key: str) -> str:
    env_key = MODELS[model_key]["api_key_env"]
    return (os.environ.get(env_key) or "").strip() or DEFAULT_DIFY_API_KEY


def _last_prune_path(user_id: str) -> str:
    return os.path.join(user_dir(user_id), ".last_prune.txt")


def _read_last_prune(user_id: str) -> str:
    p = _last_prune_path(user_id)
    try:
        with open(p, "r", encoding="utf-8") as f:
            return (f.read() or "").strip()
    except Exception:
        return ""


def _write_last_prune(user_id: str, ymd: str) -> None:
    p = _last_prune_path(user_id)
    try:
        with open(p, "w", encoding="utf-8") as f:
            f.write(ymd)
    except Exception:
        pass


def prune_history_14days(user_id: str) -> None:
    ensure_all_user_csv(user_id)
    today = datetime.now().strftime("%Y-%m-%d")
    if _read_last_prune(user_id) == today:
        return

    cutoff = datetime.now() - timedelta(days=14)
    path = history_csv_path(user_id)

    kept: List[Dict[str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            ts = (row.get("timestamp") or "").strip()
            try:
                dt = datetime.fromisoformat(ts)
            except Exception:
                kept.append({k: row.get(k, "") for k in HISTORY_FIELDS})
                continue
            if dt >= cutoff:
                kept.append({k: row.get(k, "") for k in HISTORY_FIELDS})

    csv_write_dicts_atomic(path, HISTORY_FIELDS, kept)
    _write_last_prune(user_id, today)


def append_history(user_id: str, role: str, model_key: str, thread_id: str, dify_cid: str, content: str) -> str:
    ensure_all_user_csv(user_id)
    ts = datetime.now().isoformat(timespec="seconds")
    csv_append_row(history_csv_path(user_id), [ts, role, model_key, thread_id, dify_cid or "", content])
    prune_history_14days(user_id)
    return ts


def read_history(user_id: str, thread_id: Optional[str], limit: int = 200) -> List[Dict[str, str]]:
    if not thread_id:
        return []
    ensure_all_user_csv(user_id)
    rows = csv_read_dicts_cached(history_csv_path(user_id), HISTORY_FIELDS)
    tid = thread_id.strip()
    out: List[Dict[str, str]] = []
    for row in rows:
        if (row.get("thread_id") or "").strip() != tid:
            continue
        out.append({
            "timestamp": row.get("timestamp") or "",
            "role": row.get("role") or "",
            "model_key": row.get("model_key") or DEFAULT_MODEL_KEY,
            "thread_id": tid,
            "content": row.get("content") or "",
        })
    if len(out) > limit:
        out = out[-limit:]
    return out


def read_history_all(user_id: str, thread_id: Optional[str]) -> List[Dict[str, str]]:
    if not thread_id:
        return []
    ensure_all_user_csv(user_id)
    rows = csv_read_dicts_cached(history_csv_path(user_id), HISTORY_FIELDS)
    tid = thread_id.strip()
    out: List[Dict[str, str]] = []
    for row in rows:
        if (row.get("thread_id") or "").strip() != tid:
            continue
        out.append({
            "timestamp": row.get("timestamp") or "",
            "role": row.get("role") or "",
            "model_key": row.get("model_key") or DEFAULT_MODEL_KEY,
            "thread_id": tid,
            "content": row.get("content") or "",
        })
    return out


def _load_threads(user_id: str) -> List[Dict[str, str]]:
    ensure_all_user_csv(user_id)
    rows = csv_read_dicts_cached(threads_csv_path(user_id), THREAD_FIELDS)
    out: List[Dict[str, str]] = []
    for row in rows:
        tid = (row.get("thread_id") or "").strip()
        if not tid:
            continue
        out.append({k: row.get(k, "") for k in THREAD_FIELDS})
    return out


def _save_threads(user_id: str, rows: List[Dict[str, str]]) -> None:
    csv_write_dicts_atomic(threads_csv_path(user_id), THREAD_FIELDS, rows)


def upsert_thread(user_id: str, thread_id: str, preview: str, updated_at: str) -> None:
    rows = _load_threads(user_id)
    for r in rows:
        if (r.get("thread_id") or "") == thread_id:
            if preview and not (r.get("preview") or "").strip():
                r["preview"] = preview
            r["updated_at"] = updated_at
            _save_threads(user_id, rows)
            return
    rows.append({
        "thread_id": thread_id,
        "name": "",
        "preview": preview,
        "created_at": updated_at,
        "updated_at": updated_at,
    })
    _save_threads(user_id, rows)


def list_threads(user_id: str, limit: int = 100) -> List[Dict[str, str]]:
    rows = _load_threads(user_id)
    rows.sort(key=lambda x: (x.get("updated_at") or ""), reverse=True)
    return rows[:limit]


def rename_thread(user_id: str, thread_id: str, name: str) -> bool:
    name = (name or "").strip()
    if not thread_id or not name:
        return False
    rows = _load_threads(user_id)
    for r in rows:
        if (r.get("thread_id") or "") == thread_id:
            r["name"] = name
            r["preview"] = name[:20]
            r["updated_at"] = datetime.now().isoformat(timespec="seconds")
            _save_threads(user_id, rows)
            return True
    return False


def delete_thread(user_id: str, thread_id: str) -> bool:
    if not thread_id:
        return False

    rows = _load_threads(user_id)
    new_rows = [r for r in rows if (r.get("thread_id") or "") != thread_id]
    if len(new_rows) == len(rows):
        return False
    _save_threads(user_id, new_rows)

    kept: List[Dict[str, str]] = []
    with open(history_csv_path(user_id), newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("thread_id") or "").strip() == thread_id:
                continue
            kept.append({k: row.get(k, "") for k in HISTORY_FIELDS})
    csv_write_dicts_atomic(history_csv_path(user_id), HISTORY_FIELDS, kept)

    kept2: List[Dict[str, str]] = []
    with open(map_csv_path(user_id), newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("thread_id") or "").strip() == thread_id:
                continue
            kept2.append({k: row.get(k, "") for k in MAP_FIELDS})
    csv_write_dicts_atomic(map_csv_path(user_id), MAP_FIELDS, kept2)

    return True


def _load_map(user_id: str) -> List[Dict[str, str]]:
    ensure_all_user_csv(user_id)
    rows = csv_read_dicts_cached(map_csv_path(user_id), MAP_FIELDS)
    out: List[Dict[str, str]] = []
    for row in rows:
        tid = (row.get("thread_id") or "").strip()
        mk = (row.get("model_key") or DEFAULT_MODEL_KEY).strip() or DEFAULT_MODEL_KEY
        if not tid or not mk:
            continue
        out.append({
            "thread_id": tid,
            "model_key": mk,
            "dify_conversation_id": (row.get("dify_conversation_id") or "").strip(),
            "updated_at": row.get("updated_at") or "",
        })
    return out


def get_dify_cid(user_id: str, thread_id: str, model_key: str) -> str:
    rows = _load_map(user_id)
    for r in rows:
        if r["thread_id"] == thread_id and r["model_key"] == model_key:
            return r.get("dify_conversation_id") or ""
    return ""


def set_dify_cid(user_id: str, thread_id: str, model_key: str, dify_cid: str, updated_at: str) -> None:
    rows = _load_map(user_id)
    for r in rows:
        if r["thread_id"] == thread_id and r["model_key"] == model_key:
            r["dify_conversation_id"] = dify_cid
            r["updated_at"] = updated_at
            csv_write_dicts_atomic(map_csv_path(user_id), MAP_FIELDS, rows)
            return
    rows.append({
        "thread_id": thread_id,
        "model_key": model_key,
        "dify_conversation_id": dify_cid,
        "updated_at": updated_at,
    })
    csv_write_dicts_atomic(map_csv_path(user_id), MAP_FIELDS, rows)


def feedback_state_csv_path(dir_path: str) -> str:
    return os.path.join(dir_path, FEEDBACK_STATE_NAME)


def ensure_feedback_state_csv(dir_path: str) -> None:
    ensure_dir(dir_path)
    path = feedback_state_csv_path(dir_path)
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FEEDBACK_FIELDS)
        w.writeheader()


def _safe_filename_part(s: str) -> str:
    s = (s or "").strip() or "unknown"
    return re.sub(r"[\\/:*?\"<>|]+", "_", s)


def _yyyymm_from_iso(iso: str) -> str:
    try:
        dt = datetime.fromisoformat((iso or "").strip())
        return dt.strftime("%Y%m")
    except Exception:
        return datetime.now().strftime("%Y%m")


def _feedback_md_path(dir_path: str, model_key: str, kind: str, yyyymm: str) -> str:
    mk = _safe_filename_part(model_key)
    kd = "good" if kind == "good" else "bad"
    ym = re.sub(r"\D", "", (yyyymm or ""))[:6] or datetime.now().strftime("%Y%m")
    return os.path.join(dir_path, f"{mk}_{kd}_{ym}.md")


def _feedback_key(user_id: str, model_key: str, thread_id: str, bot_ts: str) -> str:
    return f"{user_id}||{model_key}||{thread_id}||{bot_ts}"


def _load_feedback_state_from(dir_path: str) -> List[Dict[str, str]]:
    ensure_feedback_state_csv(dir_path)
    path = feedback_state_csv_path(dir_path)
    lk = _lock_for_path(path)
    with lk:
        out: List[Dict[str, str]] = []
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                out.append({k: row.get(k, "") for k in FEEDBACK_FIELDS})
        return out


def _save_feedback_state_to(dir_path: str, rows: List[Dict[str, str]]) -> None:
    ensure_feedback_state_csv(dir_path)
    path = feedback_state_csv_path(dir_path)
    csv_write_dicts_atomic(path, FEEDBACK_FIELDS, rows)


def _merge_feedback_rows(primary: List[Dict[str, str]], secondary: List[Dict[str, str]]) -> List[Dict[str, str]]:
    def ts_key(x: Dict[str, str]) -> str:
        return (x.get("saved_at") or "").strip()

    m: Dict[str, Dict[str, str]] = {}
    for r in primary:
        k = _feedback_key(r.get("user_id", ""), r.get("model_key", ""), r.get("thread_id", ""), r.get("bot_ts", ""))
        m[k] = r
    for r in secondary:
        k = _feedback_key(r.get("user_id", ""), r.get("model_key", ""), r.get("thread_id", ""), r.get("bot_ts", ""))
        if k not in m:
            m[k] = r
            continue
        if ts_key(r) >= ts_key(m[k]):
            m[k] = r
    return list(m.values())


def load_feedback_state_merged() -> List[Dict[str, str]]:
    rows_nas: List[Dict[str, str]] = []
    rows_local: List[Dict[str, str]] = []

    try:
        if os.path.exists(feedback_state_csv_path(FEEDBACK_DIR_NAS)) or is_nas_available_cached():
            if os.path.exists(feedback_state_csv_path(FEEDBACK_DIR_NAS)):
                rows_nas = _load_feedback_state_from(FEEDBACK_DIR_NAS)
    except Exception:
        rows_nas = []

    try:
        if os.path.exists(feedback_state_csv_path(FEEDBACK_DIR_LOCAL)):
            rows_local = _load_feedback_state_from(FEEDBACK_DIR_LOCAL)
    except Exception:
        rows_local = []

    return _merge_feedback_rows(rows_nas, rows_local)


def upsert_feedback_state_to_dir(
    *,
    dir_path: str,
    user_id: str,
    model_key: str,
    thread_id: str,
    bot_ts: str,
    kind: str,
    saved_at: str,
    question: str,
    answer: str,
) -> None:
    rows = _load_feedback_state_from(dir_path)
    key = _feedback_key(user_id, model_key, thread_id, bot_ts)

    out: List[Dict[str, str]] = []
    found = False
    for r in rows:
        rk = _feedback_key(r.get("user_id", ""), r.get("model_key", ""), r.get("thread_id", ""), r.get("bot_ts", ""))
        if rk != key:
            out.append(r)
            continue

        found = True
        if kind == "none":
            continue

        r2 = dict(r)
        r2["kind"] = kind
        r2["saved_at"] = saved_at
        r2["question"] = question
        r2["answer"] = answer
        out.append(r2)

    if (not found) and kind != "none":
        out.append({
            "user_id": user_id,
            "model_key": model_key,
            "thread_id": thread_id,
            "bot_ts": bot_ts,
            "kind": kind,
            "saved_at": saved_at,
            "question": question,
            "answer": answer,
        })

    _save_feedback_state_to(dir_path, out)


def _md_chunk(saved_at: str, user_id: str, model_key: str, question: str, answer: str) -> str:
    return (
        "***\n"
        f"- saved_at: {saved_at}\n"
        f"- user_id: {user_id}\n"
        f"- model: {model_key}\n"
        "## Q\n"
        f"{(question or '').rstrip()}\n\n"
        "## A\n"
        f"{(answer or '').rstrip()}\n"
    )


def append_feedback_md(
    *,
    dir_path: str,
    model_key: str,
    kind: str,
    saved_at: str,
    user_id: str,
    question: str,
    answer: str,
) -> str:
    ensure_dir(dir_path)
    ym = _yyyymm_from_iso(saved_at)
    path = _feedback_md_path(dir_path, model_key, kind, ym)
    lk = _lock_for_path(path)
    with lk:
        with open(path, "a", encoding="utf-8", newline="\n") as f:
            f.write(_md_chunk(saved_at, user_id, model_key, question, answer))
    return ym


def rebuild_feedback_md_for_model_months_in_dir(
    dir_path: str,
    model_key: str,
    months: Set[str],
) -> None:
    ensure_dir(dir_path)
    rows = _load_feedback_state_from(dir_path)
    mk = model_key

    targets = {re.sub(r"\D", "", m)[:6] for m in months if m}
    targets.discard("")
    if not targets:
        return

    buckets: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for r in rows:
        if r.get("model_key") != mk:
            continue
        kind = (r.get("kind") or "").strip().lower()
        if kind not in ("good", "bad"):
            continue
        ym = _yyyymm_from_iso(r.get("saved_at", ""))
        if ym not in targets:
            continue
        buckets.setdefault((kind, ym), []).append(r)

    mk_safe = _safe_filename_part(mk)
    for ym in targets:
        for kd in ("good", "bad"):
            p = os.path.join(dir_path, f"{mk_safe}_{kd}_{ym}.md")
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

    for (kind, ym), lst in buckets.items():
        lst.sort(key=lambda x: x.get("saved_at", ""), reverse=True)
        path = _feedback_md_path(dir_path, mk, kind, ym)
        tmp = path + ".tmp"
        lk = _lock_for_path(path)
        with lk:
            with open(tmp, "w", encoding="utf-8", newline="\n") as f:
                for r in lst:
                    f.write(_md_chunk(
                        r.get("saved_at", ""),
                        r.get("user_id", ""),
                        r.get("model_key", ""),
                        r.get("question", ""),
                        r.get("answer", ""),
                    ))
            os.replace(tmp, path)


def mark_dirty_month(model_key: str, yyyymm: str) -> None:
    ym = re.sub(r"\D", "", (yyyymm or ""))[:6]
    if not ym:
        return
    _dirty_months_by_model.setdefault(model_key, set()).add(ym)


def maybe_rebuild_dirty_months(dir_path: str, model_key: str) -> None:
    now = time.time()
    with _md_lock:
        dirty = _dirty_months_by_model.get(model_key, set())
        if not dirty:
            return
        last = _last_md_rebuild_at.get(model_key, 0.0)
        if (now - last) < MD_REBUILD_COOLDOWN_SEC:
            return
        months = set(dirty)
        _dirty_months_by_model[model_key] = set()
        _last_md_rebuild_at[model_key] = now
    rebuild_feedback_md_for_model_months_in_dir(dir_path, model_key, months)


def sync_local_spool_to_nas_if_possible() -> None:
    if not is_nas_available_cached():
        return
    local_csv = feedback_state_csv_path(FEEDBACK_DIR_LOCAL)
    if not os.path.exists(local_csv):
        return

    try:
        rows_local = _load_feedback_state_from(FEEDBACK_DIR_LOCAL)
    except Exception:
        return
    if not rows_local:
        return

    try:
        ensure_feedback_state_csv(FEEDBACK_DIR_NAS)
        rows_nas = _load_feedback_state_from(FEEDBACK_DIR_NAS)
        merged = _merge_feedback_rows(rows_nas, rows_local)
        _save_feedback_state_to(FEEDBACK_DIR_NAS, merged)

        mk_to_months: Dict[str, Set[str]] = {}
        for r in rows_local:
            mk = (r.get("model_key") or "").strip()
            ym = _yyyymm_from_iso(r.get("saved_at", ""))
            if mk and ym:
                mk_to_months.setdefault(mk, set()).add(ym)

        for mk, months in mk_to_months.items():
            try:
                rebuild_feedback_md_for_model_months_in_dir(FEEDBACK_DIR_NAS, mk, months)
            except Exception:
                pass

        bak = local_csv + f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            os.replace(local_csv, bak)
        except Exception:
            try:
                os.remove(local_csv)
            except Exception:
                pass

    except Exception:
        return


def list_feedback_state_for_user_thread(
    *,
    user_id: str,
    thread_id: str,
    model_key: Optional[str],
) -> List[Dict[str, str]]:
    rows = load_feedback_state_merged()
    out: List[Dict[str, str]] = []
    for r in rows:
        if (r.get("user_id") or "") != user_id:
            continue
        if (r.get("thread_id") or "") != thread_id:
            continue
        if model_key and (r.get("model_key") or "") != model_key:
            continue
        kind = (r.get("kind") or "").strip().lower()
        if kind not in ("good", "bad"):
            continue
        out.append({
            "bot_ts": r.get("bot_ts", ""),
            "kind": kind,
            "model_key": r.get("model_key", ""),
            "saved_at": r.get("saved_at", ""),
        })
    return out


def sse_pack(event: str, data_obj: Dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data_obj, ensure_ascii=False)}\n\n"


def iter_dify_sse(resp: requests.Response) -> Iterable[Dict[str, Any]]:
    for raw in resp.iter_lines(decode_unicode=True):
        if not raw:
            continue
        s = raw.strip()
        if not s.startswith("data:"):
            continue
        payload = s[len("data:"):].strip()
        if payload == "[DONE]":
            break
        try:
            yield json.loads(payload)
        except Exception:
            continue


def maintenance_backup_and_rotation() -> None:
    ensure_dir(BACKUP_DIR)

    day = datetime.now().strftime("%Y%m%d")
    day_dir = os.path.join(BACKUP_DIR, day)
    ensure_dir(day_dir)

    def copy_if_exists(src: str, dst: str) -> None:
        try:
            if os.path.exists(src):
                ensure_dir(os.path.dirname(dst))
                shutil.copy2(src, dst)
        except Exception:
            pass

    try:
        if os.path.isdir(USERS_DIR):
            for uid in os.listdir(USERS_DIR):
                udir = os.path.join(USERS_DIR, uid)
                if not os.path.isdir(udir):
                    continue
                for fn in ("user.csv", "history.csv", "threads.csv", "thread_map.csv", ".last_prune.txt"):
                    src = os.path.join(udir, fn)
                    dst = os.path.join(day_dir, "users", uid, fn)
                    copy_if_exists(src, dst)
    except Exception:
        pass

    try:
        src = feedback_state_csv_path(FEEDBACK_DIR_LOCAL)
        dst = os.path.join(day_dir, "spool", "good_and_bad", FEEDBACK_STATE_NAME)
        copy_if_exists(src, dst)
    except Exception:
        pass

    try:
        cutoff = datetime.now() - timedelta(days=BACKUP_KEEP_DAYS)
        for name in os.listdir(BACKUP_DIR):
            p = os.path.join(BACKUP_DIR, name)
            if not os.path.isdir(p):
                continue
            try:
                dt = datetime.strptime(name, "%Y%m%d")
            except Exception:
                continue
            if dt < cutoff:
                try:
                    shutil.rmtree(p)
                except Exception:
                    pass
    except Exception:
        pass


def maybe_run_maintenance() -> None:
    global _last_maintenance_at
    now = time.time()
    with _maintenance_lock:
        if (now - _last_maintenance_at) < MAINTENANCE_TTL_SEC:
            return
        _last_maintenance_at = now
    maintenance_backup_and_rotation()


app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

ensure_dir(USERS_DIR)
ensure_notice_file()
ensure_dir(FEEDBACK_DIR_LOCAL)
ensure_feedback_state_csv(FEEDBACK_DIR_LOCAL)
if is_nas_available_cached():
    ensure_feedback_state_csv(FEEDBACK_DIR_NAS)
ensure_dir(BACKUP_DIR)


@app.before_request
def _before():
    try:
        maybe_run_maintenance()
    except Exception:
        pass


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        if not user_exists(session["user_id"]):
            session.clear()
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


def api_login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        uid = session.get("user_id")
        if not uid or not user_exists(uid):
            return jsonify({"error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


@app.get("/login")
def login():
    return render_template("login.html")


@app.post("/login")
def login_post():
    user_id = (request.form.get("user_id") or "").strip()
    password = request.form.get("password") or ""
    if not ID7_RE.match(user_id):
        return render_template("login.html", error="IDは数字7桁で入力してください。", user_id=user_id)
    if not verify_user(user_id, password):
        return render_template("login.html", error="IDまたはパスワードが違います。", user_id=user_id)
    session["user_id"] = user_id
    return redirect(url_for("index"))


@app.get("/register")
def register():
    return render_template("register.html")


@app.post("/register")
def register_post():
    user_id = (request.form.get("user_id") or "").strip()
    password = request.form.get("password") or ""
    password2 = request.form.get("password2") or ""
    if not ID7_RE.match(user_id):
        return render_template("register.html", error="IDは数字7桁で入力してください。", user_id=user_id)
    if len(password) < 6:
        return render_template("register.html", error="パスワードは6文字以上にしてください。", user_id=user_id)
    if password != password2:
        return render_template("register.html", error="パスワードが一致しません。", user_id=user_id)
    if user_exists(user_id):
        return render_template("register.html", error="既に登録されています。", user_id=user_id)
    create_user_files(user_id, password)
    session["user_id"] = user_id
    return redirect(url_for("index"))


@app.post("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.get("/")
@login_required
def index():
    return render_template("index.html", user_id=session["user_id"])


@app.get("/api/models")
@api_login_required
def api_models():
    u = load_user(session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401
    return jsonify({
        "current": u["model_key"],
        "models": [{"key": k, "label": MODELS[k]["label"]} for k in MODELS],
        "user_id": u["user_id"],
    })


@app.post("/api/model")
@api_login_required
def api_set_model():
    u = load_user(session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401
    data = request.get_json(force=True)
    mk = (data.get("model") or "").strip()
    if mk not in MODELS:
        return jsonify({"error": "invalid model"}), 400
    u["model_key"] = mk
    save_user(u)
    return jsonify({"ok": True, "current": mk})


@app.get("/api/history")
@api_login_required
def api_history():
    u = load_user(session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401
    tid = (request.args.get("thread_id") or "").strip() or None
    rows = read_history(u["user_id"], tid, limit=200)
    items = [{
        "role": r["role"],
        "content": r["content"],
        "created_at": r["timestamp"],
        "model_key": r["model_key"],
        "thread_id": r["thread_id"],
    } for r in rows]
    return jsonify({"items": items})


@app.get("/api/export")
@api_login_required
def api_export_csv():
    u = load_user(session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    tid = (request.args.get("thread_id") or "").strip()
    if not tid:
        return jsonify({"error": "thread_id is required"}), 400

    threads = _load_threads(u["user_id"])
    if not any((t.get("thread_id") or "") == tid for t in threads):
        return jsonify({"error": "thread not found"}), 404

    items = read_history_all(u["user_id"], tid)

    sio = io.StringIO()
    w = csv.writer(sio, lineterminator="\n")
    w.writerow(["timestamp", "role", "model_key", "thread_id", "content"])
    for m in items:
        w.writerow([
            m.get("timestamp", ""),
            m.get("role", ""),
            m.get("model_key", ""),
            m.get("thread_id", ""),
            m.get("content", ""),
        ])

    csv_text = "\ufeff" + sio.getvalue()
    return Response(
        csv_text,
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="chat.csv"',
            "Cache-Control": "no-store",
        },
    )


@app.get("/api/threads")
@api_login_required
def api_threads():
    u = load_user(session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401
    try:
        limit = int(request.args.get("limit") or "100")
    except Exception:
        limit = 100
    limit = max(1, min(limit, 200))
    return jsonify({"items": list_threads(u["user_id"], limit=limit)})


@app.get("/api/conversations")
@api_login_required
def api_conversations_compat():
    return api_threads()


@app.post("/api/threads/rename")
@api_login_required
def api_thread_rename():
    u = load_user(session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401
    data = request.get_json(force=True)
    tid = (data.get("thread_id") or "").strip()
    name = (data.get("name") or "").strip()
    if not tid or not name:
        return jsonify({"error": "invalid params"}), 400
    if not rename_thread(u["user_id"], tid, name):
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.post("/api/threads/delete")
@api_login_required
def api_thread_delete():
    u = load_user(session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401
    data = request.get_json(force=True)
    tid = (data.get("thread_id") or "").strip()
    if not tid:
        return jsonify({"error": "invalid params"}), 400
    if not delete_thread(u["user_id"], tid):
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.get("/api/notice")
@api_login_required
def api_notice():
    ensure_notice_file()
    try:
        st = os.stat(NOTICE_PATH)
        version = str(int(st.st_mtime))
        with open(NOTICE_PATH, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception:
        version = "0"
        content = ""
    return jsonify({"version": version, "content": content})


@app.get("/api/feedback/state")
@api_login_required
def api_feedback_state():
    u = load_user(session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    thread_id = (request.args.get("thread_id") or "").strip()
    model_key = (request.args.get("model_key") or "").strip() or None

    if not thread_id:
        return jsonify({"error": "thread_id required"}), 400
    if model_key and model_key not in MODELS:
        model_key = None

    try:
        items = list_feedback_state_for_user_thread(
            user_id=u["user_id"],
            thread_id=thread_id,
            model_key=model_key,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"items": items})


@app.post("/api/feedback")
@api_login_required
def api_feedback():
    u = load_user(session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    data = request.get_json(force=True)

    kind = (data.get("kind") or "").strip().lower()
    model_key = (data.get("model_key") or u["model_key"] or DEFAULT_MODEL_KEY).strip() or DEFAULT_MODEL_KEY
    thread_id = (data.get("thread_id") or "").strip()
    bot_ts = (data.get("bot_ts") or "").strip()
    question = (data.get("question") or "")
    answer = (data.get("answer") or "")

    if kind not in ("good", "bad", "none"):
        return jsonify({"error": "invalid kind"}), 400
    if not thread_id:
        return jsonify({"error": "thread_id required"}), 400
    if not bot_ts:
        return jsonify({"error": "bot_ts required"}), 400
    if model_key not in MODELS:
        model_key = u["model_key"]

    if kind != "none":
        if not str(question).strip() or not str(answer).strip():
            return jsonify({"error": "question/answer empty"}), 400

    saved_at = datetime.now().isoformat(timespec="seconds")
    stored_to = "local"

    try:
        sync_local_spool_to_nas_if_possible()
        target_dir = active_feedback_dir()
        stored_to = "nas" if target_dir == FEEDBACK_DIR_NAS else "local"

        prev_rows = load_feedback_state_merged()
        prev_kind = "none"
        prev_saved_at = ""
        key = _feedback_key(u["user_id"], model_key, thread_id, bot_ts)
        for r in prev_rows:
            if _feedback_key(r.get("user_id", ""), r.get("model_key", ""), r.get("thread_id", ""), r.get("bot_ts", "")) == key:
                prev_kind = (r.get("kind") or "none").strip().lower()
                prev_saved_at = r.get("saved_at") or ""
                break

        upsert_feedback_state_to_dir(
            dir_path=target_dir,
            user_id=u["user_id"],
            model_key=model_key,
            thread_id=thread_id,
            bot_ts=bot_ts,
            kind=kind,
            saved_at=saved_at,
            question=str(question),
            answer=str(answer),
        )

        if kind in ("good", "bad"):
            ym = append_feedback_md(
                dir_path=target_dir,
                model_key=model_key,
                kind=kind,
                saved_at=saved_at,
                user_id=u["user_id"],
                question=str(question),
                answer=str(answer),
            )
            if prev_kind in ("good", "bad") and prev_kind != kind:
                mark_dirty_month(model_key, ym)
        else:
            ym_prev = _yyyymm_from_iso(prev_saved_at) if prev_saved_at else _yyyymm_from_iso(saved_at)
            mark_dirty_month(model_key, ym_prev)

        maybe_rebuild_dirty_months(target_dir, model_key)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"ok": True, "kind": kind, "stored_to": stored_to})


@app.post("/api/chat/stream")
@api_login_required
def api_chat_stream():
    u = load_user(session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    model_key = u["model_key"]
    api_key = resolve_api_key(model_key)

    if not DIFY_API_BASE:
        return jsonify({"error": "DIFY_API_BASE not set"}), 500
    if not api_key:
        return jsonify({"error": "API key not set"}), 500

    body = request.get_json(force=True)
    message = (body.get("message") or "").strip()
    thread_id = (body.get("thread_id") or "").strip() or None
    if not message:
        return jsonify({"error": "message is empty"}), 400
    if not thread_id:
        thread_id = uuid.uuid4().hex

    dify_cid_in = get_dify_cid(u["user_id"], thread_id, model_key)

    ts_user = append_history(u["user_id"], "user", model_key, thread_id, dify_cid_in, message)
    upsert_thread(u["user_id"], thread_id, message[:20], ts_user)

    def generate():
        answer_acc = ""
        dify_cid = dify_cid_in
        try:
            with requests.post(
                f"{DIFY_API_BASE}/chat-messages",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "inputs": {},
                    "query": message,
                    "response_mode": "streaming",
                    "conversation_id": dify_cid_in or "",
                    "user": u["user_id"],
                },
                stream=True,
                timeout=180,
            ) as r:
                r.raise_for_status()
                yield sse_pack("meta", {"status": "start", "model": model_key, "ts": ts_user, "thread_id": thread_id})

                for ev in iter_dify_sse(r):
                    ev_type = ev.get("event")
                    if ev.get("conversation_id"):
                        dify_cid = ev["conversation_id"]

                    if ev_type == "message":
                        delta = ev.get("answer") or ""
                        if delta:
                            answer_acc += delta
                            yield sse_pack("delta", {"text": delta})

                    elif ev_type == "message_replace":
                        rep = ev.get("answer") or ""
                        answer_acc = rep
                        yield sse_pack("replace", {"text": rep})

                    elif ev_type == "message_end":
                        ts_bot = append_history(u["user_id"], "bot", model_key, thread_id, dify_cid, answer_acc)
                        set_dify_cid(u["user_id"], thread_id, model_key, dify_cid, ts_bot)
                        upsert_thread(u["user_id"], thread_id, "", ts_bot)
                        yield sse_pack("done", {"thread_id": thread_id, "answer": answer_acc, "model": model_key, "ts": ts_bot})
                        break

                    elif ev_type == "error":
                        yield sse_pack("error", {"message": ev.get("message") or "Dify error"})
                        break

        except requests.HTTPError as e:
            try:
                body_txt = r.text
            except Exception:
                body_txt = str(e)
            yield sse_pack("error", {"message": body_txt})
        except Exception as e:
            yield sse_pack("error", {"message": str(e)})

    return Response(stream_with_context(generate()), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    })


@app.get("/ping")
def ping():
    return "pong"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5201, debug=False, threaded=True)