import csv
import io
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
from flask import g

from config import AppConfig

ID7_RE = re.compile(r"^\d{7}$")
DEFAULT_MODEL_KEY = "seisan"

MODELS: Dict[str, Dict[str, str]] = {
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

USER_FIELDS = ["user_id", "password", "model_key", "created_at"]
HISTORY_FIELDS = ["timestamp", "role", "model_key", "thread_id", "dify_conversation_id", "content"]
THREAD_FIELDS = ["thread_id", "name", "preview", "created_at", "updated_at"]
MAP_FIELDS = ["thread_id", "model_key", "dify_conversation_id", "updated_at"]

FEEDBACK_STATE_NAME = "feedback_state.csv"
FEEDBACK_FIELDS = ["user_id", "model_key", "thread_id", "bot_ts", "kind", "saved_at", "question", "answer"]

_file_locks: Dict[str, Lock] = {}
_file_locks_guard = Lock()

_nas_ok_cache: Optional[bool] = None
_nas_ok_checked_at: float = 0.0
_nas_guard = Lock()

_md_guard = Lock()
_last_md_rebuild_at: Dict[str, float] = {}
_dirty_months_by_model: Dict[str, Set[str]] = {}


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
            tmp = path + ".tmp"
            with open(tmp, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
            os.replace(tmp, path)

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


def ensure_notice_file(cfg: AppConfig) -> None:
    if os.path.exists(cfg.notice_path):
        return
    lk = _lock_for_path(cfg.notice_path)
    with lk:
        if os.path.exists(cfg.notice_path):
            return
        ensure_dir(os.path.dirname(cfg.notice_path))
        tmp = cfg.notice_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("")
        os.replace(tmp, cfg.notice_path)


def _is_dir_writable(path: str) -> bool:
    try:
        ensure_dir(path)
        probe = os.path.join(path, ".write_test.tmp")
        lk = _lock_for_path(probe)
        with lk:
            with open(probe, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(probe)
        return True
    except Exception:
        return False


def is_nas_available_cached(cfg: AppConfig) -> bool:
    global _nas_ok_cache, _nas_ok_checked_at
    now = time.time()
    with _nas_guard:
        if _nas_ok_cache is not None and (now - _nas_ok_checked_at) < cfg.nas_check_ttl_sec:
            return _nas_ok_cache
        ok = _is_dir_writable(cfg.feedback_dir_nas)
        _nas_ok_cache = ok
        _nas_ok_checked_at = now
        return ok


def active_feedback_dir(cfg: AppConfig) -> str:
    return cfg.feedback_dir_nas if is_nas_available_cached(cfg) else cfg.feedback_dir_local


def user_dir(cfg: AppConfig, user_id: str) -> str:
    return os.path.join(cfg.users_dir, user_id)


def user_csv_path(cfg: AppConfig, user_id: str) -> str:
    return os.path.join(user_dir(cfg, user_id), "user.csv")


def history_csv_path(cfg: AppConfig, user_id: str) -> str:
    return os.path.join(user_dir(cfg, user_id), "history.csv")


def threads_csv_path(cfg: AppConfig, user_id: str) -> str:
    return os.path.join(user_dir(cfg, user_id), "threads.csv")


def map_csv_path(cfg: AppConfig, user_id: str) -> str:
    return os.path.join(user_dir(cfg, user_id), "thread_map.csv")


def ensure_all_user_csv(cfg: AppConfig, user_id: str) -> None:
    ensure_dir(user_dir(cfg, user_id))
    csv_read_dicts_cached(history_csv_path(cfg, user_id), HISTORY_FIELDS)
    csv_read_dicts_cached(threads_csv_path(cfg, user_id), THREAD_FIELDS)
    csv_read_dicts_cached(map_csv_path(cfg, user_id), MAP_FIELDS)


def user_exists(cfg: AppConfig, user_id: str) -> bool:
    return os.path.exists(user_csv_path(cfg, user_id))


def load_user(cfg: AppConfig, user_id: str) -> Optional[Dict[str, str]]:
    p = user_csv_path(cfg, user_id)
    if not os.path.exists(p):
        return None
    ensure_all_user_csv(cfg, user_id)

    lk = _lock_for_path(p)
    with lk:
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


def save_user(cfg: AppConfig, u: Dict[str, str]) -> None:
    ensure_all_user_csv(cfg, u["user_id"])
    p = user_csv_path(cfg, u["user_id"])
    csv_write_dicts_atomic(p, USER_FIELDS, [{
        "user_id": u["user_id"],
        "password": u.get("password", ""),
        "model_key": u.get("model_key", DEFAULT_MODEL_KEY),
        "created_at": u.get("created_at", ""),
    }])


def create_user_files(cfg: AppConfig, user_id: str, password: str) -> None:
    ensure_all_user_csv(cfg, user_id)
    p = user_csv_path(cfg, user_id)
    csv_write_dicts_atomic(p, USER_FIELDS, [{
        "user_id": user_id,
        "password": password,
        "model_key": DEFAULT_MODEL_KEY,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }])


def verify_user(cfg: AppConfig, user_id: str, password: str) -> bool:
    u = load_user(cfg, user_id)
    return bool(u and u.get("password", "") == password)


def resolve_api_key(cfg: AppConfig, model_key: str) -> str:
    env_key = MODELS[model_key]["api_key_env"]
    return (os.environ.get(env_key) or "").strip() or cfg.default_dify_api_key


def _last_prune_path(cfg: AppConfig, user_id: str) -> str:
    return os.path.join(user_dir(cfg, user_id), ".last_prune.txt")


def _read_last_prune(cfg: AppConfig, user_id: str) -> str:
    p = _last_prune_path(cfg, user_id)
    lk = _lock_for_path(p)
    with lk:
        try:
            with open(p, "r", encoding="utf-8") as f:
                return (f.read() or "").strip()
        except Exception:
            return ""


def _write_last_prune(cfg: AppConfig, user_id: str, ymd: str) -> None:
    p = _last_prune_path(cfg, user_id)
    lk = _lock_for_path(p)
    with lk:
        ensure_dir(os.path.dirname(p))
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(ymd)
        os.replace(tmp, p)


def prune_history_14days(cfg: AppConfig, user_id: str) -> None:
    ensure_all_user_csv(cfg, user_id)
    today = datetime.now().strftime("%Y-%m-%d")
    if _read_last_prune(cfg, user_id) == today:
        return

    cutoff = datetime.now() - timedelta(days=14)
    path = history_csv_path(cfg, user_id)
    rows = csv_read_dicts_cached(path, HISTORY_FIELDS)

    kept: List[Dict[str, str]] = []
    for row in rows:
        ts = (row.get("timestamp") or "").strip()
        try:
            dt = datetime.fromisoformat(ts)
        except Exception:
            kept.append({k: row.get(k, "") for k in HISTORY_FIELDS})
            continue
        if dt >= cutoff:
            kept.append({k: row.get(k, "") for k in HISTORY_FIELDS})

    csv_write_dicts_atomic(path, HISTORY_FIELDS, kept)
    _write_last_prune(cfg, user_id, today)


def append_history(cfg: AppConfig, user_id: str, role: str, model_key: str, thread_id: str, dify_cid: str, content: str) -> str:
    ensure_all_user_csv(cfg, user_id)
    ts = datetime.now().isoformat(timespec="seconds")
    csv_append_row(history_csv_path(cfg, user_id), [ts, role, model_key, thread_id, dify_cid or "", content])
    prune_history_14days(cfg, user_id)
    return ts


def read_history(cfg: AppConfig, user_id: str, thread_id: Optional[str], limit: int = 200) -> List[Dict[str, str]]:
    if not thread_id:
        return []
    ensure_all_user_csv(cfg, user_id)
    rows = csv_read_dicts_cached(history_csv_path(cfg, user_id), HISTORY_FIELDS)

    out: List[Dict[str, str]] = []
    for row in rows:
        if (row.get("thread_id") or "").strip() != thread_id:
            continue
        out.append({
            "timestamp": row.get("timestamp") or "",
            "role": row.get("role") or "",
            "model_key": row.get("model_key") or DEFAULT_MODEL_KEY,
            "thread_id": thread_id,
            "content": row.get("content") or "",
        })

    if len(out) > limit:
        out = out[-limit:]
    return out


def read_history_all(cfg: AppConfig, user_id: str, thread_id: str) -> List[Dict[str, str]]:
    ensure_all_user_csv(cfg, user_id)
    rows = csv_read_dicts_cached(history_csv_path(cfg, user_id), HISTORY_FIELDS)
    out: List[Dict[str, str]] = []
    for row in rows:
        if (row.get("thread_id") or "").strip() != thread_id:
            continue
        out.append({
            "timestamp": row.get("timestamp") or "",
            "role": row.get("role") or "",
            "model_key": row.get("model_key") or DEFAULT_MODEL_KEY,
            "thread_id": thread_id,
            "content": row.get("content") or "",
        })
    return out


def _load_threads(cfg: AppConfig, user_id: str) -> List[Dict[str, str]]:
    ensure_all_user_csv(cfg, user_id)
    rows = csv_read_dicts_cached(threads_csv_path(cfg, user_id), THREAD_FIELDS)
    out = []
    for r in rows:
        tid = (r.get("thread_id") or "").strip()
        if not tid:
            continue
        out.append({k: r.get(k, "") for k in THREAD_FIELDS})
    return out


def _save_threads(cfg: AppConfig, user_id: str, rows: List[Dict[str, str]]) -> None:
    csv_write_dicts_atomic(threads_csv_path(cfg, user_id), THREAD_FIELDS, rows)


def upsert_thread(cfg: AppConfig, user_id: str, thread_id: str, preview: str, updated_at: str) -> None:
    rows = _load_threads(cfg, user_id)
    for r in rows:
        if (r.get("thread_id") or "") == thread_id:
            if preview and not (r.get("preview") or "").strip():
                r["preview"] = preview
            r["updated_at"] = updated_at
            _save_threads(cfg, user_id, rows)
            return

    rows.append({
        "thread_id": thread_id,
        "name": "",
        "preview": preview,
        "created_at": updated_at,
        "updated_at": updated_at,
    })
    _save_threads(cfg, user_id, rows)


def list_threads(cfg: AppConfig, user_id: str, limit: int = 100) -> List[Dict[str, str]]:
    rows = _load_threads(cfg, user_id)
    rows.sort(key=lambda x: (x.get("updated_at") or ""), reverse=True)
    return rows[:limit]


def rename_thread(cfg: AppConfig, user_id: str, thread_id: str, name: str) -> bool:
    name = (name or "").strip()
    if not thread_id or not name:
        return False
    rows = _load_threads(cfg, user_id)
    for r in rows:
        if (r.get("thread_id") or "") == thread_id:
            r["name"] = name
            r["preview"] = name[:20]
            r["updated_at"] = datetime.now().isoformat(timespec="seconds")
            _save_threads(cfg, user_id, rows)
            return True
    return False


def delete_thread(cfg: AppConfig, user_id: str, thread_id: str) -> bool:
    if not thread_id:
        return False
    rows = _load_threads(cfg, user_id)
    new_rows = [r for r in rows if (r.get("thread_id") or "") != thread_id]
    if len(new_rows) == len(rows):
        return False
    _save_threads(cfg, user_id, new_rows)

    hist_path = history_csv_path(cfg, user_id)
    hist_rows = csv_read_dicts_cached(hist_path, HISTORY_FIELDS)
    kept_hist = [r for r in hist_rows if (r.get("thread_id") or "").strip() != thread_id]
    csv_write_dicts_atomic(hist_path, HISTORY_FIELDS, kept_hist)

    map_path = map_csv_path(cfg, user_id)
    map_rows = csv_read_dicts_cached(map_path, MAP_FIELDS)
    kept_map = [r for r in map_rows if (r.get("thread_id") or "").strip() != thread_id]
    csv_write_dicts_atomic(map_path, MAP_FIELDS, kept_map)

    return True


def _load_map(cfg: AppConfig, user_id: str) -> List[Dict[str, str]]:
    ensure_all_user_csv(cfg, user_id)
    rows = csv_read_dicts_cached(map_csv_path(cfg, user_id), MAP_FIELDS)
    out = []
    for r in rows:
        tid = (r.get("thread_id") or "").strip()
        mk = (r.get("model_key") or DEFAULT_MODEL_KEY).strip() or DEFAULT_MODEL_KEY
        if not tid:
            continue
        out.append({
            "thread_id": tid,
            "model_key": mk,
            "dify_conversation_id": (r.get("dify_conversation_id") or "").strip(),
            "updated_at": r.get("updated_at") or "",
        })
    return out


def get_dify_cid(cfg: AppConfig, user_id: str, thread_id: str, model_key: str) -> str:
    rows = _load_map(cfg, user_id)
    for r in rows:
        if r["thread_id"] == thread_id and r["model_key"] == model_key:
            return r.get("dify_conversation_id") or ""
    return ""


def set_dify_cid(cfg: AppConfig, user_id: str, thread_id: str, model_key: str, dify_cid: str, updated_at: str) -> None:
    rows = _load_map(cfg, user_id)
    for r in rows:
        if r["thread_id"] == thread_id and r["model_key"] == model_key:
            r["dify_conversation_id"] = dify_cid
            r["updated_at"] = updated_at
            csv_write_dicts_atomic(map_csv_path(cfg, user_id), MAP_FIELDS, rows)
            return
    rows.append({
        "thread_id": thread_id,
        "model_key": model_key,
        "dify_conversation_id": dify_cid,
        "updated_at": updated_at,
    })
    csv_write_dicts_atomic(map_csv_path(cfg, user_id), MAP_FIELDS, rows)


def export_thread_as_csv(cfg: AppConfig, user_id: str, thread_id: str) -> str:
    items = read_history_all(cfg, user_id, thread_id)
    sio = io.StringIO()
    w = csv.writer(sio, lineterminator="\n")
    w.writerow(["timestamp", "role", "model_key", "thread_id", "content"])
    for m in items:
        w.writerow([m.get("timestamp", ""), m.get("role", ""), m.get("model_key", ""), m.get("thread_id", ""), m.get("content", "")])
    return "\ufeff" + sio.getvalue()


def create_new_thread_id() -> str:
    return uuid.uuid4().hex


def feedback_state_csv_path(dir_path: str) -> str:
    return os.path.join(dir_path, FEEDBACK_STATE_NAME)


def ensure_feedback_state_csv(dir_path: str) -> None:
    ensure_dir(dir_path)
    p = feedback_state_csv_path(dir_path)
    lk = _lock_for_path(p)
    with lk:
        if os.path.exists(p):
            return
        tmp = p + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FEEDBACK_FIELDS)
            w.writeheader()
        os.replace(tmp, p)
    _csv_cache().pop(f"read::{p}", None)


def _feedback_key(user_id: str, model_key: str, thread_id: str, bot_ts: str) -> str:
    return f"{user_id}||{model_key}||{thread_id}||{bot_ts}"


def _load_feedback_state_from(dir_path: str) -> List[Dict[str, str]]:
    ensure_feedback_state_csv(dir_path)
    p = feedback_state_csv_path(dir_path)
    lk = _lock_for_path(p)
    with lk:
        out = []
        with open(p, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                out.append({k: row.get(k, "") for k in FEEDBACK_FIELDS})
        return out


def _save_feedback_state_to(dir_path: str, rows: List[Dict[str, str]]) -> None:
    p = feedback_state_csv_path(dir_path)
    csv_write_dicts_atomic(p, FEEDBACK_FIELDS, rows)


def _merge_feedback_rows(primary: List[Dict[str, str]], secondary: List[Dict[str, str]]) -> List[Dict[str, str]]:
    def ts_key(x: Dict[str, str]) -> str:
        return (x.get("saved_at") or "").strip()

    m: Dict[str, Dict[str, str]] = {}
    for r in primary:
        m[_feedback_key(r.get("user_id", ""), r.get("model_key", ""), r.get("thread_id", ""), r.get("bot_ts", ""))] = r
    for r in secondary:
        k = _feedback_key(r.get("user_id", ""), r.get("model_key", ""), r.get("thread_id", ""), r.get("bot_ts", ""))
        if k not in m or ts_key(r) >= ts_key(m[k]):
            m[k] = r
    return list(m.values())


def load_feedback_state_merged(cfg: AppConfig) -> List[Dict[str, str]]:
    rows_nas = []
    rows_local = []
    try:
        if os.path.exists(feedback_state_csv_path(cfg.feedback_dir_nas)) or is_nas_available_cached(cfg):
            if os.path.exists(feedback_state_csv_path(cfg.feedback_dir_nas)):
                rows_nas = _load_feedback_state_from(cfg.feedback_dir_nas)
    except Exception:
        rows_nas = []
    try:
        if os.path.exists(feedback_state_csv_path(cfg.feedback_dir_local)):
            rows_local = _load_feedback_state_from(cfg.feedback_dir_local)
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

    if not found and kind != "none":
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


def _yyyymm_from_iso(iso: str) -> str:
    try:
        dt = datetime.fromisoformat((iso or "").strip())
        return dt.strftime("%Y%m")
    except Exception:
        return datetime.now().strftime("%Y%m")


def _safe_filename_part(s: str) -> str:
    s = (s or "").strip() or "unknown"
    return re.sub(r"[\\/:*?\"<>|]+", "_", s)


def _feedback_md_path(dir_path: str, model_key: str, kind: str, yyyymm: str) -> str:
    mk = _safe_filename_part(model_key)
    kd = "good" if kind == "good" else "bad"
    ym = re.sub(r"\D", "", (yyyymm or ""))[:6] or datetime.now().strftime("%Y%m")
    return os.path.join(dir_path, f"{mk}_{kd}_{ym}.md")


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
    p = _feedback_md_path(dir_path, model_key, kind, ym)
    lk = _lock_for_path(p)
    with lk:
        with open(p, "a", encoding="utf-8", newline="\n") as f:
            f.write(_md_chunk(saved_at, user_id, model_key, question, answer))
    return ym


def rebuild_feedback_md_for_model_months_in_dir(dir_path: str, model_key: str, months: Set[str]) -> None:
    ensure_dir(dir_path)
    rows = _load_feedback_state_from(dir_path)

    targets = {re.sub(r"\D", "", m)[:6] for m in months if m}
    targets.discard("")
    if not targets:
        return

    buckets: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for r in rows:
        if (r.get("model_key") or "") != model_key:
            continue
        kind = (r.get("kind") or "").strip().lower()
        if kind not in ("good", "bad"):
            continue
        ym = _yyyymm_from_iso(r.get("saved_at", ""))
        if ym not in targets:
            continue
        buckets.setdefault((kind, ym), []).append(r)

    mk_safe = _safe_filename_part(model_key)
    for ym in targets:
        for kd in ("good", "bad"):
            p = os.path.join(dir_path, f"{mk_safe}_{kd}_{ym}.md")
            lk = _lock_for_path(p)
            with lk:
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass

    for (kind, ym), lst in buckets.items():
        lst.sort(key=lambda x: x.get("saved_at", ""), reverse=True)
        p = _feedback_md_path(dir_path, model_key, kind, ym)
        tmp = p + ".tmp"
        lk = _lock_for_path(p)
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
            os.replace(tmp, p)


def mark_dirty_month(model_key: str, yyyymm: str) -> None:
    ym = re.sub(r"\D", "", (yyyymm or ""))[:6]
    if not ym:
        return
    _dirty_months_by_model.setdefault(model_key, set()).add(ym)


def maybe_rebuild_dirty_months(cfg: AppConfig, dir_path: str, model_key: str) -> None:
    now = time.time()
    with _md_guard:
        dirty = _dirty_months_by_model.get(model_key, set())
        if not dirty:
            return
        last = _last_md_rebuild_at.get(model_key, 0.0)
        if (now - last) < cfg.md_rebuild_cooldown_sec:
            return
        months = set(dirty)
        _dirty_months_by_model[model_key] = set()
        _last_md_rebuild_at[model_key] = now
    rebuild_feedback_md_for_model_months_in_dir(dir_path, model_key, months)


def list_feedback_state_for_user_thread(cfg: AppConfig, *, user_id: str, thread_id: str, model_key: Optional[str]) -> List[Dict[str, str]]:
    rows = load_feedback_state_merged(cfg)
    out = []
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


def _compute_months_by_model(rows: List[Dict[str, str]]) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for r in rows:
        mk = (r.get("model_key") or "").strip()
        kind = (r.get("kind") or "").strip().lower()
        if not mk or kind not in ("good", "bad"):
            continue
        ym = _yyyymm_from_iso(r.get("saved_at", ""))
        out.setdefault(mk, set()).add(ym)
    return out


def sync_local_spool_to_nas_if_possible(cfg: AppConfig) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "nas_available": False,
        "moved_csv": False,
        "rebuilt": [],
        "errors": [],
    }

    if not is_nas_available_cached(cfg):
        return report
    report["nas_available"] = True

    local_csv = feedback_state_csv_path(cfg.feedback_dir_local)
    if not os.path.exists(local_csv):
        return report

    try:
        rows_local = _load_feedback_state_from(cfg.feedback_dir_local)
    except Exception as e:
        report["errors"].append(str(e))
        return report

    if not rows_local:
        return report

    try:
        ensure_feedback_state_csv(cfg.feedback_dir_nas)
        rows_nas = _load_feedback_state_from(cfg.feedback_dir_nas)
        merged = _merge_feedback_rows(rows_nas, rows_local)
        _save_feedback_state_to(cfg.feedback_dir_nas, merged)

        mk_to_months = _compute_months_by_model(rows_local)
        for mk, months in mk_to_months.items():
            try:
                rebuild_feedback_md_for_model_months_in_dir(cfg.feedback_dir_nas, mk, months)
                report["rebuilt"].append({"model_key": mk, "months": sorted(list(months))})
            except Exception as e:
                report["errors"].append(f"md rebuild error: {mk}: {e}")

        bak = local_csv + f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            os.replace(local_csv, bak)
        except Exception:
            try:
                os.remove(local_csv)
            except Exception:
                pass

        report["moved_csv"] = True
        return report

    except Exception as e:
        report["errors"].append(str(e))
        return report


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