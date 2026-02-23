import os
import re
import csv
import json
import uuid
import io
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, Any, Iterable, Optional, List

import requests
from dotenv import load_dotenv
from flask import (
    Flask, render_template, request, jsonify, session, Response,
    redirect, url_for
)

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USERS_DIR = os.path.join(BASE_DIR, "users")

FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")

DIFY_API_BASE = (os.environ.get("DIFY_API_BASE") or "http://161.93.108.55:8890/v1").rstrip("/")
DEFAULT_DIFY_API_KEY = os.environ.get("DIFY_API_KEY") or ""

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
}

USER_FIELDS = ["user_id", "password", "model_key", "created_at"]
HISTORY_FIELDS = ["timestamp", "role", "model_key", "thread_id", "dify_conversation_id", "content"]
THREAD_FIELDS = ["thread_id", "name", "preview", "created_at", "updated_at"]
MAP_FIELDS = ["thread_id", "model_key", "dify_conversation_id", "updated_at"]

NOTICE_PATH = os.path.join(BASE_DIR, "notice.txt")


def ensure_notice_file() -> None:
    if os.path.exists(NOTICE_PATH):
        return
    try:
        with open(NOTICE_PATH, "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass


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


def ensure_user_dir(user_id: str) -> None:
    os.makedirs(user_dir(user_id), exist_ok=True)


def atomic_write_csv(path: str, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    os.replace(tmp, path)


def ensure_csv(path: str, fieldnames: List[str]) -> None:
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()


def ensure_all_csv(user_id: str) -> None:
    ensure_user_dir(user_id)
    ensure_csv(history_csv_path(user_id), HISTORY_FIELDS)
    ensure_csv(threads_csv_path(user_id), THREAD_FIELDS)
    ensure_csv(map_csv_path(user_id), MAP_FIELDS)


def user_exists(user_id: str) -> bool:
    return os.path.exists(user_csv_path(user_id))


def load_user(user_id: str) -> Optional[Dict[str, str]]:
    if not os.path.exists(user_csv_path(user_id)):
        return None
    ensure_all_csv(user_id)
    with open(user_csv_path(user_id), newline="", encoding="utf-8") as f:
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
    ensure_all_csv(u["user_id"])
    with open(user_csv_path(u["user_id"]), "w", newline="", encoding="utf-8") as f:
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
    ensure_all_csv(user_id)
    with open(user_csv_path(user_id), "w", newline="", encoding="utf-8") as f:
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
    ensure_all_csv(user_id)

    today = datetime.now().strftime("%Y-%m-%d")
    if _read_last_prune(user_id) == today:
        return

    cutoff = datetime.now() - timedelta(days=14)

    kept: List[Dict[str, str]] = []
    with open(history_csv_path(user_id), newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            ts = (row.get("timestamp") or "").strip()
            try:
                dt = datetime.fromisoformat(ts)
            except Exception:
                kept.append({
                    "timestamp": row.get("timestamp") or "",
                    "role": row.get("role") or "",
                    "model_key": row.get("model_key") or DEFAULT_MODEL_KEY,
                    "thread_id": row.get("thread_id") or "",
                    "dify_conversation_id": row.get("dify_conversation_id") or "",
                    "content": row.get("content") or "",
                })
                continue

            if dt >= cutoff:
                kept.append({
                    "timestamp": row.get("timestamp") or "",
                    "role": row.get("role") or "",
                    "model_key": row.get("model_key") or DEFAULT_MODEL_KEY,
                    "thread_id": row.get("thread_id") or "",
                    "dify_conversation_id": row.get("dify_conversation_id") or "",
                    "content": row.get("content") or "",
                })

    atomic_write_csv(history_csv_path(user_id), HISTORY_FIELDS, kept)
    _write_last_prune(user_id, today)


def append_history(user_id: str, role: str, model_key: str, thread_id: str, dify_cid: str, content: str) -> str:
    ensure_all_csv(user_id)
    ts = datetime.now().isoformat(timespec="seconds")
    with open(history_csv_path(user_id), "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([ts, role, model_key, thread_id, dify_cid or "", content])

    prune_history_14days(user_id)
    return ts


def read_history(user_id: str, thread_id: Optional[str], limit: int = 200) -> List[Dict[str, str]]:
    if not thread_id:
        return []
    ensure_all_csv(user_id)
    rows: List[Dict[str, str]] = []
    with open(history_csv_path(user_id), newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("thread_id") or "").strip() != thread_id:
                continue
            rows.append({
                "timestamp": row.get("timestamp") or "",
                "role": row.get("role") or "",
                "model_key": row.get("model_key") or DEFAULT_MODEL_KEY,
                "thread_id": thread_id,
                "content": row.get("content") or "",
            })
    if len(rows) > limit:
        rows = rows[-limit:]
    return rows


def read_history_all(user_id: str, thread_id: Optional[str]) -> List[Dict[str, str]]:
    if not thread_id:
        return []
    ensure_all_csv(user_id)
    rows: List[Dict[str, str]] = []
    with open(history_csv_path(user_id), newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("thread_id") or "").strip() != thread_id:
                continue
            rows.append({
                "timestamp": row.get("timestamp") or "",
                "role": row.get("role") or "",
                "model_key": row.get("model_key") or DEFAULT_MODEL_KEY,
                "thread_id": thread_id,
                "content": row.get("content") or "",
            })
    return rows


def _load_threads(user_id: str) -> List[Dict[str, str]]:
    ensure_all_csv(user_id)
    out: List[Dict[str, str]] = []
    with open(threads_csv_path(user_id), newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            tid = (row.get("thread_id") or "").strip()
            if not tid:
                continue
            out.append({
                "thread_id": tid,
                "name": row.get("name") or "",
                "preview": row.get("preview") or "",
                "created_at": row.get("created_at") or "",
                "updated_at": row.get("updated_at") or "",
            })
    return out


def _save_threads(user_id: str, rows: List[Dict[str, str]]) -> None:
    atomic_write_csv(threads_csv_path(user_id), THREAD_FIELDS, rows)


def upsert_thread(user_id: str, thread_id: str, preview: str, updated_at: str) -> None:
    rows = _load_threads(user_id)
    for r in rows:
        if r["thread_id"] == thread_id:
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
    rows.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    return rows[:limit]


def rename_thread(user_id: str, thread_id: str, name: str) -> bool:
    name = (name or "").strip()
    if not thread_id or not name:
        return False
    rows = _load_threads(user_id)
    for r in rows:
        if r["thread_id"] == thread_id:
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
    new_rows = [r for r in rows if r["thread_id"] != thread_id]
    if len(new_rows) == len(rows):
        return False
    _save_threads(user_id, new_rows)

    kept: List[Dict[str, str]] = []
    with open(history_csv_path(user_id), newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("thread_id") or "").strip() == thread_id:
                continue
            kept.append({
                "timestamp": row.get("timestamp") or "",
                "role": row.get("role") or "",
                "model_key": row.get("model_key") or DEFAULT_MODEL_KEY,
                "thread_id": (row.get("thread_id") or "").strip(),
                "dify_conversation_id": (row.get("dify_conversation_id") or "").strip(),
                "content": row.get("content") or "",
            })
    atomic_write_csv(history_csv_path(user_id), HISTORY_FIELDS, kept)

    kept2: List[Dict[str, str]] = []
    with open(map_csv_path(user_id), newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("thread_id") or "").strip() == thread_id:
                continue
            kept2.append({
                "thread_id": (row.get("thread_id") or "").strip(),
                "model_key": (row.get("model_key") or DEFAULT_MODEL_KEY).strip(),
                "dify_conversation_id": (row.get("dify_conversation_id") or "").strip(),
                "updated_at": row.get("updated_at") or "",
            })
    atomic_write_csv(map_csv_path(user_id), MAP_FIELDS, kept2)
    return True


def _load_map(user_id: str) -> List[Dict[str, str]]:
    ensure_all_csv(user_id)
    out: List[Dict[str, str]] = []
    with open(map_csv_path(user_id), newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            out.append({
                "thread_id": (row.get("thread_id") or "").strip(),
                "model_key": (row.get("model_key") or DEFAULT_MODEL_KEY).strip() or DEFAULT_MODEL_KEY,
                "dify_conversation_id": (row.get("dify_conversation_id") or "").strip(),
                "updated_at": row.get("updated_at") or "",
            })
    return [x for x in out if x["thread_id"] and x["model_key"]]


def get_dify_cid(user_id: str, thread_id: str, model_key: str) -> str:
    rows = _load_map(user_id)
    for r in rows:
        if r["thread_id"] == thread_id and r["model_key"] == model_key:
            return r["dify_conversation_id"] or ""
    return ""


def set_dify_cid(user_id: str, thread_id: str, model_key: str, dify_cid: str, updated_at: str) -> None:
    rows = _load_map(user_id)
    for r in rows:
        if r["thread_id"] == thread_id and r["model_key"] == model_key:
            r["dify_conversation_id"] = dify_cid
            r["updated_at"] = updated_at
            atomic_write_csv(map_csv_path(user_id), MAP_FIELDS, rows)
            return
    rows.append({
        "thread_id": thread_id,
        "model_key": model_key,
        "dify_conversation_id": dify_cid,
        "updated_at": updated_at,
    })
    atomic_write_csv(map_csv_path(user_id), MAP_FIELDS, rows)


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


app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY
os.makedirs(USERS_DIR, exist_ok=True)
ensure_notice_file()


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
    if not any(t["thread_id"] == tid for t in threads):
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
                body_txt = r.text  # noqa
            except Exception:
                body_txt = str(e)
            yield sse_pack("error", {"message": body_txt})
        except Exception as e:
            yield sse_pack("error", {"message": str(e)})

    return Response(generate(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    })


@app.get("/ping")
def ping():
    return "pong"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5200, debug=False, threaded=True)