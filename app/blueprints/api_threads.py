import os

from flask import Blueprint, Response, current_app, jsonify, request, session

from ..core import (
    MODELS,
    DEFAULT_MODEL_KEY,
    ensure_notice_file,
    export_thread_as_csv,
    list_threads,
    load_user,
    read_history,
    rename_thread,
    delete_thread,
    save_user,
)

bp = Blueprint("api_threads", __name__)


def _cfg():
    return current_app.config["APP_CFG"]


def _api_login_required(fn):
    def wrapper(*args, **kwargs):
        uid = session.get("user_id")
        if not uid:
            return jsonify({"error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    wrapper.__name__ = fn.__name__
    return wrapper


@bp.get("/api/models")
@_api_login_required
def api_models():
    u = load_user(_cfg(), session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401
    return jsonify({
        "current": u["model_key"],
        "models": [{"key": k, "label": MODELS[k]["label"]} for k in MODELS],
        "user_id": u["user_id"],
    })


@bp.post("/api/model")
@_api_login_required
def api_set_model():
    u = load_user(_cfg(), session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401
    data = request.get_json(force=True)
    mk = (data.get("model") or "").strip()
    if mk not in MODELS:
        return jsonify({"error": "invalid model"}), 400
    u["model_key"] = mk
    save_user(_cfg(), u)
    return jsonify({"ok": True, "current": mk})


@bp.get("/api/history")
@_api_login_required
def api_history():
    u = load_user(_cfg(), session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    tid = (request.args.get("thread_id") or "").strip() or None
    rows = read_history(_cfg(), u["user_id"], tid, limit=200)
    items = [{
        "role": r["role"],
        "content": r["content"],
        "created_at": r["timestamp"],
        "model_key": r["model_key"],
        "thread_id": r["thread_id"],
    } for r in rows]
    return jsonify({"items": items})


@bp.get("/api/export")
@_api_login_required
def api_export_csv():
    u = load_user(_cfg(), session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    tid = (request.args.get("thread_id") or "").strip()
    if not tid:
        return jsonify({"error": "thread_id is required"}), 400

    csv_text = export_thread_as_csv(_cfg(), u["user_id"], tid)
    return Response(
        csv_text,
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="chat.csv"',
            "Cache-Control": "no-store",
        },
    )


@bp.get("/api/threads")
@_api_login_required
def api_threads():
    u = load_user(_cfg(), session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    try:
        limit = int(request.args.get("limit") or "100")
    except Exception:
        limit = 100
    limit = max(1, min(limit, 200))

    return jsonify({"items": list_threads(_cfg(), u["user_id"], limit=limit)})


@bp.get("/api/conversations")
@_api_login_required
def api_conversations_compat():
    return api_threads()


@bp.post("/api/threads/rename")
@_api_login_required
def api_thread_rename():
    u = load_user(_cfg(), session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    data = request.get_json(force=True)
    tid = (data.get("thread_id") or "").strip()
    name = (data.get("name") or "").strip()
    if not tid or not name:
        return jsonify({"error": "invalid params"}), 400
    if not rename_thread(_cfg(), u["user_id"], tid, name):
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@bp.post("/api/threads/delete")
@_api_login_required
def api_thread_delete():
    u = load_user(_cfg(), session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    data = request.get_json(force=True)
    tid = (data.get("thread_id") or "").strip()
    if not tid:
        return jsonify({"error": "invalid params"}), 400
    if not delete_thread(_cfg(), u["user_id"], tid):
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@bp.get("/api/notice")
@_api_login_required
def api_notice():
    ensure_notice_file(_cfg())
    try:
        st = os.stat(_cfg().notice_path)
        version = str(int(st.st_mtime))
        with open(_cfg().notice_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception:
        version = "0"
        content = ""
    return jsonify({"version": version, "content": content})