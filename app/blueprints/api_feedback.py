import re
from datetime import datetime

from flask import Blueprint, current_app, jsonify, request, session

from ..core import (
    MODELS,
    DEFAULT_MODEL_KEY,
    active_feedback_dir,
    append_feedback_md,
    is_nas_available_cached,
    list_feedback_state_for_user_thread,
    load_user,
    mark_dirty_month,
    maybe_rebuild_dirty_months,
    rebuild_feedback_md_for_model_months_in_dir,
    sync_local_spool_to_nas_if_possible,
    upsert_feedback_state_to_dir,
    load_feedback_state_merged,
)

bp = Blueprint("api_feedback", __name__)


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


@bp.get("/api/feedback/state")
@_api_login_required
def api_feedback_state():
    u = load_user(_cfg(), session["user_id"])
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
            _cfg(),
            user_id=u["user_id"],
            thread_id=thread_id,
            model_key=model_key,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"items": items})


@bp.post("/api/feedback")
@_api_login_required
def api_feedback():
    u = load_user(_cfg(), session["user_id"])
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
        sync_local_spool_to_nas_if_possible(_cfg())

        target_dir = active_feedback_dir(_cfg())
        stored_to = "nas" if (target_dir == _cfg().feedback_dir_nas and is_nas_available_cached(_cfg())) else "local"

        prev_rows = load_feedback_state_merged(_cfg())
        prev_kind = "none"
        prev_saved_at = ""
        for r in prev_rows:
            if (r.get("user_id") or "") == u["user_id"] and (r.get("model_key") or "") == model_key and (r.get("thread_id") or "") == thread_id and (r.get("bot_ts") or "") == bot_ts:
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
            ym_prev = re.sub(r"\D", "", (prev_saved_at or ""))[:6] if prev_saved_at else ""
            if ym_prev:
                mark_dirty_month(model_key, ym_prev)

        maybe_rebuild_dirty_months(_cfg(), target_dir, model_key)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"ok": True, "kind": kind, "stored_to": stored_to})


@bp.post("/api/feedback/rebuild")
@_api_login_required
def api_feedback_rebuild():
    u = load_user(_cfg(), session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    data = request.get_json(force=True)
    model_key = (data.get("model_key") or "").strip()
    yyyymm = (data.get("yyyymm") or "").strip().lower()

    if model_key and model_key not in MODELS:
        return jsonify({"error": "invalid model_key"}), 400

    target = (data.get("target") or "").strip().lower()
    if target == "nas":
        target_dir = _cfg().feedback_dir_nas
    elif target == "local":
        target_dir = _cfg().feedback_dir_local
    else:
        target_dir = active_feedback_dir(_cfg())

    if yyyymm in ("", "all"):
        merged = load_feedback_state_merged(_cfg())
        mk_months = {}
        for r in merged:
            mk = (r.get("model_key") or "").strip()
            kd = (r.get("kind") or "").strip().lower()
            if kd not in ("good", "bad"):
                continue
            if model_key and mk != model_key:
                continue
            sa = (r.get("saved_at") or "").strip()
            ym = re.sub(r"\D", "", sa)[:6]
            if not ym:
                continue
            mk_months.setdefault(mk, set()).add(ym)

        rebuilt = []
        for mk, months in mk_months.items():
            rebuild_feedback_md_for_model_months_in_dir(target_dir, mk, set(months))
            rebuilt.append({"model_key": mk, "months": sorted(list(months))})
        return jsonify({"ok": True, "target_dir": target_dir, "rebuilt": rebuilt})

    ym = re.sub(r"\D", "", yyyymm)[:6]
    if not ym:
        return jsonify({"error": "invalid yyyymm"}), 400

    if not model_key:
        return jsonify({"error": "model_key required when yyyymm is specified"}), 400

    rebuild_feedback_md_for_model_months_in_dir(target_dir, model_key, {ym})
    return jsonify({"ok": True, "target_dir": target_dir, "rebuilt": [{"model_key": model_key, "months": [ym]}]})