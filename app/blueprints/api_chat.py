import requests
from flask import Blueprint, Response, current_app, jsonify, request, session, stream_with_context

from ..core import (
    DEFAULT_MODEL_KEY,
    MODELS,
    append_history,
    create_new_thread_id,
    get_dify_cid,
    iter_dify_sse,
    load_user,
    resolve_api_key,
    set_dify_cid,
    sse_pack,
    upsert_thread,
)

bp = Blueprint("api_chat", __name__)


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


@bp.post("/api/chat/stream")
@_api_login_required
def api_chat_stream():
    u = load_user(_cfg(), session["user_id"])
    if not u:
        session.clear()
        return jsonify({"error": "user not found"}), 401

    model_key = u["model_key"]
    if model_key not in MODELS:
        model_key = DEFAULT_MODEL_KEY

    api_key = resolve_api_key(_cfg(), model_key)

    if not _cfg().dify_api_base:
        return jsonify({"error": "DIFY_API_BASE not set"}), 500
    if not api_key:
        return jsonify({"error": "API key not set"}), 500

    body = request.get_json(force=True)
    message = (body.get("message") or "").strip()
    thread_id = (body.get("thread_id") or "").strip() or None

    if not message:
        return jsonify({"error": "message is empty"}), 400
    if not thread_id:
        thread_id = create_new_thread_id()

    dify_cid_in = get_dify_cid(_cfg(), u["user_id"], thread_id, model_key)

    ts_user = append_history(_cfg(), u["user_id"], "user", model_key, thread_id, dify_cid_in, message)
    upsert_thread(_cfg(), u["user_id"], thread_id, message[:20], ts_user)

    def generate():
        answer_acc = ""
        dify_cid = dify_cid_in

        try:
            with requests.post(
                f"{_cfg().dify_api_base}/chat-messages",
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
                        ts_bot = append_history(_cfg(), u["user_id"], "bot", model_key, thread_id, dify_cid, answer_acc)
                        set_dify_cid(_cfg(), u["user_id"], thread_id, model_key, dify_cid, ts_bot)
                        upsert_thread(_cfg(), u["user_id"], thread_id, "", ts_bot)
                        yield sse_pack("done", {"thread_id": thread_id, "answer": answer_acc, "model": model_key, "ts": ts_bot})
                        break

                    elif ev_type == "error":
                        yield sse_pack("error", {"message": ev.get("message") or "Dify error"})
                        break

        except requests.HTTPError:
            try:
                body_txt = r.text  # type: ignore[name-defined]
            except Exception:
                body_txt = "Dify HTTP error"
            yield sse_pack("error", {"message": body_txt})
        except Exception as e:
            yield sse_pack("error", {"message": str(e)})

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )