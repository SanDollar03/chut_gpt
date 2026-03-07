from flask import Blueprint, redirect, render_template, request, session, url_for

from ..core import ID7_RE, create_user_files, user_exists, verify_user

bp = Blueprint("auth", __name__)


@bp.get("/login")
def login():
    return render_template("login.html")


@bp.post("/login")
def login_post():
    user_id = (request.form.get("user_id") or "").strip()
    password = request.form.get("password") or ""

    if not ID7_RE.match(user_id):
        return render_template("login.html", error="IDは数字7桁で入力してください。", user_id=user_id)

    # cfgはsessionだけで判定できるので、ここはverify_userに委譲（core側でCSV操作）
    from flask import current_app
    cfg = current_app.config["APP_CFG"]

    if not verify_user(cfg, user_id, password):
        return render_template("login.html", error="IDまたはパスワードが違います。", user_id=user_id)

    session["user_id"] = user_id
    return redirect(url_for("auth.index"))


@bp.get("/register")
def register():
    return render_template("register.html")


@bp.post("/register")
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

    from flask import current_app
    cfg = current_app.config["APP_CFG"]

    if user_exists(cfg, user_id):
        return render_template("register.html", error="既に登録されています。", user_id=user_id)

    create_user_files(cfg, user_id, password)
    session["user_id"] = user_id
    return redirect(url_for("auth.index"))


@bp.post("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))


@bp.get("/")
def index():
    from flask import current_app
    cfg = current_app.config["APP_CFG"]

    uid = session.get("user_id")
    if not uid or not user_exists(cfg, uid):
        session.clear()
        return redirect(url_for("auth.login"))

    return render_template("index.html", user_id=uid)