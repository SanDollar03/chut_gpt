import os

from flask import Flask

from config import load_config
from .core import ensure_dir, ensure_feedback_state_csv, ensure_notice_file, is_nas_available_cached
from .blueprints.auth import bp as auth_bp
from .blueprints.api_chat import bp as api_chat_bp
from .blueprints.api_threads import bp as api_threads_bp
from .blueprints.api_feedback import bp as api_feedback_bp


def create_app(base_dir: str | None = None) -> Flask:
    base_dir = base_dir or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config(base_dir)

    app = Flask(
        __name__,
        static_folder=os.path.join(cfg.base_dir, "static"),
        template_folder=os.path.join(cfg.base_dir, "templates"),
    )
    app.secret_key = cfg.secret_key

    app.config["APP_CFG"] = cfg

    errs = cfg.validate()
    if errs:
        app.logger.warning("Config validation warnings: %s", "; ".join(errs))

    ensure_dir(cfg.users_dir)
    ensure_dir(cfg.feedback_dir_local)
    ensure_dir(cfg.backup_dir)
    ensure_notice_file(cfg)
    ensure_feedback_state_csv(cfg.feedback_dir_local)
    if is_nas_available_cached(cfg):
        ensure_feedback_state_csv(cfg.feedback_dir_nas)

    app.register_blueprint(auth_bp)
    app.register_blueprint(api_chat_bp)
    app.register_blueprint(api_threads_bp)
    app.register_blueprint(api_feedback_bp)

    @app.get("/ping")
    def ping():
        return "pong"

    return app