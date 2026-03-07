import os
from dataclasses import dataclass


def _getenv(name: str, default: str | None = None) -> str:
    v = os.environ.get(name)
    if v is None:
        return default or ""
    return str(v)


def _getenv_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


@dataclass(frozen=True)
class AppConfig:
    base_dir: str

    # Flask
    secret_key: str

    # Dify
    dify_api_base: str
    default_dify_api_key: str

    # Storage
    users_dir: str
    notice_path: str

    feedback_dir_nas: str
    feedback_dir_local: str

    backup_dir: str
    backup_keep_days: int

    # Performance
    nas_check_ttl_sec: int
    md_rebuild_cooldown_sec: int

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.secret_key:
            errors.append("FLASK_SECRET_KEY is empty")
        if not self.dify_api_base:
            errors.append("DIFY_API_BASE is empty")
        # default key can be empty if all per-model keys are set
        return errors


def load_config(base_dir: str) -> AppConfig:
    base_dir = os.path.abspath(base_dir)

    users_dir = os.path.join(base_dir, "users")
    notice_path = os.path.join(base_dir, "notice.txt")

    feedback_dir_local = os.path.join(base_dir, "_spool", "good_and_bad")

    return AppConfig(
        base_dir=base_dir,
        secret_key=_getenv("FLASK_SECRET_KEY", "dev-secret-change-me"),
        dify_api_base=_getenv("DIFY_API_BASE", "http://161.93.108.55:8890/v1").rstrip("/"),
        default_dify_api_key=_getenv("DIFY_API_KEY", "").strip(),
        users_dir=users_dir,
        notice_path=notice_path,
        feedback_dir_nas=_getenv("FEEDBACK_DIR_NAS", r"\\172.27.23.54\disk1\Chuppy\good_and_bad"),
        feedback_dir_local=_getenv("FEEDBACK_DIR_LOCAL", feedback_dir_local),
        backup_dir=_getenv("BACKUP_DIR", os.path.join(base_dir, "_backup")),
        backup_keep_days=_getenv_int("BACKUP_KEEP_DAYS", 30),
        nas_check_ttl_sec=_getenv_int("NAS_CHECK_TTL_SEC", 5),
        md_rebuild_cooldown_sec=_getenv_int("MD_REBUILD_COOLDOWN_SEC", 10),
    )