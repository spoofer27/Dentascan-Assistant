import logging
import os
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path

_SERVICE_DIR = Path(__file__).resolve().parent
_SERVICE_LOG_PATH = _SERVICE_DIR / "service.log"
_LOGGER_NAME = "dentascan_service"
_CONFIG_LOCK = threading.Lock()
_CONFIGURED = False


def get_service_log_path() -> Path:
    return _SERVICE_LOG_PATH


def configure_service_logging(level: int = logging.INFO) -> logging.Logger:
    global _CONFIGURED
    with _CONFIG_LOCK:
        base_logger = logging.getLogger(_LOGGER_NAME)
        if _CONFIGURED:
            return base_logger

        _SERVICE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

        formatter = logging.Formatter(
            # "%(asctime)s | %(levelname)s | %(process)d | %(threadName)s | %(name)s | %(message)s"
            "%(message)s"
        )

        file_handler = RotatingFileHandler(
            _SERVICE_LOG_PATH,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)

        base_logger.setLevel(level)
        base_logger.propagate = False
        base_logger.addHandler(file_handler)

        _CONFIGURED = True
        return base_logger


def get_service_logger(module_name: str) -> logging.Logger:
    configure_service_logging()
    name = module_name or "unknown"
    if name.startswith(f"{_LOGGER_NAME}."):
        return logging.getLogger(name)
    return logging.getLogger(f"{_LOGGER_NAME}.{name}")


def read_service_log_tail(limit: int = 200, max_bytes: int = 1024 * 1024) -> list[str]:
    log_path = get_service_log_path()
    if not log_path.exists():
        return []

    safe_limit = max(1, min(int(limit), 5000))
    safe_max_bytes = max(4096, min(int(max_bytes), 5 * 1024 * 1024))

    with log_path.open("rb") as file_obj:
        file_obj.seek(0, os.SEEK_END)
        file_size = file_obj.tell()
        read_start = max(0, file_size - safe_max_bytes)
        file_obj.seek(read_start, os.SEEK_SET)
        data = file_obj.read()

    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if read_start > 0 and lines:
        lines = lines[1:]
    return lines[-safe_limit:]
