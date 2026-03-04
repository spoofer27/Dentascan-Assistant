import os
import sys
import threading
import time

import servicemanager

try:
    from .uploading_logic import UploadingLogic
except ImportError:
    from uploading_logic import UploadingLogic

try:
    from service.unified_logging import get_service_logger
except Exception:
    service_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if service_dir not in sys.path:
        sys.path.insert(0, service_dir)
    from unified_logging import get_service_logger

logger = get_service_logger(__name__)


def _post_ui_log(message: str, source: str = "ServiceLog", color: str | None = None):
    text = f"[{source}] {message}"
    if color == "red":
        logger.error(text)
    elif color == "yellow":
        logger.warning(text)
    else:
        logger.info(text)


def _run_console_debug():
    stop_event = threading.Event()
    logger.info("Starting uploading console debug mode...")
    logger.info("Press Ctrl+C to stop.")

    try:
        main(stop_event)
    except KeyboardInterrupt:
        stop_event.set()
        logger.info("Keyboard interrupt received. Stopping uploading debug mode...")


def _sleep_or_stop(stop_event, seconds: int) -> bool:
    if stop_event is None:
        time.sleep(seconds)
        return False
    if hasattr(stop_event, "wait"):
        return bool(stop_event.wait(timeout=seconds))
    time.sleep(seconds)
    return False


def _log_summary(day_label: str, summary: dict):
    found = int(summary.get("found", 0))
    uploaded = int(summary.get("uploaded", 0))
    skipped = int(summary.get("skipped", 0))
    failed = int(summary.get("failed", 0))
    labels_added = int(summary.get("labels_added", 0))
    label_errors = int(summary.get("label_errors", 0))

    if uploaded == 0 and failed == 0 and labels_added == 0 and label_errors == 0:
        return

    color = "red" if (failed > 0 or label_errors > 0) else None
    _post_ui_log(
        f"{day_label} upload scan: found={found}, uploaded={uploaded}, "
        f"skipped={skipped}, failed={failed}, labels_added={labels_added}, label_errors={label_errors}",
        source="Uploading",
        color=color,
    )


def main(stop_event):
    _post_ui_log("Uploading worker is starting...", source="ServiceLog")
    try:
        while stop_event is None or not stop_event.is_set():
            uploader = None

            try:
                uploader = UploadingLogic.from_config()
            except Exception as exc:
                _post_ui_log(f"Error initializing uploading logic: {exc}", source="ServiceLog", color="red")
                servicemanager.LogErrorMsg(f"Error initializing uploading logic: {exc}")

            if uploader is not None:
                try:
                    today_folder = uploader.ensure_today_staging_folder()
                    today_summary = uploader.process_staging_folder(today_folder)
                    _log_summary("Today", today_summary)
                except Exception as exc:
                    _post_ui_log(f"Error processing today staging folder: {exc}", source="ServiceLog", color="red")
                    servicemanager.LogErrorMsg(f"Error processing today staging folder: {exc}")

                try:
                    yesterday_folder = uploader.ensure_yesterday_staging_folder()
                    yesterday_summary = uploader.process_y_staging_folder(yesterday_folder)
                    _log_summary("Yesterday", yesterday_summary)
                except Exception as exc:
                    _post_ui_log(f"Error processing yesterday staging folder: {exc}", source="ServiceLog", color="red")
                    servicemanager.LogErrorMsg(f"Error processing yesterday staging folder: {exc}")

            if _sleep_or_stop(stop_event, seconds=5):
                return

    except Exception as exc:
        _post_ui_log(f"Error in uploading thread: {exc}", source="ServiceLog", color="red")
        servicemanager.LogErrorMsg(f"Error in uploading thread: {exc}")
        raise


if __name__ == "__main__":
    _run_console_debug()
