import servicemanager
import time
from pathlib import Path
import os
import sys
import logging
import threading

try:
    import service_config
    from service_config import SERVICE_NAME
except ImportError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    service_dir = os.path.dirname(current_dir)
    if service_dir not in sys.path:
        sys.path.insert(0, service_dir)
    import service_config
    from service_config import SERVICE_NAME

try:
    from .staging_logic import StagingLogic
except ImportError:
    from staging_logic import StagingLogic

try:
    from service.unified_logging import get_service_logger
except Exception:
    service_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if service_dir not in sys.path:
        sys.path.insert(0, service_dir)
    from unified_logging import get_service_logger

logger = get_service_logger(__name__)
import importlib
try:
    from . import ris_logic
    logger.info("Imported ris_logic via package-relative import")
except ImportError:
    try:
        ris_logic = importlib.import_module("service.service_logic.ris_logic")
        logger.info("Imported ris_logic via service.service_logic path")
    except Exception:
        try:
            service_logic_dir = os.path.dirname(os.path.abspath(__file__))
            if service_logic_dir not in sys.path:
                sys.path.insert(0, service_logic_dir)
            import ris_logic
            logger.info("Imported ris_logic via local-path fallback")
        except ImportError as exc:
            logger.exception("Failed to import ris_logic module", exc_info=exc)
            ris_logic = None
    
if ris_logic is None:
    logger.error("ris_logic is unavailable after all import attempts")
else:
    logger.info("ris_logic module is ready")

def _run_console_debug():
    stop_event = threading.Event()
    logger.info("Starting staging console debug mode...")
    logger.info("Press Ctrl+C to stop.")

    try:
        main(stop_event)
    except KeyboardInterrupt:
        stop_event.set()
        logger.info("Keyboard interrupt received. Stopping staging debug mode...")

def _post_ui_log(message: str, source: str = "ServiceLog", color: str | None = None):
    text = f"[{source}] {message}"
    if color == "red":
        logger.error(text)
    else:
        logger.info(text)


def main(stop_event):
    _post_ui_log("Staging worker is starting...", source="ServiceLog")
    try:
        yesterday_check_counter = 0
        ris_counter = 60 # start with 60 to trigger RIS login immediately
        while not stop_event.is_set():
            monitor = None
            try: # calling basic StagingLogic class
                monitor = StagingLogic.from_config() # gets configurations
                folder_path = monitor.ensure_today_folder() # gets today's folder
                staging_folder_path = monitor.ensure_today_staging_folder() # gets today's staging folder
            except Exception as exc:
                logger.exception("Error initializing staging logic")
                servicemanager.LogErrorMsg(f"Error initializing staging logic: {exc}")
                _post_ui_log(f"Error initializing staging logic: {exc}", source="ServiceLog", color="red")

            if monitor is not None:
                try:
                    folder_path = monitor.ensure_today_folder()
                    staging_folder_path = monitor.ensure_today_staging_folder()
                    case_count, cases = monitor.find_cases()
                    now = time.localtime()
                    date_str = time.strftime("%d-%m-%Y", now)
                    hour = time.strftime("%I", now).lstrip("0") or "12"
                    minute = time.strftime("%M", now)
                    suffix = time.strftime("%p", now).lower()
                    header_time = f"{hour}.{minute}{suffix}"
                    logger.info("%s %s - Found %s Cases", date_str, header_time, case_count)
                    _post_ui_log(f"{date_str} {header_time} - Found {case_count} Cases", source="ServiceLog")
                    for idx, case in enumerate(cases, start=1):
                        name = case.get("name", "")
                        case_date = case.get("date", "")
                        case_time = case.get("time", "")
                        case_has_pdf = case.get("has_pdf", False)
                        case_pdf_count = case.get("pdf_count", 0)
                        case_has_images = case.get("has_images", False)
                        case_image_count = case.get("image_count", 0)
                        case_has_single_dicom = case.get("has_single_dicom", False)
                        case_single_dicom_count = case.get("single_dicom_count", 0)
                        case_has_multiple_dicom = case.get("has_multiple_dicom", False)
                        case_multiple_dicom_count = case.get("multiple_dicom_count", 0)
                        case_has_project = case.get("has_project", False)
                        case_project_count = case.get("project_count", 0)
                        case_romexis = case.get("romexis", False)
                        logger.info(
                            "    %s-%s-%s-%s-PDFs:%s-IMGs:%s-DICOMs:%s-M-DICOMs:%s-Projs:%s-Rmx:%s",
                            idx,
                            name,
                            case_date,
                            case_time,
                            case_pdf_count,
                            case_image_count,
                            case_single_dicom_count,
                            case_multiple_dicom_count,
                            case_project_count,
                            case_romexis,
                        )
                        _post_ui_log(f"     {idx}-{name}-{case_date}-{case_time}-PDFs:{case_pdf_count}-IMGs:{case_image_count}-DICOMs:{case_single_dicom_count}-M-DICOMs:{case_multiple_dicom_count}-Projs:{case_project_count}-Rmx: {case_romexis}", source="ServiceLog")

                    # Check yesterday's cases every 30 seconds (every 6 iterations)
                    yesterday_check_counter += 1 # counnt to 6 loops (30s) to check yesterday's cases
                    if yesterday_check_counter >= 6: # reset counter and check yesterday's cases
                        yesterday_check_counter = 0
                        try:
                            yesterday_count, yesterday_cases = monitor.find_yesterday_cases()
                            if yesterday_count > 0:
                                
                                now = time.localtime()
                                date_str = time.strftime("%d-%m-%Y", now)
                                hour = time.strftime("%I", now).lstrip("0") or "12"
                                minute = time.strftime("%M", now)
                                suffix = time.strftime("%p", now).lower()
                                header_time = f"{hour}.{minute}{suffix}"
                                logger.info("=============================================================================")
                                _post_ui_log("=============================================================================", source="ServiceLog")
                                logger.info("Yesterday recovery: processed %s case(s)", yesterday_count)
                                _post_ui_log(f"Yesterday recovery: processed {yesterday_count} case(s)", source="ServiceLog")
                                logger.info("%s %s - Found %s Cases", date_str, header_time, yesterday_count)
                                _post_ui_log(f"{date_str} {header_time} - Found {yesterday_count} Cases", source="ServiceLog")
                                for idx, case in enumerate(yesterday_cases, start=1):
                                    name = case.get("name", "")
                                    case_date = case.get("date", "")
                                    case_time = case.get("time", "")
                                    case_has_pdf = case.get("has_pdf", False)
                                    case_pdf_count = case.get("pdf_count", 0)
                                    case_has_images = case.get("has_images", False)
                                    case_image_count = case.get("image_count", 0)
                                    case_has_single_dicom = case.get("has_single_dicom", False)
                                    case_single_dicom_count = case.get("single_dicom_count", 0)
                                    case_has_multiple_dicom = case.get("has_multiple_dicom", False)
                                    case_multiple_dicom_count = case.get("multiple_dicom_count", 0)
                                    case_has_project = case.get("has_project", False)
                                    case_project_count = case.get("project_count", 0)
                                    case_romexis = case.get("romexis", False)
                                    logger.info(
                                        "  %s-%s-%s-%s-PDFs:%s-IMGs:%s-DICOMs:%s-M-DICOMs:%s-Projs:%s-Rmx:%s",
                                        idx,
                                        name,
                                        case_date,
                                        case_time,
                                        case_pdf_count,
                                        case_image_count,
                                        case_single_dicom_count,
                                        case_multiple_dicom_count,
                                        case_project_count,
                                        case_romexis,
                                    )
                                    _post_ui_log(f"  {idx}-{name}-{case_date}-{case_time}-PDFs:{case_pdf_count}-IMGs:{case_image_count}-DICOMs:{case_single_dicom_count}-M-DICOMs:{case_multiple_dicom_count}-Projs:{case_project_count}-Rmx: {case_romexis}", source="ServiceLog")

                        except Exception as exc:
                            _post_ui_log(f"Yesterday processing failed: {exc}", source="ServiceLog", color="red")

                    ris_counter += 1 # count to 60 loops (5min) to run RIS
                    if ris_counter >= 60: # reset counter and run RIS
                        ris_counter = 0
                        ris_enabled = ris_logic is not None
                        if ris_enabled:
                            try:
                                ris_logic.start_login()
                                time.sleep(5)
                            except Exception as exc:
                                _post_ui_log(f"Error during RIS login: {exc}", source="StagingLogic")
                        else:
                            _post_ui_log("RIS:) module unavailable; skipping RIS login", source="StagingLogic")

                        
                except Exception as exc:
                    logger.exception("Error in staging loop")
                    servicemanager.LogErrorMsg(f"Error in staging loop: {exc}")
                    _post_ui_log(f"Error in staging loop: {exc}", source="ServiceLog", color="red")
            
            if stop_event is None:
                time.sleep(5)
                continue

            # Wait up to 5s but exit early if stop is requested.
            if hasattr(stop_event, "wait"):
                if stop_event.wait(timeout=5):
                    return
            else:
                time.sleep(5)
    
    except Exception as exc:
        logger.exception("Error in staging thread")
        servicemanager.LogErrorMsg(f"Error in staging thread: {exc}")
        _post_ui_log(f"Error in staging thread: {exc}", source="ServiceLog", color="red")
        raise

if __name__ == "__main__":
    _run_console_debug()