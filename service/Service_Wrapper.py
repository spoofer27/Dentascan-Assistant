import os
import sys
import subprocess
import servicemanager
import win32event
import win32service
import win32serviceutil
import threading
import time
from service_config import SERVICE_NAME
from unified_logging import get_service_logger

logger = get_service_logger(__name__)


def _preferred_api_python() -> str:
    exe_dir = os.path.dirname(sys.executable)
    python_exe = os.path.join(exe_dir, "python.exe")
    pythonw_exe = os.path.join(exe_dir, "pythonw.exe")
    if os.path.exists(python_exe):
        return python_exe
    if os.path.exists(pythonw_exe):
        return pythonw_exe
    return sys.executable


def _start_api_process(log_info, log_error):
    api_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "service_api.py")
    if not os.path.exists(api_script):
        log_error(f"API script not found: {api_script}")
        return None

    env = dict(os.environ)
    creationflags = subprocess.CREATE_NO_WINDOW if (os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW")) else 0

    try:
        process = subprocess.Popen(
            [_preferred_api_python(), api_script],
            cwd=os.path.dirname(api_script),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
            creationflags=creationflags,
        )
        log_info(f"api process started. pid={process.pid}")
        return process
    except Exception as exc:
        log_error(f"Failed to start api process: {exc}")
        return None


def _run_worker_supervisor(stop_event, wait_for_stop_ms, log_info, log_error):
    MAX_CRASHES = 5
    BASE_BACKOFF = 2

    try:
        from service_logic import staging
        from service_logic import uploading
    except Exception as exc:
        log_error(f"Failed to import modules: {exc}")
        return

    workers = {
        "staging": {
            "target": staging.main,
            "thread": None,
            "crash_count": 0,
            "last_crash": 0,
        },
        "uploading": {
            "target": uploading.main,
            "thread": None,
            "crash_count": 0,
            "last_crash": 0,
        }
    }

    api_process = None
    api_crash_count = 0

    def start_worker(name):
        worker = workers[name]
        thread = threading.Thread(
            target=worker["target"],
            args=(stop_event,),
            daemon=False,
        )
        thread.start()
        worker["thread"] = thread
        log_info(f"{name} thread started.")

    try:
        for name in workers:
            start_worker(name)
        api_process = _start_api_process(log_info=log_info, log_error=log_error)
    except Exception as exc:
        log_error(f"Failed to start worker threads: {exc}")

    while not stop_event.is_set():
        should_exit = False
        for name, worker in workers.items():
            thread = worker["thread"]
            if thread is not None and thread.is_alive():
                continue

            now = time.time()
            worker["crash_count"] += 1
            worker["last_crash"] = now
            log_error(f"{name} crashed ({worker['crash_count']} times).")

            if worker["crash_count"] >= MAX_CRASHES:
                log_error(f"{name} exceeded max crashes. Not restarting.")
                continue

            delay = BASE_BACKOFF * (2 ** (worker["crash_count"] - 1))
            log_info(f"Restarting {name} in {delay} seconds...")
            if wait_for_stop_ms(int(delay * 1000)):
                log_info("Stop signal received during backoff wait. Exiting supervisor loop.")
                should_exit = True
                break

            start_worker(name)

        if api_process is None:
            api_crash_count += 1
            if api_crash_count <= MAX_CRASHES:
                delay = BASE_BACKOFF * (2 ** (api_crash_count - 1))
                log_info(f"Restarting api in {delay} seconds...")
                if wait_for_stop_ms(int(delay * 1000)):
                    log_info("Stop signal received during API backoff wait. Exiting supervisor loop.")
                    should_exit = True
                else:
                    api_process = _start_api_process(log_info=log_info, log_error=log_error)
            else:
                log_error("api exceeded max crashes. Not restarting.")
        else:
            api_exit_code = api_process.poll()
            if api_exit_code is not None:
                api_crash_count += 1
                log_error(f"api exited unexpectedly with code {api_exit_code} ({api_crash_count} times).")
                api_process = None

        if should_exit:
            break

        wait_for_stop_ms(2000)

    log_info("Stop signal received. Waiting for threads to finish...")
    for worker in workers.values():
        if worker["thread"] is not None:
            worker["thread"].join(timeout=30)

    if api_process is not None and api_process.poll() is None:
        try:
            api_process.terminate()
            api_process.wait(timeout=10)
            log_info("api process terminated cleanly.")
        except Exception:
            try:
                api_process.kill()
                log_info("api process killed.")
            except Exception:
                pass

    log_info("Service stopped cleanly.")


def _run_console_debug():
    stop_event = threading.Event()

    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(line_buffering=True, write_through=True)
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(line_buffering=True, write_through=True)
    except Exception:
        pass

    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        os.chdir(parent_dir)
    except Exception as exc:
        logger.exception("Failed to change directory")

    logger.info("Starting console debug mode... PID: %s", os.getpid())
    logger.info("Press Ctrl+C to stop.")

    def wait_for_stop_ms(timeout_ms):
        return stop_event.wait(timeout_ms / 1000)

    def log_info(message):
        logger.info(message)

    def log_error(message):
        logger.error(message)

    try:
        _run_worker_supervisor(
            stop_event=stop_event,
            wait_for_stop_ms=wait_for_stop_ms,
            log_info=log_info,
            log_error=log_error,
        )
    except KeyboardInterrupt:
        stop_event.set()
        logger.info("Keyboard interrupt received. Stopping...")

class MyService(win32serviceutil.ServiceFramework):
    _svc_name_ = SERVICE_NAME
    _svc_display_name_ = "Dentascan Assistant Service"
    _svc_description_ = "Dentascan Service for background monitoring, processing, and uploading tasks."
    _svc_type_ = win32service.SERVICE_WIN32_OWN_PROCESS
    _svc_start_type_ = win32service.SERVICE_AUTO_START
    _svc_delayed_start_ = True

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.stop_event = threading.Event()
        self.worker_staging_thread = None
        self.worker_uploading_thread = None

        # setting the current working directory to the project root 
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            parent_dir = os.path.dirname(current_dir)
            os.chdir(parent_dir)
        except Exception as e:
            logger.exception("Failed to change directory")
            servicemanager.LogErrorMsg(f"Failed to change directory: {e}")
    
    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.stop_event.set()
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE, 
            servicemanager.PYS_SERVICE_STARTED, 
            (self._svc_name_, ''))
        try:
            self.main()
        except Exception as exc:
            logger.exception("Service error")
            servicemanager.LogErrorMsg(f"Service error: {exc}")
            raise
    
    def main(self):
        try:
            logger.info("Starting Service... PID: %s", os.getpid())
        except Exception as e:
            logger.exception("Failed to write startup log")
            servicemanager.LogErrorMsg(f"Failed to print PID: {e}")

        def wait_for_stop_ms(timeout_ms):
            waited = win32event.WaitForSingleObject(self.hWaitStop, timeout_ms)
            return waited == win32event.WAIT_OBJECT_0

        def log_info(message):
            logger.info(message)

        def log_error(message):
            logger.error(message)
            servicemanager.LogErrorMsg(message)

        _run_worker_supervisor(
            stop_event=self.stop_event,
            wait_for_stop_ms=wait_for_stop_ms,
            log_info=log_info,
            log_error=log_error,
        )

if __name__ == '__main__':
    cmd_args = [arg.lower() for arg in sys.argv[1:]]

    if "debug" in cmd_args:
        _run_console_debug()
    elif len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MyService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MyService)