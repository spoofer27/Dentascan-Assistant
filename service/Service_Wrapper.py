import os
import sys
import servicemanager
import win32event
import win32service
import win32serviceutil
import threading
import time
from service_config import SERVICE_NAME


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

        if should_exit:
            break

        wait_for_stop_ms(2000)

    log_info("Stop signal received. Waiting for threads to finish...")
    for worker in workers.values():
        if worker["thread"] is not None:
            worker["thread"].join(timeout=30)

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
        print(f"Failed to change directory: {exc}")

    print(f"Starting console debug mode... PID: {os.getpid()}")
    print("Press Ctrl+C to stop.")

    def wait_for_stop_ms(timeout_ms):
        return stop_event.wait(timeout_ms / 1000)

    def log_info(message):
        print(message, flush=True)

    def log_error(message):
        print(message, flush=True)

    try:
        _run_worker_supervisor(
            stop_event=stop_event,
            wait_for_stop_ms=wait_for_stop_ms,
            log_info=log_info,
            log_error=log_error,
        )
    except KeyboardInterrupt:
        stop_event.set()
        print("Keyboard interrupt received. Stopping...")

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
            print(f"Failed to change directory: {e}")
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
            print(f"Service error: {exc}")
            servicemanager.LogErrorMsg(f"Service error: {exc}")
            raise
    
    def main(self):
        try:
            print(f'Starting Service... PID: {os.getpid()}')
        except Exception as e:
            print(f"Failed to print PID: {e}")
            servicemanager.LogErrorMsg(f"Failed to print PID: {e}")

        def wait_for_stop_ms(timeout_ms):
            waited = win32event.WaitForSingleObject(self.hWaitStop, timeout_ms)
            return waited == win32event.WAIT_OBJECT_0

        def log_info(message):
            print(message)

        def log_error(message):
            print(message)
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