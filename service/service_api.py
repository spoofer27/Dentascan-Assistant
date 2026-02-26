import json
import os
import subprocess
import sys
import ctypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from collections import deque
import threading
import time
from ctypes import wintypes

from service_config import SERVICE_NAME
HOST = os.environ.get("SERVICE_API_HOST", "127.0.0.1")
PORT = int(os.environ.get("SERVICE_API_PORT", "8085"))

IS_WIN = os.name == "nt"
NO_WINDOW = subprocess.CREATE_NO_WINDOW if IS_WIN and hasattr(subprocess, "CREATE_NO_WINDOW") else 0

# Windows SCM constants
SC_MANAGER_CONNECT = 0x0001
SERVICE_QUERY_STATUS = 0x0004
SC_STATUS_PROCESS_INFO = 0  # for QueryServiceStatusEx

SERVICE_STOPPED = 0x00000001
SERVICE_START_PENDING = 0x00000002
SERVICE_STOP_PENDING = 0x00000003
SERVICE_RUNNING = 0x00000004
SERVICE_CONTINUE_PENDING = 0x00000005
SERVICE_PAUSE_PENDING = 0x00000006
SERVICE_PAUSED = 0x00000007

STATE_MAP = {
    SERVICE_STOPPED: "STOPPED",
    SERVICE_START_PENDING: "START_PENDING",
    SERVICE_STOP_PENDING: "STOP_PENDING",
    SERVICE_RUNNING: "RUNNING",
    SERVICE_CONTINUE_PENDING: "CONTINUE_PENDING",
    SERVICE_PAUSE_PENDING: "PAUSE_PENDING",
    SERVICE_PAUSED: "PAUSED",
}

# In-memory UI log buffer
_ui_log_buffer = deque(maxlen=1000)
_ui_log_lock = threading.Lock()
_ui_log_next_id = 1

def append_ui_log(message, source=None, timestamp=None):
    global _ui_log_next_id
    if not isinstance(message, str):
        return None
    ts = timestamp if isinstance(timestamp, (int, float)) else time.time()
    with _ui_log_lock:
        entry = {
            "id": _ui_log_next_id,
            "message": message,
            "source": source or "service",
            "ts": ts,
        }
        _ui_log_buffer.appendleft(entry)  # newest-first for easy slicing
        _ui_log_next_id += 1
        return entry

def get_ui_logs(since_id=None, limit=200):
    # Returns newest-first logs, optionally filtered by id
    with _ui_log_lock:
        if since_id is None:
            return list(list(_ui_log_buffer)[:limit])
        try:
            sid = int(since_id)
        except Exception:
            sid = None
        if sid is None:
            return list(list(_ui_log_buffer)[:limit])
        return [e for e in _ui_log_buffer if e["id"] >= sid][:limit]

class SERVICE_STATUS_PROCESS(ctypes.Structure):
    _fields_ = [
        ("dwServiceType", wintypes.DWORD),
        ("dwCurrentState", wintypes.DWORD),
        ("dwControlsAccepted", wintypes.DWORD),
        ("dwWin32ExitCode", wintypes.DWORD),
        ("dwServiceSpecificExitCode", wintypes.DWORD),
        ("dwCheckPoint", wintypes.DWORD),
        ("dwWaitHint", wintypes.DWORD),
        ("dwProcessId", wintypes.DWORD),
        ("dwServiceFlags", wintypes.DWORD),
    ]

def query_service_state(service_name):
    advapi32 = ctypes.WinDLL("Advapi32", use_last_error=True)

    OpenSCManagerW = advapi32.OpenSCManagerW
    OpenSCManagerW.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
    OpenSCManagerW.restype = wintypes.HANDLE

    OpenServiceW = advapi32.OpenServiceW
    OpenServiceW.argtypes = [wintypes.HANDLE, wintypes.LPCWSTR, wintypes.DWORD]
    OpenServiceW.restype = wintypes.HANDLE

    QueryServiceStatusEx = advapi32.QueryServiceStatusEx
    QueryServiceStatusEx.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(SERVICE_STATUS_PROCESS),
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    QueryServiceStatusEx.restype = wintypes.BOOL

    CloseServiceHandle = advapi32.CloseServiceHandle
    CloseServiceHandle.argtypes = [wintypes.HANDLE]
    CloseServiceHandle.restype = wintypes.BOOL

    scm = svc = None
    try:
        scm = OpenSCManagerW(None, None, SC_MANAGER_CONNECT)
        if not scm:
            raise ctypes.WinError(ctypes.get_last_error())

        svc = OpenServiceW(scm, service_name, SERVICE_QUERY_STATUS)
        if not svc:
            raise ctypes.WinError(ctypes.get_last_error())

        status = SERVICE_STATUS_PROCESS()
        needed = wintypes.DWORD(0)
        ok = QueryServiceStatusEx(
            svc, SC_STATUS_PROCESS_INFO,
            ctypes.byref(status),
            ctypes.sizeof(status),
            ctypes.byref(needed),
        )
        if not ok:
            raise ctypes.WinError(ctypes.get_last_error())

        return STATE_MAP.get(status.dwCurrentState, "UNKNOWN")
    finally:
        if svc:
            CloseServiceHandle(svc)
        if scm:
            CloseServiceHandle(scm)
            

def run_sc(args):
    result = subprocess.run(
        ["sc"] + args,
        capture_output=True,
        text=True,
        timeout=10,
        creationflags=NO_WINDOW,
    )
    return result.returncode, result.stdout, result.stderr


def run_wrapper(args):
    wrapper_path = os.path.join(os.path.dirname(__file__), "Service_Wrapper.py")
    result = subprocess.run(
        [sys.executable, wrapper_path] + args,
        capture_output=True,
        text=True,
        timeout=30,
        cwd=os.path.dirname(wrapper_path),
        creationflags=NO_WINDOW,
    )
    return result.returncode, result.stdout, result.stderr


def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False


def parse_state(sc_output):
    state = "UNKNOWN"
    for line in sc_output.splitlines():
        if "STATE" in line:
            parts = line.split(":", 1)
            if len(parts) == 2:
                tail = parts[1].strip()
                tokens = tail.split()
                if len(tokens) >= 2:
                    state = tokens[1].upper()
                elif len(tokens) == 1:
                    state = tokens[0].upper()
            break
    return state


def get_service_status():
    try:
        state = query_service_state(SERVICE_NAME)
        return {
            "ok": True,
            "service": SERVICE_NAME,
            "state": state,
            "running": state == "RUNNING",
        }
    except Exception as e:
        # Fallback to sc.exe (hidden window)
        code, out, err = run_sc(["query", SERVICE_NAME])
        if code != 0:
            return {
                "ok": False,
                "service": SERVICE_NAME,
                "state": "UNKNOWN",
                "running": False,
                "error": (err or out).strip(),
            }
        state = parse_state(out)
        return {
            "ok": True,
            "service": SERVICE_NAME,
            "state": state,
            "running": state == "RUNNING",
        }


def write_json(handler, status_code, payload):
    data = json.dumps(payload).encode("utf-8")
    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/status":
            status = get_service_status()
            status["apiConnected"] = True
            write_json(self, 200, status)
            return

        if parsed.path == "/api/ui-log":
            qs = parse_qs(parsed.query or "")
            since_id = None
            limit = 200
            if "since_id" in qs:
                since_id = qs.get("since_id", [None])[0]
            if "limit" in qs:
                try:
                    limit = int(qs.get("limit", [limit])[0])
                except Exception:
                    pass
            logs = get_ui_logs(since_id=since_id, limit=limit)
            payload = {"ok": True, "logs": logs}
            write_json(self, 200, payload)
            return

        self.send_error(404, "Not Found")

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/ui-log":
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except Exception:
                length = 0
            body = b""
            if length > 0:
                body = self.rfile.read(length)
            try:
                data = json.loads(body.decode("utf-8")) if body else {}
            except Exception:
                data = {}
            msg = data.get("message")
            source = data.get("source")
            ts = data.get("timestamp")
            entry = append_ui_log(str(msg) if msg is not None else None, source=source, timestamp=ts)
            if entry is None:
                write_json(self, 400, {"ok": False, "error": "Invalid message"})
            else:
                write_json(self, 200, {"ok": True, "entry": entry})
            return
        if parsed.path == "/api/connect":
            write_json(self, 200, {"ok": True, "message": "Connected"})
            return

        if parsed.path == "/api/disconnect":
            write_json(self, 200, {"ok": True, "message": "Disconnected"})
            return
        if parsed.path == "/api/start":
            # Admin privileges are required to control Windows services
            if not is_admin():
                write_json(self, 200, {"ok": False, "output": "Administrator privileges required"})
                return
            code, out, err = run_sc(["start", SERVICE_NAME])
            payload = {"ok": code == 0, "output": (out or err).strip()}
            # Always return 200; UI will read ok + output for user-friendly errors
            write_json(self, 200, payload)
            return

        if parsed.path == "/api/stop":
            # Admin privileges are required to control Windows services
            if not is_admin():
                write_json(self, 200, {"ok": False, "output": "Administrator privileges required"})
                return
            code, out, err = run_sc(["stop", SERVICE_NAME])
            payload = {"ok": code == 0, "output": (out or err).strip()}
            # Always return 200; UI will read ok + output for user-friendly errors
            write_json(self, 200, payload)
            return

        if parsed.path == "/api/restart":
            # Admin privileges are required to control Windows services
            if not is_admin():
                write_json(self, 200, {"ok": False, "output": "Administrator privileges required"})
                return
            stop_code, stop_out, stop_err = run_sc(["stop", SERVICE_NAME])
            start_code, start_out, start_err = run_sc(["start", SERVICE_NAME])
            ok = stop_code == 0 and start_code == 0
            payload = {
                "ok": ok,
                "stop": (stop_out or stop_err).strip(),
                "start": (start_out or start_err).strip(),
            }
            # Always return 200; UI will surface details via payload
            write_json(self, 200, payload)
            return

        if parsed.path == "/api/reconnect":
            write_json(self, 200, {"ok": True, "message": "UI reconnected"})
            return

        if parsed.path == "/api/install":
            # code, out, err = run_wrapper(["install"])
            code, out, err = run_sc(["install", SERVICE_NAME])
            payload = {"ok": code == 0, "output": (out or err).strip()}
            write_json(self, 200 if code == 0 else 500, payload)
            return

        if parsed.path == "/api/uninstall":
            print("Uninstall requested")
            if not is_admin():
                print("Uninstall failed: not admin")
                write_json(self, 403, {"ok": False, "output": "Administrator privileges required"})
                return

            # code, out, err = run_wrapper(["remove"])
            stop_code, stop_out, stop_err = run_sc(["stop", SERVICE_NAME])
            code, out, err = run_sc(["delete", SERVICE_NAME])

            payload = {
                "ok": code == 0,
                "stop": (stop_out or stop_err).strip(),
                "delete": (out or err).strip(),
            }
            write_json(self, 200 if code == 0 else 500, payload)
            return

        self.send_error(404, "Not Found")

    def log_message(self, format, *args):
        return


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Service API running on http://{HOST}:{PORT}")
    print(f"Service name: {SERVICE_NAME}")
    server.serve_forever()


if __name__ == "__main__":
    main()
