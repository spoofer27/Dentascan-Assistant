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
from datetime import datetime, timedelta
from pathlib import Path

from service_config import SERVICE_NAME, SERVICE_API_HOST, SERVICE_API_PORT, SERVICE_STAGING_PATH
from unified_logging import get_service_logger, get_service_log_path, read_service_log_tail

logger = get_service_logger(__name__)

HOST = os.environ.get("SERVICE_API_HOST", SERVICE_API_HOST)
PORT = int(os.environ.get("SERVICE_API_PORT", str(SERVICE_API_PORT)))

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

_ris_state_lock = threading.Lock()
_ris_online = False
_ris_updated_ts = None


def set_ris_state(online, timestamp=None):
    global _ris_online, _ris_updated_ts
    with _ris_state_lock:
        _ris_online = bool(online)
        _ris_updated_ts = timestamp if isinstance(timestamp, (int, float)) else time.time()


def get_ris_state():
    with _ris_state_lock:
        return {
            "ris_online": _ris_online,
            "ris_updated_ts": _ris_updated_ts,
        }

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


def _ps_single_quoted(value):
    return "'" + str(value).replace("'", "''") + "'"


def _windows_cli_quoted(value):
    text = str(value)
    if not text:
        return '""'
    if any(ch in text for ch in (' ', '\t', '"')):
        return '"' + text.replace('"', '\\"') + '"'
    return text


def run_sc_elevated(args):
    if not IS_WIN:
        return 1, "", "Elevation is only supported on Windows"

    arg_string = " ".join(_windows_cli_quoted(arg) for arg in args)
    script = (
        "$ErrorActionPreference='Stop';"
        f"$p = Start-Process -FilePath 'sc.exe' -ArgumentList {_ps_single_quoted(arg_string)} -Verb RunAs -WindowStyle Hidden -Wait -PassThru;"
        "Write-Output $p.ExitCode"
    )

    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                script,
            ],
            capture_output=True,
            text=True,
            timeout=45,
            creationflags=NO_WINDOW,
        )
    except subprocess.TimeoutExpired:
        return 1, "", "Elevated command timed out"
    except Exception as exc:
        return 1, "", str(exc)

    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()

    if "canceled by the user" in stderr.lower():
        return 1, "", "Administrator approval was canceled"

    if result.returncode != 0:
        return result.returncode, stdout, stderr

    exit_code = None
    if stdout:
        for line in reversed(stdout.splitlines()):
            token = line.strip()
            if token.isdigit():
                exit_code = int(token)
                break
    if exit_code is None:
        exit_code = 0

    if exit_code == 0:
        return 0, "Elevated command completed", ""
    return exit_code, stdout, stderr or "Elevated command failed"


def run_sc_with_auto_elevation(args):
    if IS_WIN and not is_admin():
        return run_sc_elevated(args)
    return run_sc(args)


def _friendly_sc_message(raw_output):
    text = (raw_output or "").strip()
    if not text:
        return text

    known = {
        "5": "Access denied. Run the UI/API as Administrator or approve UAC.",
        "1056": "Service is already running.",
        "1060": "Service is not installed. Use Install first.",
        "1062": "Service is not running.",
    }
    if text in known:
        return f"{known[text]} (code {text})"
    return text


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
        payload = {
            "ok": True,
            "service": SERVICE_NAME,
            "state": state,
            "running": state == "RUNNING",
        }
        payload.update(get_ris_state())
        return payload
    except Exception as e:
        # Fallback to sc.exe (hidden window)
        code, out, err = run_sc(["query", SERVICE_NAME])
        if code != 0:
            payload = {
                "ok": False,
                "service": SERVICE_NAME,
                "state": "UNKNOWN",
                "running": False,
                "error": (err or out).strip(),
            }
            payload.update(get_ris_state())
            return payload
        state = parse_state(out)
        payload = {
            "ok": True,
            "service": SERVICE_NAME,
            "state": state,
            "running": state == "RUNNING",
        }
        payload.update(get_ris_state())
        return payload


def get_cases_data():
    try:
        def _day_folder(target_dt: datetime) -> Path:
            staging_root = Path(SERVICE_STAGING_PATH) / "Staging"
            return (
                staging_root
                / target_dt.strftime("%Y")
                / target_dt.strftime("%m-%Y")
                / target_dt.strftime("%d-%m-%Y")
            )

        def _load_day_cases(target_dt: datetime) -> list:
            day_folder = _day_folder(target_dt)
            if not day_folder.exists():
                return []

            cases = []
            case_dirs = sorted(
                [item for item in day_folder.iterdir() if item.is_dir()],
                key=lambda p: p.stat().st_mtime if p.exists() else 0,
                reverse=True,
            )
            for case_dir in case_dirs:
                details_file = case_dir / f"{case_dir.name}_details.json"
                if not details_file.exists():
                    matches = list(case_dir.glob("*_details.json"))
                    if not matches:
                        continue
                    details_file = matches[0]

                try:
                    with details_file.open("r", encoding="utf-8") as f:
                        payload = json.load(f)
                    if isinstance(payload, dict):
                        cases.append(payload)
                except Exception:
                    continue
            return cases

        now = datetime.now()
        today_cases = _load_day_cases(now)
        yesterday_cases = _load_day_cases(now - timedelta(days=1))

        return {
            "ok": True,
            "today": today_cases,
            "yesterday": yesterday_cases,
        }
    except Exception as exc:
        append_ui_log(f"Cases API error: {exc}", source="service_api")
        logger.exception("Cases API error")
        return {
            "ok": False,
            "error": str(exc),
            "today": [],
            "yesterday": [],
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

        if parsed.path == "/api/cases":
            payload = get_cases_data()
            write_json(self, 200, payload)
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

        if parsed.path == "/api/service-log":
            qs = parse_qs(parsed.query or "")
            limit = 200
            if "limit" in qs:
                try:
                    limit = int(qs.get("limit", [limit])[0])
                except Exception:
                    pass

            lines = read_service_log_tail(limit=limit)
            payload = {
                "ok": True,
                "path": str(get_service_log_path()),
                "lines": lines,
            }
            write_json(self, 200, payload)
            return

        self.send_error(404, "Not Found")

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/ris-status":
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except Exception:
                length = 0
            body = self.rfile.read(length) if length > 0 else b""
            try:
                data = json.loads(body.decode("utf-8")) if body else {}
            except Exception:
                data = {}

            online = bool(data.get("online", False))
            ts = data.get("timestamp")
            set_ris_state(online, timestamp=ts)
            write_json(self, 200, {"ok": True, "ris_online": online})
            return

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
            code, out, err = run_sc_with_auto_elevation(["start", SERVICE_NAME])
            payload = {"ok": code == 0, "output": _friendly_sc_message((out or err))}
            # Always return 200; UI will read ok + output for user-friendly errors
            write_json(self, 200, payload)
            return

        if parsed.path == "/api/stop":
            code, out, err = run_sc_with_auto_elevation(["stop", SERVICE_NAME])
            payload = {"ok": code == 0, "output": _friendly_sc_message((out or err))}
            # Always return 200; UI will read ok + output for user-friendly errors
            write_json(self, 200, payload)
            return

        if parsed.path == "/api/restart":
            stop_code, stop_out, stop_err = run_sc_with_auto_elevation(["stop", SERVICE_NAME])
            start_code, start_out, start_err = run_sc_with_auto_elevation(["start", SERVICE_NAME])
            ok = stop_code == 0 and start_code == 0
            payload = {
                "ok": ok,
                "stop": _friendly_sc_message((stop_out or stop_err)),
                "start": _friendly_sc_message((start_out or start_err)),
            }
            # Always return 200; UI will surface details via payload
            write_json(self, 200, payload)
            return

        if parsed.path == "/api/reconnect":
            write_json(self, 200, {"ok": True, "message": "UI reconnected"})
            return

        if parsed.path == "/api/install":
            if not is_admin():
                write_json(self, 200, {"ok": False, "output": "Administrator privileges required"})
                return
            code, out, err = run_wrapper(["install"])
            payload = {"ok": code == 0, "output": (out or err).strip()}
            write_json(self, 200 if code == 0 else 500, payload)
            return

        if parsed.path == "/api/uninstall":
            logger.info("Uninstall requested")
            if not is_admin():
                logger.warning("Uninstall failed: not admin")
                write_json(self, 403, {"ok": False, "output": "Administrator privileges required"})
                return

            code, out, err = run_wrapper(["remove"])

            payload = {
                "ok": code == 0,
                "output": (out or err).strip(),
            }
            write_json(self, 200 if code == 0 else 500, payload)
            return

        self.send_error(404, "Not Found")

    def log_message(self, format, *args):
        return


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    logger.info("Service API running on http://%s:%s", HOST, PORT)
    logger.info("Service name: %s", SERVICE_NAME)
    server.serve_forever()


if __name__ == "__main__":
    main()
