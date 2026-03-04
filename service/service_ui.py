import json
import os
import re
import subprocess
import sys
import time
from urllib import request
from urllib.error import URLError
from ctypes import wintypes
from PyQt5 import QtCore, QtGui, QtWidgets, QtNetwork
import service_config

class UiLogBus(QtCore.QObject):
    line = QtCore.pyqtSignal(str)

_ui_log_bus = UiLogBus()

def _ui_log(*args):
    msg = "[UI] " + " ".join(str(a) for a in args)
    print(msg, flush=True)
    try:
        _ui_log_bus.line.emit(msg)
    except Exception:
        # Emitting may fail before Qt app is fully initialized; ignore
        pass

HOST = service_config.SERVICE_API_HOST
PORT = service_config.SERVICE_API_PORT
API_BASE = f"http://{HOST}:{PORT}"
API_HOST = HOST
API_PORT = PORT
API_SCRIPT = os.path.join(os.path.dirname(__file__), "service_api.py")

# Single-instance server name for UI
UI_SERVER_NAME = "DentascanServiceUI"


def _show_already_running_alert():
    message = "App is Already running"
    if os.name == "nt":
        try:
            ctypes = __import__("ctypes")
            ctypes.windll.user32.MessageBoxW(0, message, "Dentascan Service UI", 0x00000030)
            return
        except Exception:
            pass
    _ui_log(message)


def _prompt_close_other_instances() -> bool:
    message = "App is Already running.\nClose all other instances and open this one?"
    try:
        if os.name == "nt":
            ctypes = __import__("ctypes")
            MB_YESNO = 0x00000004
            MB_ICONWARNING = 0x00000030
            MB_SYSTEMMODAL = 0x00001000
            IDYES = 6
            result = ctypes.windll.user32.MessageBoxW(
                0,
                message,
                "Dentascan Service UI",
                MB_YESNO | MB_ICONWARNING | MB_SYSTEMMODAL,
            )
            return result == IDYES
    except Exception:
        pass

    try:
        reply = QtWidgets.QMessageBox.question(
            None,
            "Dentascan Service UI",
            message,
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No,
        )
        return reply == QtWidgets.QMessageBox.Yes
    except Exception:
        return False


def _get_service_ui_processes():
    if os.name != "nt":
        return []
    try:
        creationflags = subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
        cmd = (
            "Get-CimInstance Win32_Process | "
            "Where-Object { $_.CommandLine -and $_.Name -match 'python|pythonw' -and $_.CommandLine -match 'service_ui\\.py' } | "
            "Select-Object ProcessId,Name,CommandLine | ConvertTo-Json -Compress"
        )
        proc = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                cmd,
            ],
            capture_output=True,
            text=True,
            timeout=5,
            creationflags=creationflags,
        )
        raw = (proc.stdout or "").strip()
        if not raw:
            return []
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            parsed = [parsed]
        return [
            {
                "pid": int(entry.get("ProcessId")),
                "name": entry.get("Name") or "",
                "cmd": entry.get("CommandLine") or "",
            }
            for entry in parsed
            if entry.get("ProcessId")
        ]
    except Exception:
        return []


def _kill_other_service_ui_instances():
    current_pid = os.getpid()
    procs = _get_service_ui_processes()
    for proc in procs:
        pid = proc.get("pid")
        if not pid or pid == current_pid:
            continue
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/F", "/T"],
                capture_output=True,
                text=True,
                timeout=5,
                creationflags=subprocess.CREATE_NO_WINDOW if (os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW")) else 0,
            )
        except Exception:
            pass

def _reexec_with_pythonw_if_needed():
    # If launched with python.exe on Windows, re-exec using pythonw.exe to avoid a console window
    if os.name == "nt" and sys.executable.lower().endswith("python.exe") and not os.environ.get("DSCAN_PYTHONW"):
        pythonw = os.path.join(os.path.dirname(sys.executable), "pythonw.exe")
        if os.path.exists(pythonw):
            env = dict(os.environ)
            env["DSCAN_PYTHONW"] = "1"
            creationflags = subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
            subprocess.Popen(
                [pythonw, os.path.abspath(__file__), *sys.argv[1:]],
                cwd=os.path.dirname(os.path.abspath(__file__)),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creationflags,
                env=env,
            )
            sys.exit(0)


class WorkerSignals(QtCore.QObject):
    finished = QtCore.pyqtSignal(str, object, str)


class RequestWorker(QtCore.QRunnable):
    def __init__(self, method, path):
        super().__init__()
        self.method = method
        self.path = path
        self.signals = WorkerSignals()

    def run(self):
        payload = None
        error = None
        try:
            req = request.Request(API_BASE + self.path, method=self.method)
            with request.urlopen(req, timeout=3) as resp:
                raw = resp.read().decode("utf-8")
                payload = json.loads(raw)
        except URLError as err:
            error = str(err)
        except Exception as err:
            error = str(err)
        self.signals.finished.emit(self.path, payload, error)


class ServiceMonitorApp(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        # Consistent window title to aid external detection (if needed)
        self.setWindowTitle("Dentascan Service UI")
        # Allow window resizing
        self.resize(720, 480)

        self.api_process = None
        self.thread_pool = QtCore.QThreadPool.globalInstance()
        self.is_dark = True
        self.log_path = os.path.join(os.path.dirname(__file__), "service.log")
        self._log_last_mtime = None
        self._log_last_size = None
        self._log_max_lines = 5000

        self._build_ui()
        self._apply_style()

        # Setup system tray for minimize-to-tray behavior
        # self._setup_tray()

        # Connect UI log bus to Live Log view
        _ui_log_bus.line.connect(self._append_ui_log)

        self.poll_timer = QtCore.QTimer(self)
        self.poll_timer.setInterval(5000)
        self.poll_timer.timeout.connect(self._poll_status)
        self.poll_timer.start()

        _ui_log("UI initialized; polling every 5000ms; log refresh every 5000ms")

        # Disable file tail by default; show internal UI logs in Live Log
        self.read_file_log = False
        self.log_timer = QtCore.QTimer(self)
        self.log_timer.setInterval(5000)
        if self.read_file_log:
            self.log_timer.timeout.connect(self._refresh_log)
            self.log_timer.start()
        else:
            if hasattr(self, "log_path_label"):
                self.log_path_label.setText("Unified service log stream from /api/service-log")

        # Poll API for unified service.log lines
        self._service_log_last_lines = []
        self._service_log_recovering = False
        self._service_log_endpoint = "/api/service-log?limit=400"
        self._service_log_retry_count = 0
        self._service_log_last_retry_ts = 0.0
        self._service_log_fallback_mode = False
        self.api_log_timer = QtCore.QTimer(self)
        self.api_log_timer.setInterval(1500)
        self.api_log_timer.timeout.connect(self._poll_api_logs)
        self.api_log_timer.start()

    def _setup_single_instance_server(self, server: QtNetwork.QLocalServer):
        self._server = server
        self._server.newConnection.connect(self._on_server_new_connection)

    def _on_server_new_connection(self):
        conn = self._server.nextPendingConnection()
        if not conn:
            return
        def handle():
            try:
                data = bytes(conn.readAll()).strip()
                if b"SHOW" in data:
                    self.showNormal()
                    self.activateWindow()
                    self._restore_from_tray()
            finally:
                conn.close()
        conn.readyRead.connect(handle)

    def _tray_icon(self):
        # Single normal Windows icon from system style (rasterized)
        base_icon = self.style().standardIcon(QtWidgets.QStyle.SP_ComputerIcon)
        pixmap = base_icon.pixmap(24, 24)
        return QtGui.QIcon(pixmap)

    def _setup_tray(self):
        if not QtWidgets.QSystemTrayIcon.isSystemTrayAvailable():
            self.message_label.setText("System tray not available")
            return

        # Set window icon as well to keep consistent
        self.setWindowIcon(self._tray_icon())

        self.tray = QtWidgets.QSystemTrayIcon(self._tray_icon(), self)
        self.tray.setToolTip("Dentascan Service Monitor")
        # Keep menu and actions referenced to avoid garbage collection
        self._tray_menu = QtWidgets.QMenu()
        self._tray_action_show = self._tray_menu.addAction("Show")
        self._tray_action_quit = self._tray_menu.addAction("Quit")
        self._tray_action_show.triggered.connect(self._restore_from_tray)
        self._tray_action_quit.triggered.connect(QtWidgets.QApplication.instance().quit)
        self.tray.setContextMenu(self._tray_menu)
        self.tray.activated.connect(self._on_tray_activated)
        self.tray.show()

    def _on_tray_activated(self, reason):
        if reason in (QtWidgets.QSystemTrayIcon.Trigger, QtWidgets.QSystemTrayIcon.DoubleClick):
            self._restore_from_tray()

    def _restore_from_tray(self):
        self.showNormal()
        self.activateWindow()

    def _build_ui(self):
        central = QtWidgets.QWidget(self)
        self.setCentralWidget(central)
        root = QtWidgets.QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)
        self._build_top_nav(root)
        self._build_body(root)

    def _build_body(self, layout):
        body = QtWidgets.QWidget()
        body.setObjectName("Body")
        body_layout = QtWidgets.QVBoxLayout(body)
        body_layout.setSpacing(0)
        self.body_stack = QtWidgets.QStackedWidget()
        self.body_stack.setObjectName("BodyStack")
        self._build_main_notebook()
        self.body_stack.addWidget(self.main_notebook)
        self.body_stack.setCurrentWidget(self.main_notebook)
        body_layout.addWidget(self.body_stack)
        layout.addWidget(body, 1)

    def _build_top_nav(self, layout):
        header = QtWidgets.QWidget()
        header.setObjectName("Header")
        header_layout = QtWidgets.QVBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)

        nav_container = QtWidgets.QWidget()
        nav_container.setObjectName("NavBar")
        nav = QtWidgets.QHBoxLayout(nav_container)
        nav.setContentsMargins(12, 8, 12, 8)
        nav.setSpacing(0)

        self.theme_icon_light = self._icon_from_path(os.path.join("res", "icons", "light.svg"))
        self.theme_icon_dark = self._icon_from_path(os.path.join("res", "icons", "dark.svg"))

        self.title = QtWidgets.QLabel("Dentascan Service Monitor")
        self.title.setObjectName("Title")
        nav.addWidget(self.title)
        nav.addStretch(1)

        self.theme_btn = QtWidgets.QToolButton()
        self.theme_btn.setObjectName("NavButton")
        self.theme_btn.setToolTip("Toggle theme")
        self.theme_btn.setIcon(self.theme_icon_dark if self.is_dark else self.theme_icon_light)
        self.theme_btn.setIconSize(QtCore.QSize(24, 24))
        self.theme_btn.setToolButtonStyle(QtCore.Qt.ToolButtonIconOnly)
        self.theme_btn.clicked.connect(self._toggle_theme)
        nav.addWidget(self.theme_btn)

        header_layout.addWidget(nav_container)
        layout.addWidget(header)

    def _icon_from_path(self, icon_path):
        if not os.path.isabs(icon_path):
            icon_path = os.path.join(os.path.dirname(__file__), icon_path)
        if os.path.exists(icon_path):
            return QtGui.QIcon(icon_path)
        return QtGui.QIcon()

    def _build_main_notebook(self):
        self.main_notebook = QtWidgets.QTabWidget()
        self.main_notebook.setObjectName("MainNotebook")
        self.main_notebook.setDocumentMode(True)
        self.main_notebook.setTabPosition(QtWidgets.QTabWidget.North)
        self.main_notebook.setContentsMargins(5, 5, 5, 5)

        self.status_tab = QtWidgets.QWidget()
        self.status_tab.setContentsMargins(5, 5, 5, 5)
        self.conf_tab = QtWidgets.QWidget()
        self.conf_tab.setContentsMargins(5, 5, 5, 5)
        self.log_tab = QtWidgets.QWidget()
        self.log_tab.setContentsMargins(5, 5, 5, 5)
        self._build_service_monitor(self.status_tab)
        self._build_configuration_tab(self.conf_tab)
        self._build_log_tab(self.log_tab)

        self.main_notebook.addTab(self.status_tab, "Status")
        self.main_notebook.addTab(self.conf_tab, "Configuration")
        self.main_notebook.addTab(self.log_tab, "Live Log")

    def _build_configuration_tab(self, parent):
        container = QtWidgets.QVBoxLayout(parent)
        container.setContentsMargins(16, 16, 16, 16)
        container.setSpacing(12)

        title = QtWidgets.QLabel("Service Configuration")
        title.setObjectName("Title")
        container.addWidget(title)

        form = QtWidgets.QFormLayout()
        form.setHorizontalSpacing(12)
        form.setVerticalSpacing(10)

        # Removed Service Name from Configuration tab

        self.config_auto_start = QtWidgets.QCheckBox("Enable auto-start")
        self.config_auto_start.setChecked(service_config.SERVICE_AUTO_START)
        form.addRow("Service AutoStart :", self.config_auto_start)

        self.config_api_host = QtWidgets.QLineEdit(API_HOST)
        form.addRow("API Host :", self.config_api_host)

        self.config_api_port = QtWidgets.QSpinBox()
        self.config_api_port.setRange(1, 65535)
        self.config_api_port.setValue(API_PORT)
        form.addRow("API Port :", self.config_api_port)

        # Separator below API Port
        sep = QtWidgets.QFrame()
        sep.setFrameShape(QtWidgets.QFrame.HLine)
        sep.setFrameShadow(QtWidgets.QFrame.Sunken)
        form.addRow(sep)

        # Local and staging Path selector bound to SERVICE_ROOT_PATH and SERVICE_STAGING_PATH
        self.config_local_path = QtWidgets.QLineEdit(service_config.SERVICE_ROOT_PATH)

        browse_btn = QtWidgets.QPushButton("Browse")
        def do_browse():
            start_dir = self.config_local_path.text().strip() or service_config.DEFAULT_SERVICE_ROOT_PATH
            directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Local Path", start_dir)
            if directory:
                self.config_local_path.setText(directory)
        browse_btn.clicked.connect(do_browse)
        path_row = QtWidgets.QWidget()
        path_row_layout = QtWidgets.QHBoxLayout(path_row)
        path_row_layout.setContentsMargins(0, 0, 0, 0)
        path_row_layout.setSpacing(8)
        path_row_layout.addWidget(self.config_local_path, 1)
        path_row_layout.addWidget(browse_btn)
        form.addRow("Local Path :", path_row)


        self.config_staging_path = QtWidgets.QLineEdit(service_config.SERVICE_STAGING_PATH)

        staging_browse_btn = QtWidgets.QPushButton("Browse")
        def do_browse_staging():
            start_dir = self.config_staging_path.text().strip() or service_config.DEFAULT_SERVICE_STAGING_PATH
            directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Staging Path", start_dir)
            if directory:
                self.config_staging_path.setText(directory)
        staging_browse_btn.clicked.connect(do_browse_staging)
        staging_path_row = QtWidgets.QWidget()
        staging_path_row_layout = QtWidgets.QHBoxLayout(staging_path_row)
        staging_path_row_layout.setContentsMargins(0, 0, 0, 0)
        staging_path_row_layout.setSpacing(8)
        staging_path_row_layout.addWidget(self.config_staging_path, 1)
        staging_path_row_layout.addWidget(staging_browse_btn)
        form.addRow("Staging Path :", staging_path_row)

        self.config_institution_name = QtWidgets.QLineEdit(service_config.INSTITUTION_NAME)
        form.addRow("Institution Name :", self.config_institution_name)

        self.config_pacs_max_upload_kbps = QtWidgets.QSpinBox()
        self.config_pacs_max_upload_kbps.setRange(0, 2_000_000_000)
        self.config_pacs_max_upload_kbps.setSingleStep(100)
        self.config_pacs_max_upload_kbps.setSuffix(" KB/s (0 = unlimited)")
        current_kbps = getattr(service_config, "PACS_MAX_UPLOAD_BPS", None)
        self.config_pacs_max_upload_kbps.setValue(int(current_kbps) if current_kbps else 0)
        form.addRow("PACS Max Upload :", self.config_pacs_max_upload_kbps)

        container.addLayout(form)

        btn_row = QtWidgets.QHBoxLayout()
        btn_row.addStretch(1)
        self.config_save_btn = QtWidgets.QPushButton("Save")
        self.config_save_btn.clicked.connect(self._save_config)
        self.config_reset_btn = QtWidgets.QPushButton("Reset to Default")
        self.config_reset_btn.clicked.connect(self._reset_config)
        btn_row.addWidget(self.config_save_btn)
        btn_row.addWidget(self.config_reset_btn)
        container.addLayout(btn_row)
        container.addStretch(1)

    def _build_service_monitor(self, parent):
        container = QtWidgets.QVBoxLayout(parent)
        container.setContentsMargins(16, 16, 16, 16)
        container.setSpacing(12)

        title = QtWidgets.QLabel("Service Status")
        title.setObjectName("Title")
        container.addWidget(title)

        self.service_name_label = QtWidgets.QLabel(f"Service Name: {service_config.SERVICE_NAME}")
        container.addWidget(self.service_name_label)

        status_grid = QtWidgets.QGridLayout()
        status_grid.setHorizontalSpacing(12)
        status_grid.setVerticalSpacing(8)

        self.service_state_label = QtWidgets.QLabel("Unknown")
        self.api_state_label = QtWidgets.QLabel("Disconnected")
        self.api_process_label = QtWidgets.QLabel("Not running")

        status_grid.addWidget(QtWidgets.QLabel("API Host:"), 0, 0)
        self.api_host_value_label = QtWidgets.QLabel(f"{API_HOST}")
        status_grid.addWidget(self.api_host_value_label, 0, 1)

        status_grid.addWidget(QtWidgets.QLabel("API Port:"), 1, 0)
        self.api_port_value_label = QtWidgets.QLabel(f"{API_PORT}")
        status_grid.addWidget(self.api_port_value_label, 1, 1)

        status_grid.addWidget(QtWidgets.QLabel("API Process:"), 2, 0)
        status_grid.addWidget(self.api_process_label, 2, 1)

        status_grid.addWidget(QtWidgets.QLabel("API Instances:"), 3, 0)
        status_grid.setRowMinimumHeight(3, 32)
        api_instances_row = QtWidgets.QWidget()
        api_instances_layout = QtWidgets.QHBoxLayout(api_instances_row)
        api_instances_layout.setContentsMargins(0, 0, 0, 0)
        api_instances_layout.setSpacing(10)
        self.api_instances_label = QtWidgets.QLabel("Checking...")
        api_instances_layout.addWidget(self.api_instances_label)
        self.api_reset_btn = QtWidgets.QPushButton("Reset API Processes")
        self.api_reset_btn.clicked.connect(self._reset_api_processes)
        self.api_reset_btn.setMinimumHeight(16)
        self.api_reset_btn.setSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Fixed)
        api_instances_layout.addWidget(self.api_reset_btn)
        api_instances_layout.addStretch(1)
        status_grid.addWidget(api_instances_row, 3, 1)

        status_grid.addWidget(QtWidgets.QLabel("API Connected:"), 4, 0)
        status_grid.setRowMinimumHeight(4, 32)
        api_state_row = QtWidgets.QWidget()
        api_state_row.setMinimumHeight(36)
        api_state_layout = QtWidgets.QHBoxLayout(api_state_row)
        api_state_layout.setContentsMargins(0, 0, 0, 0)
        api_state_layout.setSpacing(10)
        api_state_layout.addWidget(self.api_state_label)

        self.api_connect_btn = QtWidgets.QPushButton("Connect")
        self.api_connect_btn.clicked.connect(lambda: self._post_action("/api/connect"))
        self.api_connect_btn.setMinimumHeight(16)
        self.api_connect_btn.setSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Fixed)
        self.api_disconnect_btn = QtWidgets.QPushButton("Disconnect")
        self.api_disconnect_btn.clicked.connect(lambda: self._post_action("/api/disconnect"))
        self.api_disconnect_btn.setMinimumHeight(16)
        self.api_disconnect_btn.setSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Fixed)
        api_state_layout.addWidget(self.api_connect_btn)
        api_state_layout.addWidget(self.api_disconnect_btn)
        api_state_layout.addStretch(1)
        status_grid.addWidget(api_state_row, 4, 1)

        status_grid.addWidget(QtWidgets.QLabel("Service:"), 5, 0)
        status_grid.setRowMinimumHeight(5, 32)
        service_state_row = QtWidgets.QWidget()
        service_state_row.setMinimumHeight(32)
        service_state_layout = QtWidgets.QHBoxLayout(service_state_row)
        service_state_layout.setContentsMargins(0, 0, 0, 0)
        service_state_layout.setSpacing(10)
        service_state_layout.addWidget(self.service_state_label)

        self.service_start_btn = QtWidgets.QPushButton("Start")
        self.service_start_btn.clicked.connect(lambda: self._post_action("/api/start"))
        self.service_start_btn.setMinimumHeight(16)
        self.service_start_btn.setSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Fixed)

        self.service_stop_btn = QtWidgets.QPushButton("Stop")
        self.service_stop_btn.clicked.connect(lambda: self._post_action("/api/stop"))
        self.service_stop_btn.setMinimumHeight(16)
        self.service_stop_btn.setSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Fixed)

        service_state_layout.addWidget(self.service_start_btn)
        service_state_layout.addWidget(self.service_stop_btn)
        service_state_layout.addStretch(1)
        status_grid.addWidget(service_state_row, 5, 1)

        container.addLayout(status_grid)

        self.message_label = QtWidgets.QLabel("Waiting for status...")
        self.message_label.setObjectName("Subtle")
        container.addWidget(self.message_label)
        container.addStretch(1)

    def _build_log_tab(self, parent):
        container = QtWidgets.QVBoxLayout(parent)
        container.setContentsMargins(16, 16, 16, 16)
        container.setSpacing(12)

        title = QtWidgets.QLabel("Live Log")
        title.setObjectName("Title")
        container.addWidget(title)

        self.log_path_label = QtWidgets.QLabel(f"Log file: {self.log_path}")
        self.log_path_label.setObjectName("Subtle")
        container.addWidget(self.log_path_label)

        self.log_view = QtWidgets.QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setLineWrapMode(QtWidgets.QPlainTextEdit.NoWrap)
        font = QtGui.QFont("Consolas")
        font.setStyleHint(QtGui.QFont.Monospace)
        self.log_view.setFont(font)
        container.addWidget(self.log_view, 1)

    def _action_btn(self, label, path):
        btn = QtWidgets.QPushButton(label)
        btn.clicked.connect(lambda: self._post_action(path))
        return btn
    
    def _append_ui_log(self, text, color=None):
        # Prepend text so most recent is on top and cap total lines
        try:
            cursor = self.log_view.textCursor()
            cursor.movePosition(QtGui.QTextCursor.Start)
            
            # If color is specified, apply it to the text block
            if color:
                fmt = QtGui.QTextCharFormat()
                # Map color names to hex values
                color_map = {
                    "red": "#ef4444",
                    "green": "#16a34a",
                    "yellow": "#f59e0b",
                    "blue": "#3b82f6",
                    "orange": "#f59e0b",
                }
                hex_color = color_map.get(color.lower(), color)
                fmt.setForeground(QtGui.QColor(hex_color))
                cursor.insertText(text + "\n", fmt)
            else:
                cursor.insertText(text + "\n")
            
            # Keep view at top to show latest
            cursor = self.log_view.textCursor()
            cursor.movePosition(QtGui.QTextCursor.Start)
            self.log_view.setTextCursor(cursor)
            # Cap to first N lines (newest at top)
            if self.log_view.blockCount() > self._log_max_lines:
                doc_text = self.log_view.document().toPlainText()
                lines = doc_text.splitlines()
                if len(lines) > self._log_max_lines:
                    lines = lines[:self._log_max_lines]
                    self.log_view.setPlainText("\n".join(lines))
                    cursor = self.log_view.textCursor()
                    cursor.movePosition(QtGui.QTextCursor.Start)
                    self.log_view.setTextCursor(cursor)
        except Exception:
            pass

    def _toggle_theme(self):
        self.is_dark = not self.is_dark
        self._apply_style()
        # Update tray icon when theme changes
        if hasattr(self, "tray") and self.tray is not None:
            new_icon = self._tray_icon()
            self.tray.setIcon(new_icon)
            self.setWindowIcon(new_icon)

    def _poll_status(self):
        self._refresh_api_processes_status()
        self._enqueue_request("GET", "/api/status")

    def _get_api_processes(self):
        if os.name != "nt":
            return []
        try:
            creationflags = subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
            cmd = (
                "Get-CimInstance Win32_Process | "
                "Where-Object { $_.CommandLine -and $_.Name -match 'python|pythonw' -and $_.CommandLine -match 'service_api\\.py' } | "
                "Select-Object ProcessId,Name,CommandLine | ConvertTo-Json -Compress"
            )
            proc = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-Command",
                    cmd,
                ],
                capture_output=True,
                text=True,
                timeout=5,
                creationflags=creationflags,
            )
            raw = (proc.stdout or "").strip()
            if not raw:
                return []
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                parsed = [parsed]
            results = []
            for entry in parsed:
                try:
                    pid = int(entry.get("ProcessId"))
                except Exception:
                    continue
                results.append(
                    {
                        "pid": pid,
                        "name": entry.get("Name") or "",
                        "cmd": entry.get("CommandLine") or "",
                    }
                )
            return results
        except Exception:
            return []

    def _refresh_api_processes_status(self):
        if not hasattr(self, "api_instances_label"):
            return
        procs = self._get_api_processes()
        count = len(procs)
        if count <= 0:
            self.api_instances_label.setText("0 (none)")
            self.api_instances_label.setStyleSheet("color: #ef4444;")
        elif count == 1:
            self.api_instances_label.setText(f"1 (PID {procs[0]['pid']})")
            self.api_instances_label.setStyleSheet("color: #16a34a;")
        else:
            pids = ", ".join(str(p["pid"]) for p in procs)
            self.api_instances_label.setText(f"{count} (PIDs: {pids})")
            self.api_instances_label.setStyleSheet("color: #f59e0b;")

    def _preferred_api_python(self):
        service_dir = os.path.dirname(os.path.abspath(__file__))
        workspace_root = os.path.dirname(service_dir)
        candidate = os.path.join(workspace_root, ".venv", "Scripts", "python.exe")
        if os.path.exists(candidate):
            return candidate
        return sys.executable

    def _start_api_process(self):
        creationflags = subprocess.CREATE_NO_WINDOW if (os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW")) else 0
        env = dict(os.environ)
        env["SERVICE_API_HOST"] = str(API_HOST)
        env["SERVICE_API_PORT"] = str(API_PORT)
        self.api_process = subprocess.Popen(
            [self._preferred_api_python(), API_SCRIPT],
            cwd=os.path.dirname(API_SCRIPT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
            env=env,
        )

    def _reset_api_processes(self):
        try:
            procs = self._get_api_processes()
            for proc in procs:
                try:
                    subprocess.run(
                        ["taskkill", "/PID", str(proc["pid"]), "/F", "/T"],
                        capture_output=True,
                        text=True,
                        timeout=5,
                        creationflags=subprocess.CREATE_NO_WINDOW if (os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW")) else 0,
                    )
                except Exception:
                    pass
            self.api_process = None
            self._start_api_process()
            self._refresh_api_processes_status()
            self.message_label.setText("API reset complete (fresh venv process started)")
            self.message_label.setStyleSheet("color: #16a34a;")
        except Exception as err:
            self.message_label.setText(f"API reset failed: {err}")
            self.message_label.setStyleSheet("color: #ef4444;")

    def _poll_api_logs(self):
        try:
            path = self._service_log_endpoint
            self._enqueue_request("GET", path)
        except Exception as e:
            _ui_log("Log poll error:", e)

    def _compute_new_service_log_lines(self, latest_lines):
        previous = list(self._service_log_last_lines or [])
        current = list(latest_lines or [])

        if not previous:
            return current

        max_overlap = min(len(previous), len(current))
        for overlap in range(max_overlap, 0, -1):
            if previous[-overlap:] == current[:overlap]:
                return current[overlap:]

        return current

    def _post_action(self, path):
        self._enqueue_request("POST", path)

    def _enqueue_request(self, method, path):
        worker = RequestWorker(method, path)
        worker.signals.finished.connect(self._handle_response)
        self.thread_pool.start(worker)

    def _update_api_base(self, host, port):
        global API_HOST, API_PORT, API_BASE
        API_HOST = host
        API_PORT = port
        API_BASE = f"http://{API_HOST}:{API_PORT}"
        if hasattr(self, "api_base_label"):
            self.api_base_label.setText(f"API: {API_BASE}")
        if hasattr(self, "api_host_value_label"):
            self.api_host_value_label.setText(str(API_HOST))
        if hasattr(self, "api_port_value_label"):
            self.api_port_value_label.setText(str(API_PORT))

    def _save_config(self):
        auto_start = self.config_auto_start.isChecked()
        host = self.config_api_host.text().strip() or service_config.DEFAULT_SERVICE_API_HOST
        port = int(self.config_api_port.value())
        local_path = self.config_local_path.text().strip() or service_config.DEFAULT_SERVICE_ROOT_PATH
        staging_path = self.config_staging_path.text().strip() or service_config.DEFAULT_SERVICE_STAGING_PATH
        institution_name = self.config_institution_name.text().strip() or service_config.DEFAULT_INSTITUTION_NAME
        pacs_max_upload_kbps_raw = int(self.config_pacs_max_upload_kbps.value())
        pacs_max_upload_kbps = None if pacs_max_upload_kbps_raw <= 0 else pacs_max_upload_kbps_raw
        config_path = os.path.join(os.path.dirname(__file__), "service_config.py")
        try:
            with open(config_path, "r", encoding="utf-8") as fh:
                lines = fh.readlines()
        except FileNotFoundError:
            lines = []

        def replace_line(key, value):
            pattern = re.compile(rf"^(\s*{re.escape(key)}\s*=\s*).*$")
            for i, line in enumerate(lines):
                match = pattern.match(line)
                if match:
                    newline = "\n" if line.endswith("\n") else ""
                    lines[i] = f"{match.group(1)}{value!r}{newline}"
                    return True
            return False

        if not replace_line("SERVICE_AUTO_START", auto_start):
            lines.append(f"SERVICE_AUTO_START = {auto_start!r}\n")
        if not replace_line("SERVICE_API_HOST", host):
            lines.append(f"SERVICE_API_HOST = {host!r}\n")
        if not replace_line("SERVICE_API_PORT", port):
            lines.append(f"SERVICE_API_PORT = {port!r}\n")
        if not replace_line("SERVICE_ROOT_PATH", local_path):
            lines.append(f"SERVICE_ROOT_PATH = {local_path!r}\n")
        if not replace_line("SERVICE_STAGING_PATH", staging_path):
            lines.append(f"SERVICE_STAGING_PATH = {staging_path!r}\n")
        if not replace_line("INSTITUTION_NAME", institution_name):
            lines.append(f"INSTITUTION_NAME = {institution_name!r}\n")
        if not replace_line("PACS_MAX_UPLOAD_BPS", pacs_max_upload_kbps):
            lines.append(f"PACS_MAX_UPLOAD_BPS = {pacs_max_upload_kbps!r}\n")

        with open(config_path, "w", encoding="utf-8") as fh:
            fh.writelines(lines)

        service_config.SERVICE_AUTO_START = auto_start
        service_config.SERVICE_API_HOST = host
        service_config.SERVICE_API_PORT = port
        service_config.SERVICE_ROOT_PATH = local_path
        service_config.SERVICE_STAGING_PATH = staging_path
        service_config.INSTITUTION_NAME = institution_name
        service_config.PACS_MAX_UPLOAD_BPS = pacs_max_upload_kbps

        self._update_api_base(host, port)
        self.message_label.setText("Configuration saved")

    def _reset_config(self):
        self.config_auto_start.setChecked(service_config.DEFAULT_SERVICE_AUTO_START)
        self.config_api_host.setText(service_config.DEFAULT_SERVICE_API_HOST)
        self.config_api_port.setValue(service_config.DEFAULT_SERVICE_API_PORT)
        self.config_local_path.setText(service_config.DEFAULT_SERVICE_ROOT_PATH)
        self.config_staging_path.setText(service_config.DEFAULT_SERVICE_STAGING_PATH)
        self.config_institution_name.setText(service_config.DEFAULT_INSTITUTION_NAME)
        self.config_pacs_max_upload_kbps.setValue(
            int(service_config.DEFAULT_PACS_MAX_UPLOAD_BPS)
            if service_config.DEFAULT_PACS_MAX_UPLOAD_BPS
            else 0
        )
        self._save_config()

    def _try_start_api(self):
        if self.api_process is not None and self.api_process.poll() is None:
            return

        if not os.path.exists(API_SCRIPT):
            return

        try:
            self._start_api_process()
        except Exception as err:
            _ui_log("Failed to start API:", err)

    def _handle_response(self, path, payload, error):
        if error:
            if path.startswith("/api/service-log") and "404" in str(error):
                now = time.time()
                can_retry = (now - self._service_log_last_retry_ts) >= 15.0
                if (not self._service_log_recovering) and (self._service_log_retry_count < 1) and can_retry:
                    self._service_log_recovering = True
                    self._service_log_retry_count += 1
                    self._service_log_last_retry_ts = now
                    _ui_log("/api/service-log returned 404; resetting API process...")
                    try:
                        self._reset_api_processes()
                    except Exception as recover_err:
                        _ui_log("API reset after 404 failed:", recover_err)
                    finally:
                        self._service_log_recovering = False
                elif not self._service_log_fallback_mode:
                    self._service_log_fallback_mode = True
                    self._service_log_endpoint = "/api/ui-log?limit=200"
                    _ui_log("/api/service-log unavailable; switched log polling to /api/ui-log fallback.")
                return

            self.api_state_label.setText("Disconnected")
            self.api_state_label.setStyleSheet("color: #ef4444;")
            self.service_state_label.setText("Unknown")
            self.service_state_label.setStyleSheet("color: #f59e0b;")
            self.message_label.setText(f"API error: {error}")
            self.message_label.setStyleSheet("color: #ef4444;")
            self.api_connect_btn.setEnabled(True)
            self.api_disconnect_btn.setEnabled(False)
            self.service_start_btn.setEnabled(True)
            self.service_stop_btn.setEnabled(False)
            if self.api_process is None:
                self.api_process_label.setText("Not running")
            if path == "/api/status":
                self._try_start_api()
            return

        if path == "/api/status":            
            self.api_state_label.setText("Connected")
            self.api_state_label.setStyleSheet("color: #16a34a;")
            self.api_connect_btn.setEnabled(False)
            self.api_disconnect_btn.setEnabled(True)
            self.service_state_label.setText(payload.get("state", "Unknown"))
            state_text = self.service_state_label.text().strip()
            state_lower = state_text.lower()
            if state_text == "Unknown":
                self.service_state_label.setStyleSheet("color: #f59e0b;")
                self.service_start_btn.setEnabled(True)
                self.service_stop_btn.setEnabled(False)
            elif state_lower in {"stopped", "not running", "failed", "error", "inactive", "stopping"}:
                self.service_state_label.setStyleSheet("color: #ef4444;")
                self.service_start_btn.setEnabled(True)
                self.service_stop_btn.setEnabled(False)
            else:
                self.service_state_label.setStyleSheet("color: #16a34a;")
                self.service_start_btn.setEnabled(False)
                self.service_stop_btn.setEnabled(True)
            if payload.get("service"):
                self.service_name_label.setText(f"Service Name: {payload.get('service')}")
            if self.api_process is not None and self.api_process.poll() is None:
                self.api_process_label.setText("Running")
                self.api_process_label.setStyleSheet("color: #16a34a;")
            elif self.api_process is not None and self.api_process.poll() is not None:
                self.api_process_label.setText("Stopped")
                self.api_process_label.setStyleSheet("")
            else:
                self.api_process_label.setText("Running (external)")
                self.api_process_label.setStyleSheet("color: #16a34a;")
            if payload.get("ok"):
                self.message_label.setText("Status OK")
                self.message_label.setStyleSheet("color: #16a34a;")
            else:
                service_name = payload.get("service") or self.service_name_label.text()
                self.message_label.setText(
                    f"Error: {payload.get('error', 'Unknown')} (Service: {service_name})"
                )
                self.message_label.setStyleSheet("color: #ef4444;")
        elif path.startswith("/api/service-log"):
            try:
                lines = (payload or {}).get("lines") or []
                new_lines = self._compute_new_service_log_lines(lines)
                for line in new_lines:
                    self._append_ui_log(str(line))
                self._service_log_last_lines = list(lines)
                self._service_log_retry_count = 0
            except Exception as e:
                _ui_log("Failed to process api logs:", e)
        elif path.startswith("/api/ui-log"):
            try:
                logs = (payload or {}).get("logs") or []
                for entry in reversed(logs):
                    msg = entry.get("message") or ""
                    if msg:
                        self._append_ui_log(str(msg))
            except Exception as e:
                _ui_log("Failed to process fallback api logs:", e)
        else:
            _ui_log("Action result", path, "ok=" if (payload and payload.get("ok")) else "failed",
                    "msg=", (payload.get("output") or payload.get("message")) if payload else None)
            if payload and payload.get("ok"):
                self.message_label.setText("Action OK")
                self.message_label.setStyleSheet("color: #16a34a;")
            else:
                out = None
                if payload:
                    out = payload.get("output") or payload.get("message")
                self.message_label.setText(f"Action failed: {out or 'Unknown'}")
                self.message_label.setStyleSheet("color: #ef4444;")

    def _apply_style(self):
        if hasattr(self, "theme_btn"):
            self.theme_btn.setIcon(self.theme_icon_dark if self.is_dark else self.theme_icon_light)

        if self.is_dark:
            palette = {
                "window_bg": "#111827",
                "text": "#e2e8f0",
                "header_bg": "#0b1220",
                "body_bg": "#111827",
                "status_bg": "#0b1220",
                "status_border": "#1f2937",
                "nav_bg": "#1e293b",
                "button_bg": "#111827",
                "button_border": "#334155",
                "button_hover": "#1f2937",
                "tab_bar_bg": "#0b1220",
                "tab_bg": "#1f2937",
                "tab_selected": "#111827",
                "subtle": "#e2e8f0",
                "menu_bg": "#0f172a",
                "menu_border": "#334155",
                "menu_hover": "#1f2937",
                "input_bg": "#1f2937",
                "input_border": "#475569",
                "input_focus": "#3b82f6",
            }
        else:
            palette = {
                "window_bg": "#f5f7fb",
                "text": "#1f2937",
                "header_bg": "#e8f1ff",
                "body_bg": "#ffffff",
                "status_bg": "#f1f5f9",
                "status_border": "#cbd5e1",
                "nav_bg": "#e8f1ff",
                "button_bg": "#ffffff",
                "button_border": "#d5dbe7",
                "button_hover": "#eef2ff",
                "tab_bar_bg": "#e2e8f0",
                "tab_bg": "#e9edf7",
                "tab_selected": "#ffffff",
                "subtle": "#1f2937",
                "menu_bg": "#ffffff",
                "menu_border": "#d5dbe7",
                "menu_hover": "#eef2ff",
                "input_bg": "#ffffff",
                "input_border": "#cbd5e1",
                "input_focus": "#3b82f6",
            }

        self.setStyleSheet(
            f"""
            QWidget {{
                font-family: "Segoe UI";
                font-size: 12pt;
                color: {palette['text']};
                background: transparent;
                border: none;
            }}
            QMainWindow {{
                background: {palette['window_bg']};
            }}
            QWidget#Header {{
                background: {palette['header_bg']};
                border-radius: 12px;
            }}
            QWidget#Body {{
                background: {palette['body_bg']};
            }}
            QWidget#StatusBar {{
                background: {palette['status_bg']};
                border-top: 1px solid {palette['status_border']};
            }}
            QWidget#NavBar {{
                background: {palette['nav_bg']};
            }}
            QTabWidget {{
                border: 0px;
                background: {palette['tab_bar_bg']};
            }}
            QToolButton, QPushButton {{
                font-size: 12pt;
                background: {palette['button_bg']};
                border: 1px solid {palette['button_border']};
                border-radius: 8px;
                padding: 3px 20px;
                min-height: 16px;
            }}
            QToolButton#NavButton {{
                background: transparent;
                border: none;
                padding: 4px;
            }}
            QToolButton#NavButton::menu-indicator {{
                image: none;
                width: 0px;
            }}
            QToolButton#NavButton:hover {{
                background: {palette['button_hover']};
                border-radius: 8px;
            }}
            QToolButton:hover, QPushButton:hover {{
                border-color: {palette['button_border']};
                background: {palette['button_hover']};
            }}
            QLineEdit, QSpinBox, QPlainTextEdit {{
                background: {palette['input_bg']};
                border: 2px solid {palette['input_border']};
                border-radius: 6px;
                padding: 6px 10px;
                selection-background-color: {palette['input_focus']};
            }}
            QLineEdit:focus, QSpinBox:focus, QPlainTextEdit:focus {{
                border: 2px solid {palette['input_focus']};
            }}
            QSpinBox::up-button, QSpinBox::down-button {{
                background: {palette['button_bg']};
                border: 1px solid {palette['input_border']};
                border-radius: 3px;
                width: 16px;
            }}
            QSpinBox::up-button:hover, QSpinBox::down-button:hover {{
                background: {palette['button_hover']};
            }}
            QCheckBox {{
                spacing: 8px;
            }}
            QCheckBox::indicator {{
                width: 18px;
                height: 18px;
                border: 2px solid {palette['input_border']};
                border-radius: 4px;
                background: {palette['input_bg']};
            }}
            QCheckBox::indicator:checked {{
                background: {palette['input_focus']};
                border: 2px solid {palette['input_focus']};
            }}
            QCheckBox::indicator:hover {{
                border: 2px solid {palette['input_focus']};
            }}
            QTabWidget::pane {{
                border: 0px;
                border-radius: 12px;
                background: {palette['tab_selected']};
                top: 0px;
            }}
            QTabWidget::tab-bar {{
                left: 0px;
            }}
            QTabBar {{
                background: {palette['tab_bar_bg']};
                border: 0px;
                padding: 4px;
            }}
            QTabBar::base {{
                border: 0px;
                background: {palette['tab_bar_bg']};
            }}
            QTabBar::tab {{
                background: {palette['tab_bg']};
                border: 0px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
                margin-top: 0px;
                padding: 8px 16px;
                margin-right: 4px;
            }}
            QTabBar::tab:selected {{
                background: {palette['tab_selected']};
                margin-top: 0px;
            }}
            QLabel#Title {{
                font-size: 18pt;
                font-weight: 600;
            }}
            QLabel#Subtle {{
                color: {palette['subtle']};
            }}
            QLabel#StatusDot[status="green"] {{
                border-radius: 6px;
                background: #16a34a;
            }}
            QLabel#StatusDot[status="red"] {{
                border-radius: 6px;
                background: #ef4444;
            }}
            QMenu {{
                background: {palette['menu_bg']};
                border: 1px solid {palette['menu_border']};
                border-radius: 8px;
                padding: 6px;
            }}
            QMenu::item {{
                padding: 6px 16px;
                border-radius: 6px;
            }}
            QMenu::item:selected {{
                background: {palette['menu_hover']};
            }}
            """
        )

    # def closeEvent(self, event):
    #     # Close button minimizes to tray; keep app alive
    #     event.ignore()
    #     self.hide()
    #     if hasattr(self, "tray") and self.tray is not None:
    #         try:
    #             self.tray.showMessage(
    #                 "Dentascan Service Monitor",
    #                 "App is running in tray.",
    #                 QtWidgets.QSystemTrayIcon.Information,
    #                 2000,
    #             )
    #         except Exception:
    #             pass

    def closeEvent(self, event):
        # Close button exits the application normally
        try:
            # Clean up tray icon if present
            if hasattr(self, "tray") and self.tray is not None:
                self.tray.hide()
        except Exception:
            pass
        try:
            # Clean up single-instance server if present
            if hasattr(self, "_server") and self._server is not None:
                self._server.close()
                try:
                    QtNetwork.QLocalServer.removeServer(UI_SERVER_NAME)
                except Exception:
                    pass
        except Exception:
            pass
        # Accept the close and quit the app
        event.accept()
        QtWidgets.QApplication.instance().quit()
            
    def _refresh_log(self):
        # Respect mode: skip file tail when internal UI log mode is active
        if not getattr(self, "read_file_log", False):
            return
        if not os.path.exists(self.log_path):
            self.log_view.setPlainText("Log file not found.")
            return

        try:
            mtime = os.path.getmtime(self.log_path)
            size = os.path.getsize(self.log_path)
            if self._log_last_mtime == mtime and self._log_last_size == size:
                return
            self._log_last_mtime = mtime
            self._log_last_size = size

            with open(self.log_path, "r", encoding="utf-8", errors="ignore") as fh:
                lines = fh.read().splitlines()

            if self._log_max_lines and len(lines) > self._log_max_lines:
                lines = lines[-self._log_max_lines:]

            # Log after lines are prepared

            lines.reverse()
            self.log_view.setPlainText("\n".join(lines))
            cursor = self.log_view.textCursor()
            cursor.movePosition(QtGui.QTextCursor.Start)
            self.log_view.setTextCursor(cursor)
        except Exception as err:
            self.log_view.setPlainText(f"Failed to read log: {err}")


def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)

    socket = QtNetwork.QLocalSocket()
    socket.connectToServer(UI_SERVER_NAME)
    if socket.waitForConnected(200):
        try:
            if "--show" in sys.argv:
                socket.write(b"SHOW")
                socket.flush()
                socket.waitForBytesWritten(200)
            else:
                if _prompt_close_other_instances():
                    _kill_other_service_ui_instances()
                    QtCore.QThread.msleep(400)
                else:
                    _show_already_running_alert()
                    sys.exit(0)
        except Exception as e:
            _ui_log("Error forwarding to instance:", e)
        finally:
            socket.disconnectFromServer()
        if "--show" in sys.argv:
            sys.exit(0)

    try:
        QtNetwork.QLocalServer.removeServer(UI_SERVER_NAME)
    except Exception:
        pass
    server = QtNetwork.QLocalServer()
    server.listen(UI_SERVER_NAME)

    window = ServiceMonitorApp()
    window._setup_single_instance_server(server)

    if "--hidden" in sys.argv:
        window.hide()
    else:
        window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
