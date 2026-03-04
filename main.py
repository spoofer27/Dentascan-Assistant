import io
import json
import os
import sys
import threading
import subprocess
import tempfile
from pathlib import Path
from urllib import request
from urllib.error import URLError

# Setup logging for pythonw compatibility
import logging
log_file = Path(__file__).parent / "app_error.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)

def safe_flush():
    """Safe flush that works when stdout is None (pythonw)"""
    try:
        if sys.stdout:
            sys.stdout.flush()
    except (AttributeError, ValueError):
        pass

from PIL import Image
from PyQt6.QtCore import (
    QBuffer, QIODevice, QObject, QRunnable, QThread, QThreadPool, QTimer, Qt, pyqtSignal
)
from PyQt6.QtGui import QFont, QIcon, QImage, QPixmap
from PyQt6.QtWidgets import (
    QApplication,
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

try:
    from qfluentwidgets import Theme, setTheme
except ImportError:
    Theme = None
    setTheme = None

try:
    from service import service_config
except ImportError:
    service_config = None


ROOT_DIR = Path(__file__).resolve().parent
ICON_DIR = ROOT_DIR / "res" / "icons"

if service_config:
    API_HOST = service_config.SERVICE_API_HOST
    API_PORT = service_config.SERVICE_API_PORT
else:
    API_HOST = "127.0.0.1"
    API_PORT = 8085

API_BASE = f"http://{API_HOST}:{API_PORT}"


def image_to_qicon(image: Image.Image) -> QIcon:
    rgba = image.convert("RGBA")
    raw = rgba.tobytes("raw", "RGBA")
    q_image = QImage(raw, rgba.width, rgba.height, QImage.Format.Format_RGBA8888)
    return QIcon(QPixmap.fromImage(q_image.copy()))


class RequestWorkerSignals(QObject):
    finished = pyqtSignal(str, object, str)


class StatusRequestWorker(QThread):
    finished = pyqtSignal(str, object, str)

    def __init__(self, method: str, path: str):
        super().__init__()
        self.method = method
        self.path = path
        # print(f"[StatusRequestWorker.__init__] Created worker for {method} {path}", flush=True)

    def run(self):
        # print(f"[StatusRequestWorker.run] ===== START (Thread: {threading.current_thread().name}) =====", flush=True)
        payload = None
        error = None
        try:
            try:
                try:
                    try:
                        # print(f"[StatusRequestWorker.run] Step 1: Building URL...", flush=True)
                        full_url = API_BASE + self.path
                    except Exception as e:
                        print(f"[StatusRequestWorker.run] Exception in URL building: {type(e).__name__}: {e}", flush=True)
                        error = f"URL error: {e}"
                        return
                        
                    try:
                        # print(f"[StatusRequestWorker.run] Step 3: Creating request...", flush=True)
                        req = request.Request(full_url, method=self.method)
                    except Exception as e:
                        print(f"[StatusRequestWorker.run] Exception creating request: {type(e).__name__}: {e}", flush=True)
                        error = f"Request creation error: {e}"
                        return
                    
                    try:
                        # print(f"[StatusRequestWorker.run] Step 5: Opening URL...", flush=True)
                        response = request.urlopen(req, timeout=3)
                    except Exception as e:
                        print(f"[StatusRequestWorker.run] Exception opening URL: {type(e).__name__}: {e}", flush=True)
                        error = f"URL open error: {e}"
                        return
                    
                    try:
                        # print(f"[StatusRequestWorker.run] Step 7: Reading response...", flush=True)
                        raw = response.read().decode("utf-8")
                        response.close()
                    except Exception as e:
                        print(f"[StatusRequestWorker.run] Exception reading response: {type(e).__name__}: {e}", flush=True)
                        error = f"Read error: {e}"
                        return
                    
                    try:
                        # print(f"[StatusRequestWorker.run] Step 9: Parsing JSON...", flush=True)
                        payload = json.loads(raw)
                    except Exception as e:
                        print(f"[StatusRequestWorker.run] Exception parsing JSON: {type(e).__name__}: {e}", flush=True)
                        error = f"JSON parse error: {e}"
                        return                   
                except Exception as e:
                    print(f"[StatusRequestWorker.run] Outer exception (type: {type(e).__name__}): {e}", flush=True)
                    error = f"Outer error: {e}"
            except Exception as e:
                print(f"[StatusRequestWorker.run] VERY outer exception: {e}", flush=True)
                error = f"Very outer: {e}"
        except Exception as e:
            print(f"[StatusRequestWorker.run] OUTERMOST exception: {e}", flush=True)
            error = f"Outermost: {e}"
        finally:
            # print(f"[StatusRequestWorker.run] Finally block: about to emit signal...", flush=True)
            try:
                # print(f"[StatusRequestWorker.run] Emitting with path={self.path}, error={error}", flush=True)
                self.finished.emit(self.path, payload, error)
            except Exception as e:
                print(f"[StatusRequestWorker.run] Exception emitting signal: {type(e).__name__}: {e}", flush=True)
            print(f"[StatusRequestWorker.run] ===== END =====", flush=True)


class CasesRequestWorker(QThread):
    """Worker to fetch cases"""
    finished = pyqtSignal(dict, str)  # (cases_data, error_message)

    def __init__(self):
        super().__init__()
        # print(f"[CasesRequestWorker.__init__] Created cases worker", flush=True)

    def run(self):
        # print(f"[CasesRequestWorker.run] ===== START Fetching Cases =====", flush=True)
        cases_data = None
        error = None
        try:
            req = request.Request(API_BASE + "/api/cases", method="GET")
            with request.urlopen(req, timeout=8) as response:
                raw = response.read().decode("utf-8")
            payload = json.loads(raw)
            cases_data = {
                "today": payload.get("today", []),
                "yesterday": payload.get("yesterday", []),
            }
            print(f"cases - Today: {len(cases_data.get('today', []))} | Yesterday: {len(cases_data.get('yesterday', []))}", flush=True)
            
        except Exception as e:
            error = str(e)
            print(f"[CasesRequestWorker.run] ERROR: {error}", flush=True)
            logger.error(f"Failed to fetch cases: {error}")
            cases_data = {"today": [], "yesterday": []}
        finally:
            # print(f"[CasesRequestWorker.run] ===== END Fetching Cases =====", flush=True)
            self.finished.emit(cases_data, error)


class ServiceLogRequestWorker(QThread):
    finished = pyqtSignal(list, str)

    def run(self):
        lines = []
        error = None
        try:
            req = request.Request(API_BASE + "/api/service-log?limit=400", method="GET")
            with request.urlopen(req, timeout=5) as response:
                raw = response.read().decode("utf-8")
            payload = json.loads(raw)
            lines = payload.get("lines") or []
            if not isinstance(lines, list):
                lines = []
        except Exception as exc:
            error = str(exc)
        finally:
            self.finished.emit(lines, error)


def pil_process_icon(icon_path: Path, size: int = 20) -> QIcon:
    if not icon_path.exists():
        return QIcon()

    ext = icon_path.suffix.lower()

    try:
        if ext in {".png", ".jpg", ".jpeg", ".bmp", ".webp"}:
            image = Image.open(icon_path)
            image = image.resize((size, size), Image.Resampling.LANCZOS)
            return image_to_qicon(image)

        if ext == ".svg":
            raster = QIcon(str(icon_path)).pixmap(size, size).toImage()
            if raster.isNull():
                return QIcon(str(icon_path))

            output_buffer = QBuffer()
            output_buffer.open(QIODevice.OpenModeFlag.ReadWrite)
            QPixmap.fromImage(raster).save(output_buffer, "PNG")
            output = io.BytesIO(bytes(output_buffer.data()))
            output.seek(0)

            image = Image.open(output)
            image = image.resize((size, size), Image.Resampling.LANCZOS)
            return image_to_qicon(image)

        return QIcon(str(icon_path))
    except Exception:
        return QIcon(str(icon_path))


class NavButton(QPushButton):
    def __init__(self, text: str, icon: QIcon | None = None, active: bool = False):
        super().__init__(text)
        if icon:
            self.setIcon(icon)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setFixedHeight(44)
        self.set_active(active)

    def set_active(self, active: bool):
        if active:
            self.setStyleSheet(
                """
                QPushButton {
                    background-color: #1D4ED8;
                    color: #E5E7EB;
                    border: none;
                    border-radius: 10px;
                    text-align: left;
                    padding-left: 14px;
                    font-size: 14px;
                    font-weight: 600;
                }
                """
            )
        else:
            self.setStyleSheet(
                """
                QPushButton {
                    background-color: transparent;
                    color: #D1D5DB;
                    border: none;
                    border-radius: 10px;
                    text-align: left;
                    padding-left: 14px;
                    font-size: 14px;
                    font-weight: 500;
                }
                QPushButton:hover {
                    background-color: #1F2937;
                }
                """
            )


class StatusIndicator(QFrame):
    def __init__(self, label: str, online: bool = True):
        super().__init__()
        self.setObjectName("StatusIndicator")
        self.setStyleSheet("#StatusIndicator { background-color: #111827; border-radius: 8px; }")

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 5, 10, 5)
        layout.setSpacing(6)

        self.dot = QLabel()
        self.dot.setFixedSize(8, 8)
        color = "#22C55E" if online else "#EF4444"
        self.dot.setStyleSheet(f"background-color: {color}; border-radius: 4px;")
        layout.addWidget(self.dot)

        self.label = QLabel(label)
        self.label.setStyleSheet("color: #D1D5DB; font-size: 12px; font-weight: 600;")
        layout.addWidget(self.label)

        self.online = online

    def set_online(self, online: bool):
        self.online = online
        color = "#22C55E" if online else "#EF4444"
        self.dot.setStyleSheet(f"background-color: {color}; border-radius: 4px;")


class CaseRow(QFrame):
    def __init__(self, case_data: dict):
        super().__init__()
        self.setFixedHeight(50)  # Fixed height prevents expansion when added to layout
        self.setStyleSheet("QFrame { background-color: #111827; border-radius: 10px; border: none; }")

        row = QHBoxLayout(self)
        row.setContentsMargins(12, 10, 12, 10)
        row.setSpacing(12)
        # print(f"case_data: {case_data}", flush=True)

        self._add_text(row, case_data["id"], 88)
        self._add_text(row, case_data["name"], 130)
        self._add_text(row, case_data["scan_type"], 100)
        self._add_contact_cell(row, case_data.get("phone_values", []), "phone", 260)
        self._add_contact_cell(row, case_data.get("email_values", []), "email", 260)
        self._add_text(row, case_data.get("pacs_text", "No"), 150)
        # row.addWidget(self._status_dot(case_data["staged"], 1))
        # row.addWidget(self._status_dot(case_data["pacs"], case_data["pacs_text"]), 1)

        action_button = QPushButton("View" if case_data["action"] == "view" else "Retry")
        action_button.setCursor(Qt.CursorShape.PointingHandCursor)
        action_button.setFixedHeight(30)
        action_button.setFixedWidth(72)

        if case_data["action"] == "view":
            action_button.setStyleSheet(
                """
                QPushButton {
                    background-color: #2563EB;
                    color: #F9FAFB;
                    border: none;
                    border-radius: 8px;
                    font-size: 12px;
                    font-weight: 600;
                }
                QPushButton:hover { background-color: #1D4ED8; }
                """
            )
        else:
            action_button.setStyleSheet(
                """
                QPushButton {
                    background-color: rgba(239, 68, 68, 0.15);
                    color: #FCA5A5;
                    border: none;
                    border-radius: 8px;
                    font-size: 12px;
                    font-weight: 600;
                }
                QPushButton:hover { background-color: rgba(239, 68, 68, 0.24); }
                """
            )

        row.addWidget(action_button)

    def _add_text(self, parent: QHBoxLayout, text: str, width: int):
        label = QLabel(str(text))
        label.setFixedWidth(width)
        label.setStyleSheet("color: #E5E7EB; font-size: 12px;")
        label.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse
            | Qt.TextInteractionFlag.TextSelectableByKeyboard
        )
        parent.addWidget(label)

    def _add_contact_cell(self, parent: QHBoxLayout, values: list, contact_type: str, width: int):
        wrapper = QWidget()
        wrapper.setFixedWidth(width)
        layout = QHBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        combo = QComboBox()
        combo.setEditable(True)
        combo.setFixedHeight(30)
        combo.setMinimumWidth(width - 84)
        combo.setStyleSheet(
            """
            QComboBox {
                background-color: #1F2937;
                border: 1px solid #374151;
                border-radius: 6px;
                color: #E5E7EB;
                padding: 2px 6px 2px 6px;
                font-size: 11px;
            }
            QComboBox::drop-down {
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 18px;
                border-left: 1px solid #374151;
            }
            """
        )

        unique_values = []
        for value in values or []:
            text = str(value).strip() if value is not None else ""
            if text and text not in unique_values:
                unique_values.append(text)

        if unique_values:
            combo.addItems(unique_values)
        else:
            combo.addItem("No saved values")

        combo.lineEdit().setPlaceholderText("Add phone..." if contact_type == "phone" else "Add email...")

        send_btn = QPushButton("send")
        send_btn.setFixedSize(44, 26)
        send_btn.setStyleSheet(
            """
            QPushButton {
                background-color: #2563EB;
                color: #F9FAFB;
                border: none;
                border-radius: 6px;
                font-size: 10px;
                font-weight: 600;
            }
            QPushButton:hover { background-color: #1D4ED8; }
            """
        )

        send_all_btn = QPushButton("all")
        send_all_btn.setFixedSize(34, 26)
        send_all_btn.setStyleSheet(
            """
            QPushButton {
                background-color: #374151;
                color: #E5E7EB;
                border: none;
                border-radius: 6px;
                font-size: 10px;
                font-weight: 600;
            }
            QPushButton:hover { background-color: #4B5563; }
            """
        )

        def _send_current():
            value = combo.currentText().strip()
            if value:
                print(f"[CaseRow] send {contact_type}: {value}", flush=True)

        def _send_all():
            values_to_send = []
            for i in range(combo.count()):
                item = combo.itemText(i).strip()
                if item and item not in values_to_send:
                    values_to_send.append(item)
            typed = combo.currentText().strip()
            if typed and typed not in values_to_send:
                values_to_send.append(typed)
            if values_to_send:
                print(f"[CaseRow] send all {contact_type}: {', '.join(values_to_send)}", flush=True)

        send_btn.clicked.connect(_send_current)
        send_all_btn.clicked.connect(_send_all)

        layout.addWidget(combo, 1)
        layout.addWidget(send_btn)
        layout.addWidget(send_all_btn)
        parent.addWidget(wrapper)

    def _status_dot(self, status: str, text: str) -> QWidget:
        colors = {
            "processing": "#3B82F6",
            "completed": "#22C55E",
            "error": "#EF4444",
        }
        wrapper = QWidget()
        layout = QHBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        dot = QLabel()
        dot.setFixedSize(10, 10)
        dot.setStyleSheet(f"background-color: {colors.get(status, '#9CA3AF')}; border-radius: 5px;")

        value = QLabel(text)
        value.setStyleSheet("color: #D1D5DB; font-size: 12px; font-weight: 600;")

        layout.addWidget(dot)
        layout.addWidget(value)
        layout.addStretch()
        return wrapper


class DashboardWindow(QMainWindow):
    def __init__(self):
        super().__init__()        
        self.setWindowTitle("Dentascan Case Monitor")
        self.resize(1460, 900)
        
        root = QWidget()
        root.setStyleSheet("background-color: #0F172A;")
        self.setCentralWidget(root)

        main_layout = QHBoxLayout(root)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Initialize member variables early (before build methods that set these)
        self.status_indicators = {}
        self.service_indicator = None
        self.ris_indicator = None
        self.pacs_indicator = None
        self.current_worker = None  # Keep reference to prevent GC
        self.cases_worker = None  # Keep reference to cases worker
        self.service_log_worker = None
        self.today_cases_layout = None  # Will hold today's cases
        self.yesterday_cases_layout = None  # Will hold yesterday's cases
        self.today_case_widgets = {}
        self.yesterday_case_widgets = {}
        self._service_log_last_lines = []
        self.thread_pool = QThreadPool.globalInstance()

        main_layout.addWidget(self.build_sidebar())
        
        self.content_stack = QWidget()
        self.content_layout = QVBoxLayout(self.content_stack)
        self.content_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(self.content_stack, 1)
        
        self.monitor_widget = self.build_monitor_content()
        
        self.ris_widget = self.build_ris_content()
        
        self.pacs_widget = self.build_pacs_content()
        
        self.content_layout.addWidget(self.monitor_widget)
        self.content_layout.addWidget(self.ris_widget)
        self.content_layout.addWidget(self.pacs_widget)
        
        self.poll_timer = QTimer(self)
        self.poll_timer.timeout.connect(self._poll_status, Qt.ConnectionType.QueuedConnection)
        self.poll_timer.setSingleShot(False)
        self.poll_timer.setInterval(2000)
        
        # print("[DashboardWindow] Creating cases poll timer...")
        self.cases_timer = QTimer(self)
        self.cases_timer.timeout.connect(self._poll_cases, Qt.ConnectionType.QueuedConnection)
        self.cases_timer.setSingleShot(False)
        self.cases_timer.setInterval(3000)  # Poll cases every 3 seconds

        self.service_log_timer = QTimer(self)
        self.service_log_timer.timeout.connect(self._poll_service_log, Qt.ConnectionType.QueuedConnection)
        self.service_log_timer.setSingleShot(False)
        self.service_log_timer.setInterval(2000)
        
        # print("[DashboardWindow] Scheduling delayed timer start...")
        QTimer.singleShot(500, self.poll_timer.start)
        QTimer.singleShot(1000, self.cases_timer.start)  # Start cases polling slightly delayed
        QTimer.singleShot(1200, self.service_log_timer.start)
        
        self.show_monitor()

    def build_sidebar(self) -> QWidget:
        # print("[DashboardWindow.build_sidebar] Building sidebar...")
        sidebar = QFrame()
        sidebar.setFixedWidth(250)
        sidebar.setStyleSheet("background-color: #111827; border-right: 1px solid #1F2937;")

        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(14, 18, 14, 14)
        layout.setSpacing(8)

        top = QHBoxLayout()
        logo = QLabel()
        logo.setFixedSize(42, 42)
        logo.setStyleSheet("background-color: #2563EB; border-radius: 8px;")
        logo.setAlignment(Qt.AlignmentFlag.AlignCenter)
        logo.setText("D")
        logo.setFont(QFont("Segoe UI", 16, QFont.Weight.Bold))
        logo.setStyleSheet("background-color: #2563EB; border-radius: 8px; color: white;")

        title_box = QVBoxLayout()
        app_title = QLabel("Dentascan")
        app_title.setStyleSheet("color: #F9FAFB; font-size: 16px; font-weight: 700;")
        sub = QLabel("Radiology Center")
        sub.setStyleSheet("color: #9CA3AF; font-size: 11px;")
        title_box.addWidget(app_title)
        title_box.addWidget(sub)

        top.addWidget(logo)
        top.addLayout(title_box)
        top.addStretch()
        layout.addLayout(top)
        layout.addSpacing(14)

        home_icon = pil_process_icon(ICON_DIR / "home.svg", 18)
        settings_icon = pil_process_icon(ICON_DIR / "settings.svg", 18)

        self.nav_buttons = []
        nav_data = [
            ("Monitor", home_icon, True),
            ("RIS", None, False),
            ("PACS", None, False),
            ("Settings", settings_icon, False),
        ]

        for name, icon, is_active in nav_data:
            button = NavButton(name, icon=icon, active=is_active)
            button.clicked.connect(lambda checked, b=name: self.activate_nav(b))
            self.nav_buttons.append(button)
            layout.addWidget(button)

        layout.addStretch(1)

        profile = QFrame()
        profile.setStyleSheet("QFrame { background-color: #1F2937; border-radius: 12px; }")
        profile_layout = QHBoxLayout(profile)
        profile_layout.setContentsMargins(10, 10, 10, 10)
        profile_layout.setSpacing(10)

        avatar = QLabel("DR")
        avatar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        avatar.setFixedSize(40, 40)
        avatar.setStyleSheet("background-color: #374151; color: #E5E7EB; border-radius: 20px; font-weight: 700;")

        profile_text = QVBoxLayout()
        name = QLabel("Dr. Aris Thorne")
        role = QLabel("Administrator")
        name.setStyleSheet("color: #F3F4F6; font-size: 12px; font-weight: 700;")
        role.setStyleSheet("color: #9CA3AF; font-size: 11px;")
        profile_text.addWidget(name)
        profile_text.addWidget(role)

        profile_layout.addWidget(avatar)
        profile_layout.addLayout(profile_text)
        layout.addWidget(profile)

        # print("[DashboardWindow.build_sidebar] Sidebar built successfully")
        return sidebar

    def build_monitor_content(self) -> QWidget:
        # print("[DashboardWindow.build_monitor_content] Building monitor content...")
        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(18, 16, 18, 16)
        content_layout.setSpacing(14)

        header = QFrame()
        header.setFixedHeight(72)
        header.setStyleSheet("QFrame { background-color: #111827; border-radius: 12px; border: none; }")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(14, 10, 14, 10)

        search = QLineEdit()
        search.setPlaceholderText("Search patients or IDs...")
        search.setFixedWidth(300)
        search.setStyleSheet(
            """
            QLineEdit {
                background-color: #1F2937;
                border: 1px solid #374151;
                border-radius: 8px;
                color: #E5E7EB;
                font-size: 13px;
                padding: 8px 10px;
            }
            QLineEdit:focus { border: 1px solid #2563EB; }
            """
        )

        status_container = QHBoxLayout()
        status_container.setSpacing(6)
        self.service_indicator = StatusIndicator("Service", True)
        self.ris_indicator = StatusIndicator("RIS", True)
        self.pacs_indicator = StatusIndicator("PACS", True)
        # print(f"[build_monitor_content] Indicators created - service: {self.service_indicator}, ris: {self.ris_indicator}, pacs: {self.pacs_indicator}")
        status_container.addWidget(self.service_indicator)
        status_container.addWidget(self.ris_indicator)
        status_container.addWidget(self.pacs_indicator)
        status_container.addStretch()

        left_header = QHBoxLayout()
        left_header.addWidget(search)

        header_layout.addLayout(left_header)
        header_layout.addStretch()
        header_layout.addLayout(status_container)

        content_layout.addWidget(header)

        section_title = QLabel("Today's Cases")
        section_title.setStyleSheet("color: #E5E7EB; font-size: 16px; font-weight: 700;")
        content_layout.addWidget(section_title)

        header_row = self.case_header()
        content_layout.addWidget(header_row)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet(
            """
            QScrollArea {
                border: none;
                background-color: transparent;
            }
            QScrollBar:vertical {
                background: #111827;
                width: 10px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical {
                background: #374151;
                min-height: 20px;
                border-radius: 5px;
            }
            """
        )

        rows_host = QWidget()
        rows_layout = QVBoxLayout(rows_host)
        rows_layout.setContentsMargins(0, 0, 0, 0)
        rows_layout.setSpacing(8)
        self.today_cases_layout = rows_layout  # Store for dynamic updates

        rows = []

        for case in rows:
            rows_layout.addWidget(CaseRow(case))

        rows_layout.addStretch(1)
        scroll.setWidget(rows_host)
        scroll.setMinimumHeight(240)
        content_layout.addWidget(scroll)

        yesterday_title = QLabel("Yesterday's Cases")
        yesterday_title.setStyleSheet("color: #9CA3AF; font-size: 16px; font-weight: 700;")
        content_layout.addWidget(yesterday_title)

        yesterday_header_row = self.case_header()
        content_layout.addWidget(yesterday_header_row)

        yesterday_scroll = QScrollArea()
        yesterday_scroll.setWidgetResizable(True)
        yesterday_scroll.setStyleSheet(
            """
            QScrollArea {
                border: none;
                background-color: transparent;
            }
            QScrollBar:vertical {
                background: #111827;
                width: 10px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical {
                background: #374151;
                min-height: 20px;
                border-radius: 5px;
            }
            """
        )

        yesterday_host = QWidget()
        yesterday_layout = QVBoxLayout(yesterday_host)
        yesterday_layout.setContentsMargins(0, 0, 0, 0)
        yesterday_layout.setSpacing(8)
        self.yesterday_cases_layout = yesterday_layout  # Store for dynamic updates

        yesterday_rows = []

        for case in yesterday_rows:
            yesterday_layout.addWidget(CaseRow(case))

        yesterday_layout.addStretch(1)
        yesterday_scroll.setWidget(yesterday_host)
        yesterday_scroll.setMinimumHeight(170)
        content_layout.addWidget(yesterday_scroll, 1)

        return content

    def case_header(self) -> QWidget:
        frame = QFrame()
        frame.setStyleSheet("QFrame { background-color: #1F2937; border-radius: 10px; border: none; }")
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(12)

        labels = [
            ("ID", 88),
            ("Name", 130),
            ("Scan Type", 100),
            ("Phones", 260),
            ("E-mails", 260),
            ("PACs Uploaded", 150),
            ("Action", 70),
        ]

        for text, width in labels:
            item = QLabel(text)
            item.setFixedWidth(width)
            item.setStyleSheet("color: #9CA3AF; font-size: 11px; font-weight: 700;")
            layout.addWidget(item)

        return frame

    def _poll_status(self):
        try:
            self.current_worker = StatusRequestWorker("GET", "/api/status")
            safe_flush()
            self.current_worker.finished.connect(self._handle_status_response, Qt.ConnectionType.QueuedConnection)
            # print("[DashboardWindow._poll_status] Signal connected, starting worker thread...", flush=True)
            self.current_worker.start()
        except Exception as e:
            print(f"[DashboardWindow._poll_status] Exception in status polling: {e}", flush=True)
            import traceback
            traceback.print_exc()
            safe_flush()
      
    def _handle_status_response(self, path: str, payload: dict, error: str):
        # print(f"[_handle_status_response] Processing response - error={error}, payload exists={payload is not None}", flush=True)
        if error or not payload:
            print(f"[_handle_status_response] Error state - setting all indicators offline", flush=True)
            if self.service_indicator:
                self.service_indicator.set_online(False)
            if self.ris_indicator:
                self.ris_indicator.set_online(False)
            if self.pacs_indicator:
                self.pacs_indicator.set_online(False)
            return
        
        try:
            service_state = payload.get("state", "").lower()
            service_online = service_state == "running"
            ris_online = bool(payload.get("ris_online", False))

            # Update indicators based on API response
            if self.service_indicator:
                self.service_indicator.set_online(service_online)
            else:
                print(f"[_handle_status_response] [WARN] service_indicator is None!", flush=True)
                
            if self.ris_indicator:
                self.ris_indicator.set_online(ris_online)
            if self.pacs_indicator:
                self.pacs_indicator.set_online(True)  # Assume PACS is online if we got a response
        except Exception as e:
            print(f"[_handle_status_response] Exception: {e}", flush=True)
    
    def _format_pacs_progress(self, progress: dict) -> str:
        """Format PACS upload progress for display"""
        total = progress.get("total_files", 0)
        uploaded = progress.get("uploaded_files", 0)
        percent = progress.get("current_percent", 0)
        is_uploading = progress.get("is_uploading", False)
        is_complete = progress.get("is_complete", False)
        
        if total == 0:
            return "No files"
        
        if is_complete:
            return f"{total}/{total} - 100%"
        
        if is_uploading:
            # During active upload, show progress percentage
            return f"Uploading... {percent}%"
        
        # Not uploading - show uploaded/total count
        if uploaded > 0 or total > 0:
            return f"{uploaded}/{total}"
        
        return "Not uploaded"
    
    def _poll_cases(self):
        try:
            if self.cases_worker is not None:
                # print("[DashboardWindow._poll_cases] Previous worker still running, skipping", flush=True)
                return
            
            # print("[DashboardWindow._poll_cases] Creating CasesRequestWorker...", flush=True)
            self.cases_worker = CasesRequestWorker()
            # print("[DashboardWindow._poll_cases] Worker created, connecting signal...", flush=True)
            self.cases_worker.finished.connect(self._handle_cases_response, Qt.ConnectionType.QueuedConnection)
            # print("[DashboardWindow._poll_cases] Signal connected, starting worker...", flush=True)
            self.cases_worker.start()
            # print("[DashboardWindow._poll_cases] Worker thread started", flush=True)
        except Exception as e:
            print(f"[DashboardWindow._poll_cases] Exception: {e}", flush=True)
            logger.error(f"Error polling cases: {e}")

    def _poll_service_log(self):
        try:
            if self.service_log_worker is not None:
                return
            self.service_log_worker = ServiceLogRequestWorker()
            self.service_log_worker.finished.connect(self._handle_service_log_response, Qt.ConnectionType.QueuedConnection)
            self.service_log_worker.start()
        except Exception as e:
            print(f"[DashboardWindow._poll_service_log] Exception: {e}", flush=True)

    def _compute_new_service_log_lines(self, latest_lines: list) -> list:
        previous = list(self._service_log_last_lines or [])
        current = list(latest_lines or [])

        if not previous:
            return current

        max_overlap = min(len(previous), len(current))
        for overlap in range(max_overlap, 0, -1):
            if previous[-overlap:] == current[:overlap]:
                return current[overlap:]

        return current

    def _handle_service_log_response(self, lines: list, error: str):
        try:
            if error:
                return
            new_lines = self._compute_new_service_log_lines(lines)
            for line in new_lines:
                text = str(line).rstrip()
                if text:
                    print(text, flush=True)
            self._service_log_last_lines = list(lines or [])
        finally:
            self.service_log_worker = None

    def _build_case_row_payload(self, case_data: dict, fallback_scan_type: str = "Unknown") -> dict:
        return {
            "id": case_data.get("case_id", "Unknown") if case_data.get("case_id") is not None else "Unknown",
            "name": case_data.get("name", "Unknown"),
            "scan_type": case_data.get("exam", fallback_scan_type),
            "phone_values": [
                case_data.get("pt_mobile_value"),
                case_data.get("pt_phone_value"),
                case_data.get("ref_mobile_value"),
                case_data.get("ref_phone_value"),
            ],
            "email_values": [
                case_data.get("pt_email_value"),
                case_data.get("ref_email_value"),
            ],
            "pacs": case_data.get("is_uploaded", False),
            "action": "view",
        }

    def _case_row_key(self, case_data: dict) -> str:
        return "|".join([
            str(case_data.get("case_id") or "Unknown"),
            str(case_data.get("name") or "Unknown"),
            str(case_data.get("date") or ""),
            str(case_data.get("time") or ""),
        ])

    def _upsert_case_rows(self, layout: QVBoxLayout, widget_map: dict, cases: list, fallback_scan_type: str):
        target_order = []

        for index, case_data in enumerate(cases):
            row_key = self._case_row_key(case_data)
            row_payload = self._build_case_row_payload(case_data, fallback_scan_type=fallback_scan_type)
            row_signature = json.dumps(row_payload, sort_keys=True, ensure_ascii=False)
            target_order.append(row_key)

            existing = widget_map.get(row_key)
            if existing and existing.get("signature") == row_signature:
                widget = existing.get("widget")
                current_index = layout.indexOf(widget)
                if widget is not None and current_index != index and current_index != -1:
                    layout.removeWidget(widget)
                    layout.insertWidget(index, widget)
                continue

            new_widget = CaseRow(row_payload)
            if existing:
                old_widget = existing.get("widget")
                if old_widget is not None:
                    layout.removeWidget(old_widget)
                    old_widget.deleteLater()
            layout.insertWidget(index, new_widget)
            widget_map[row_key] = {
                "widget": new_widget,
                "signature": row_signature,
            }

        for old_key in list(widget_map.keys()):
            if old_key not in target_order:
                old_widget = widget_map[old_key].get("widget")
                if old_widget is not None:
                    layout.removeWidget(old_widget)
                    old_widget.deleteLater()
                widget_map.pop(old_key, None)

    def _is_contact_dropdown_open(self) -> bool:
        try:
            combos = self.findChildren(QComboBox)
            return any(combo.view().isVisible() for combo in combos)
        except Exception:
            return False
    
    def _handle_cases_response(self, cases_data: dict, error: str):
        """Handle cases response and update the UI"""        
        try:
            if error:
                print(f"[_handle_cases_response] Error: {error}", flush=True)
                return
            
            if not cases_data:
                print(f"[_handle_cases_response] No cases data", flush=True)
                return

            if self._is_contact_dropdown_open():
                return
            
            # Populate today's cases
            today_cases = cases_data.get("today", [])
            print(f"Found {len(today_cases)} cases today", flush=True)
            self._upsert_case_rows(
                layout=self.today_cases_layout,
                widget_map=self.today_case_widgets,
                cases=today_cases,
                fallback_scan_type="Unknown",
            )
            
            # Populate yesterday's cases
            yesterday_cases = cases_data.get("yesterday", [])
            print(f"Found {len(yesterday_cases)} cases yesterday", flush=True)
            self._upsert_case_rows(
                layout=self.yesterday_cases_layout,
                widget_map=self.yesterday_case_widgets,
                cases=yesterday_cases,
                fallback_scan_type="CBCT",
            )
                        
        except Exception as e:
            print(f"[_handle_cases_response] Exception: {e}", flush=True)
            logger.error(f"Error handling cases response: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cases_worker = None
    
    def activate_nav(self, nav_name: str):
        # print(f"[DashboardWindow.activate_nav] Navigation requested: {nav_name}")
        for button in self.nav_buttons:
            button.set_active(button.text() == nav_name)
        
        if nav_name == "Monitor":
            # print(f"[DashboardWindow.activate_nav] Showing Monitor")
            self.show_monitor()
        elif nav_name == "RIS":
            # print(f"[DashboardWindow.activate_nav] Showing RIS")
            self.show_ris()
        elif nav_name == "PACS":
            # print(f"[DashboardWindow.activate_nav] Showing PACS")
            self.show_pacs()
    
    def show_monitor(self):
        # print("[show_monitor] START")
        safe_flush()
        try:
            # print("[show_monitor] Showing monitor widget...")
            safe_flush()
            self.monitor_widget.show()
            # print("[show_monitor] Monitor widget shown")
            safe_flush()
            
            # print("[show_monitor] Hiding RIS widget...")
            safe_flush()
            self.ris_widget.hide()
            # print("[show_monitor] RIS widget hidden")
            safe_flush()
            
            # print("[show_monitor] Hiding PACS widget...")
            safe_flush()
            self.pacs_widget.hide()
            # print("[show_monitor] PACS widget hidden")
            safe_flush()
        except Exception as e:
            print(f"[show_monitor] Exception: {type(e).__name__}: {e}")
            safe_flush()
            import traceback
            traceback.print_exc()
            safe_flush()
        # print("[show_monitor] END")
        safe_flush()
    
    def show_ris(self):
        print("[show_ris] START")
        safe_flush()
        try:
            self.monitor_widget.hide()
            self.ris_widget.show()
            self.pacs_widget.hide()
        except Exception as e:
            print(f"[show_ris] Exception: {e}")
            safe_flush()
        print("[show_ris] END")
        safe_flush()
    
    def show_pacs(self):
        print("[show_pacs] START")
        safe_flush()
        try:
            self.monitor_widget.hide()
            self.ris_widget.hide()
            self.pacs_widget.show()
        except Exception as e:
            print(f"[show_pacs] Exception: {e}")
            safe_flush()
        print("[show_pacs] END")
        safe_flush()
    
    def build_ris_content(self) -> QWidget:
        print("[DashboardWindow.build_ris_content] Building RIS content...")
        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(18, 16, 18, 16)
        content_layout.setSpacing(12)

        filter_frame = QFrame()
        filter_frame.setFixedHeight(60)
        filter_frame.setStyleSheet("QFrame { background-color: #111827; border-radius: 12px; border: none; }")
        filter_layout = QHBoxLayout(filter_frame)
        filter_layout.setContentsMargins(14, 10, 14, 10)
        filter_layout.setSpacing(10)

        search = QLineEdit()
        search.setPlaceholderText("Search patient ID, name...")
        search.setFixedWidth(250)
        search.setStyleSheet(
            """
            QLineEdit {
                background-color: #1F2937;
                border: 1px solid #374151;
                border-radius: 8px;
                color: #E5E7EB;
                font-size: 12px;
                padding: 6px 10px;
            }
            QLineEdit:focus { border: 1px solid #2563EB; }
            """
        )
        filter_layout.addWidget(search)

        date_filter = QLineEdit()
        date_filter.setPlaceholderText("Date range...")
        date_filter.setFixedWidth(180)
        date_filter.setStyleSheet(
            """
            QLineEdit {
                background-color: #1F2937;
                border: 1px solid #374151;
                border-radius: 8px;
                color: #E5E7EB;
                font-size: 12px;
                padding: 6px 10px;
            }
            QLineEdit:focus { border: 1px solid #2563EB; }
            """
        )
        filter_layout.addWidget(date_filter)

        status_filter = QLineEdit()
        status_filter.setPlaceholderText("Status...")
        status_filter.setFixedWidth(150)
        status_filter.setStyleSheet(
            """
            QLineEdit {
                background-color: #1F2937;
                border: 1px solid #374151;
                border-radius: 8px;
                color: #E5E7EB;
                font-size: 12px;
                padding: 6px 10px;
            }
            QLineEdit:focus { border: 1px solid #2563EB; }
            """
        )
        filter_layout.addWidget(status_filter)
        filter_layout.addStretch()

        content_layout.addWidget(filter_frame)

        header_row = self.case_header()
        content_layout.addWidget(header_row)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet(
            """
            QScrollArea {
                border: none;
                background-color: transparent;
            }
            QScrollBar:vertical {
                background: #111827;
                width: 10px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical {
                background: #374151;
                min-height: 20px;
                border-radius: 5px;
            }
            """
        )

        rows_host = QWidget()
        rows_layout = QVBoxLayout(rows_host)
        rows_layout.setContentsMargins(0, 0, 0, 0)
        rows_layout.setSpacing(8)

        ris_rows = [
            {
                "id": "PX-9001",
                "name": "Ahmed Ali",
                "scan_type": "CBCT",
                "phone_values": [],
                "email_values": [],
                # "staged_pct": "100%",
                "pacs": "completed",
                # "pacs_text": "Completed",
                "action": "view",
            },
            {
                "id": "PX-9002",
                "name": "Fatima Hassan",
                "scan_type": "CT SCAN",
                "phone_values": [],
                "email_values": [],
                # "staged_pct": "45%",
                "pacs": "processing",
                # "pacs_text": "32%",
                "action": "view",
            },
        ]

        for case in ris_rows:
            rows_layout.addWidget(CaseRow(case))

        rows_layout.addStretch(1)
        scroll.setWidget(rows_host)
        content_layout.addWidget(scroll, 1)
        print("[DashboardWindow.build_ris_content] RIS content built successfully")

        return content

    def build_pacs_content(self) -> QWidget:
        print("[DashboardWindow.build_pacs_content] Building PACS content...")
        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(18, 16, 18, 16)
        content_layout.setSpacing(12)

        filter_frame = QFrame()
        filter_frame.setFixedHeight(60)
        filter_frame.setStyleSheet("QFrame { background-color: #111827; border-radius: 12px; border: none; }")
        filter_layout = QHBoxLayout(filter_frame)
        filter_layout.setContentsMargins(14, 10, 14, 10)
        filter_layout.setSpacing(10)

        search = QLineEdit()
        search.setPlaceholderText("Search accession number, patient...")
        search.setFixedWidth(280)
        search.setStyleSheet(
            """
            QLineEdit {
                background-color: #1F2937;
                border: 1px solid #374151;
                border-radius: 8px;
                color: #E5E7EB;
                font-size: 12px;
                padding: 6px 10px;
            }
            QLineEdit:focus { border: 1px solid #2563EB; }
            """
        )
        filter_layout.addWidget(search)

        modality_filter = QLineEdit()
        modality_filter.setPlaceholderText("Modality...")
        modality_filter.setFixedWidth(150)
        modality_filter.setStyleSheet(
            """
            QLineEdit {
                background-color: #1F2937;
                border: 1px solid #374151;
                border-radius: 8px;
                color: #E5E7EB;
                font-size: 12px;
                padding: 6px 10px;
            }
            QLineEdit:focus { border: 1px solid #2563EB; }
            """
        )
        filter_layout.addWidget(modality_filter)

        upload_status = QLineEdit()
        upload_status.setPlaceholderText("Upload status...")
        upload_status.setFixedWidth(160)
        upload_status.setStyleSheet(
            """
            QLineEdit {
                background-color: #1F2937;
                border: 1px solid #374151;
                border-radius: 8px;
                color: #E5E7EB;
                font-size: 12px;
                padding: 6px 10px;
            }
            QLineEdit:focus { border: 1px solid #2563EB; }
            """
        )
        filter_layout.addWidget(upload_status)
        filter_layout.addStretch()

        content_layout.addWidget(filter_frame)

        header_row = self.case_header()
        content_layout.addWidget(header_row)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet(
            """
            QScrollArea {
                border: none;
                background-color: transparent;
            }
            QScrollBar:vertical {
                background: #111827;
                width: 10px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical {
                background: #374151;
                min-height: 20px;
                border-radius: 5px;
            }
            """
        )

        rows_host = QWidget()
        rows_layout = QVBoxLayout(rows_host)
        rows_layout.setContentsMargins(0, 0, 0, 0)
        rows_layout.setSpacing(8)

        pacs_rows = [
            {
                "id": "ACC-5001",
                "name": "Mohammed Karim",
                "scan_type": "CT SCAN",
                "phone_values": [],
                "email_values": [],
                # "staged_pct": "100%",
                "pacs": "completed",
                # "pacs_text": "Completed",
                "action": "view",
            },
            {
                "id": "ACC-5002",
                "name": "Sara Jamal",
                "scan_type": "MRI",
                "phone_values": [],
                "email_values": [],
                # "staged_pct": "100%",
                "pacs": "processing",
                # "pacs_text": "89%",
                "action": "view",
            },
        ]

        for case in pacs_rows:
            rows_layout.addWidget(CaseRow(case))

        rows_layout.addStretch(1)
        scroll.setWidget(rows_host)
        content_layout.addWidget(scroll, 1)
        print("[DashboardWindow.build_pacs_content] PACS content built successfully")

        return content


def main():
    try:
        logger.info("[MAIN] Starting application...")
        app = QApplication(sys.argv)
        app.setFont(QFont("Segoe UI", 10))
        if setTheme and Theme:
            setTheme(Theme.DARK)
        window = DashboardWindow()        
        window.show()
        sys.exit(app.exec())
    except Exception as e:
        import traceback
        logger.error(f"[MAIN] FATAL ERROR: {e}")
        logger.error(traceback.format_exc())
        print(f"FATAL ERROR: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
