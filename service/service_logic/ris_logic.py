from cmath import log
import json
import sys
import tkinter as tk
from tkinter import scrolledtext, messagebox, ttk, filedialog
from urllib import request
from urllib.error import URLError
import threading
import time
from threading import Lock
import os
import re
from urllib.request import Request, urlopen
from urllib.parse import urlsplit
import logging


def _inject_project_venv_site_packages() -> bool:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    service_dir = os.path.dirname(current_dir)
    project_root = os.path.dirname(service_dir)
    candidates = [
        os.path.join(project_root, ".venv", "Lib", "site-packages"),
        os.path.join(project_root, "venv", "Lib", "site-packages"),
    ]

    added = False
    for candidate in candidates:
        if os.path.isdir(candidate) and candidate not in sys.path:
            sys.path.insert(0, candidate)
            added = True
    return added

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
    from webdriver_manager.chrome import ChromeDriverManager
    _selenium_import_error = None
except Exception as exc:
    if _inject_project_venv_site_packages():
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.common.by import By
            from selenium.webdriver.common.action_chains import ActionChains
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
            from webdriver_manager.chrome import ChromeDriverManager
            _selenium_import_error = None
        except Exception as fallback_exc:
            webdriver = None
            Service = None
            By = None
            ActionChains = None
            WebDriverWait = None
            EC = None
            TimeoutException = Exception
            NoSuchElementException = Exception
            ElementClickInterceptedException = Exception
            ChromeDriverManager = None
            _selenium_import_error = fallback_exc
    else:
        webdriver = None
        Service = None
        By = None
        ActionChains = None
        WebDriverWait = None
        EC = None
        TimeoutException = Exception
        NoSuchElementException = Exception
        ElementClickInterceptedException = Exception
        ChromeDriverManager = None
        _selenium_import_error = exc
try:
    import service_config
except ImportError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    service_dir = os.path.dirname(current_dir)
    if service_dir not in sys.path:
        sys.path.insert(0, service_dir)
    import service_config

try:
    from service.unified_logging import get_service_logger
except Exception:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    service_dir = os.path.dirname(current_dir)
    if service_dir not in sys.path:
        sys.path.insert(0, service_dir)
    from unified_logging import get_service_logger

logger = get_service_logger(__name__)


# --- Selenium scraper function ---
# Global driver state used by the three separate actions
driver = None
wait = None
driver_lock = Lock()
_log_throttle_lock = Lock()
_last_log_by_key = {}

def _post_ui_log(message: str, source: str = "RISLogic"):
        logger.info("[%s] %s", source, message)


def _post_ui_log_throttled(key: str, message: str, min_interval_seconds: float = 20.0, source: str = "RISLogic"):
        now = time.time()
        with _log_throttle_lock:
            last_ts = _last_log_by_key.get(key)
            if last_ts is not None and (now - last_ts) < float(min_interval_seconds):
                return
            _last_log_by_key[key] = now
        _post_ui_log(message, source=source)


def _post_ris_status(online: bool):
        host = getattr(service_config, "SERVICE_API_HOST", "127.0.0.1")
        port = int(getattr(service_config, "SERVICE_API_PORT", 8085))
        url = f"http://{host}:{port}/api/ris-status"
        try:
            payload = {
                "online": bool(online),
                "timestamp": time.time(),
            }
            data = json.dumps(payload).encode("utf-8")
            req = request.Request(url, data=data, method="POST")
            req.add_header("Content-Type", "application/json; charset=utf-8")
            with request.urlopen(req, timeout=0.5) as resp:
                resp.read(0)
            _post_ui_log(f"RIS status updated: {'Online' if online else 'Offline'}")
        except URLError as e:
            logger.warning("Failed to update RIS status: %s", e)
            pass
        except Exception as e:
            logger.exception("Unexpected error updating RIS status: %s", e)
            pass


def _ensure_selenium_ready() -> bool:
    if _selenium_import_error is None:
        return True
    _post_ui_log(f"Selenium import failed: {_selenium_import_error}", source="RISLogic")
    _post_ui_log(f"Python executable: {sys.executable}", source="RISLogic")
    return False
        
def _create_driver(headless=True):
    """Create and return a headless Chrome driver and WebDriverWait."""
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    d = webdriver.Chrome(service=service, options=options)
    w = WebDriverWait(d, 20)
    return d, w

def click_element_safe(driver_obj, wait_obj, locator, timeout=10):
    """Click element with scroll + JS fallback to handle intercepted clicks."""
    element = WebDriverWait(driver_obj, timeout).until(EC.presence_of_element_located(locator))
    driver_obj.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", element)
    time.sleep(0.15)

    try:
        WebDriverWait(driver_obj, timeout).until(EC.element_to_be_clickable(locator))
        element.click()
    except ElementClickInterceptedException:
        element = wait_obj.until(EC.presence_of_element_located(locator))
        driver_obj.execute_script("arguments[0].click();", element)
    except Exception:
        element = wait_obj.until(EC.presence_of_element_located(locator))
        driver_obj.execute_script("arguments[0].click();", element)

def click_any_locator_safe(driver_obj, wait_obj, locators, timeout=10, retries=2):
    """Try multiple locators and retries to click a target element."""
    last_exception = None
    for _ in range(retries):
        for locator in locators:
            try:
                click_element_safe(driver_obj, wait_obj, locator, timeout=timeout)
                return True
            except Exception as e:
                last_exception = e
                continue
        time.sleep(0.25)
    if last_exception:
        raise last_exception
    return False

def get_cell_text(td_element):
    """Extract text from a table cell, preferring nested <label> when available."""
    try:
        return td_element.find_element(By.TAG_NAME, "label").text.strip()
    except Exception:
        return td_element.text.strip()

def normalize_rtl_text(text):
    if text is None:
        return ""
    cleaned = str(text)
    cleaned = cleaned.replace("\u200f", "").replace("\u200e", "")
    cleaned = cleaned.replace("\u202a", "").replace("\u202b", "").replace("\u202c", "")
    cleaned = " ".join(cleaned.split())
    return cleaned.strip()

def fill_input_text_safe(driver_obj, element, value):
    text_value = normalize_rtl_text(value)
    try:
        element.clear()
    except Exception:
        pass

    try:
        element.send_keys(text_value)
    except Exception:
        driver_obj.execute_script("arguments[0].value = arguments[1];", element, text_value)

    current_value = (element.get_attribute("value") or "").strip()
    if normalize_rtl_text(current_value) != text_value:
        driver_obj.execute_script(
            "arguments[0].value = arguments[1];"
            "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
            "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
            element,
            text_value,
        )

def click_web_element_safe(driver_obj, element):
    """Click an already-found web element with JS fallback."""
    driver_obj.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", element)
    time.sleep(0.15)
    try:
        element.click()
    except Exception:
        try:
            ActionChains(driver_obj).move_to_element(element).pause(0.1).click().perform()
        except Exception:
            driver_obj.execute_script("arguments[0].click();", element)

def click_and_wait_new_window(driver_obj, element, timeout=8):
    """Click and wait for a new window/tab. Returns new handle or None."""
    before = set(driver_obj.window_handles)
    click_web_element_safe(driver_obj, element)

    end_time = time.time() + timeout
    while time.time() < end_time:
        current = set(driver_obj.window_handles)
        diff = list(current - before)
        if diff:
            return diff[0]
        time.sleep(0.15)
    return None

def run_login_only(username, password, log_widget=None):
    """Create driver (if needed) and perform only the login step.

    Runs in a background thread and updates `log_widget`.
    """

    global driver, wait
    if not _ensure_selenium_ready():
        _post_ris_status(False)
        return
    with driver_lock:
        _post_ui_log("Starting.....")
        try:
            if driver is None:
                driver, wait = _create_driver(headless=True)
                _post_ui_log("Driver created")
            driver.get("http://41.33.211.219:2220/ris-ui/Login.jspx")
            # If we are already on a logged-in page, attempt safe logout first
            if "Login.jspx" not in driver.current_url:
                safe_logout(driver, _post_ui_log)
            _post_ui_log("Logging in...")
            wait.until(EC.presence_of_element_located((By.ID, "j_id23:username"))).clear()
            wait.until(EC.presence_of_element_located((By.ID, "j_id23:username"))).send_keys(username)
            driver.find_element(By.ID, "j_id23:password").clear()
            driver.find_element(By.ID, "j_id23:password").send_keys(password)
            driver.find_element(By.ID, "j_id23:j_id41").click()
            # Wait for redirect away from login
            WebDriverWait(driver, 10).until(lambda d: "Login.jspx" not in d.current_url)
            _post_ui_log("Login Successful!")
            _post_ris_status(True)

            _post_ui_log("Opening Cases Page...")
            click_element_safe(driver, wait, (By.ID, "j_id46:j_id391"))
            click_element_safe(driver, wait, (By.ID, "j_id46:j_id400"))
            time.sleep(1)
            _post_ui_log("Monitoring for alert popups...")
            try:
                alert = WebDriverWait(driver, 5).until(EC.alert_is_present())
                _post_ui_log("Alert appeared: " + alert.text)
                alert.accept()
                _post_ui_log("Alert Removed.")
            except TimeoutException:
                _post_ui_log("No alert appeared")

        except Exception as e:
            _post_ui_log(f"Login ERROR: {e}")
            _post_ris_status(False)

def run_search_case_by_code(case_code, log_widget=None):
    global driver, wait
    if not _ensure_selenium_ready():
        return None
    with driver_lock:
        if driver is None:
            _post_ui_log("Driver not available. Please login first.")
            return None
        try:
            code_value = str(case_code).strip()
            matched_case_id = None
            exam = None
            pt = None
            pt_email_value = None
            pt_phone_value = None
            pt_mobile_value = None
            ref_doc = None
            ref_email_value = None
            ref_phone_value = None
            ref_mobile_value = None

            if code_value:
                _post_ui_log_throttled(
                    key=f"search_case_id:{code_value}",
                    message=f"Searching Case ID: {code_value}",
                    min_interval_seconds=30.0,
                )
            else:
                _post_ui_log("Case ID empty. Loading all available cases...")

            _post_ui_log_throttled(
                key=f"opening_cases_page:{code_value or 'all'}",
                message="Opening Cases Page...",
                min_interval_seconds=15.0,
            )
            click_element_safe(driver, wait, (By.ID, "j_id46:j_id391"))
            click_element_safe(driver, wait, (By.ID, "j_id46:j_id400"))
            time.sleep(1)
            _post_ui_log_throttled(
                key=f"monitoring_alerts:{code_value or 'all'}",
                message="Monitoring for alert popups...",
                min_interval_seconds=15.0,
            )

            try:
                alert = WebDriverWait(driver, 5).until(EC.alert_is_present())
                _post_ui_log("Alert appeared: " + alert.text)
                alert.accept()
                _post_ui_log("Alert Removed.")
            except TimeoutException:
                _post_ui_log("No alert appeared")

            code_input = wait.until(EC.presence_of_element_located((By.ID, "SUB:searchFrm:j_id782")))
            code_input.clear()
            if code_value:
                code_input.send_keys(code_value)
            click_element_safe(driver, wait, (By.ID, "SUB:searchFrm:j_id824"))
            time.sleep(1)
            _post_ui_log_throttled(
                key=f"search_submitted:{code_value or 'all'}",
                message="Search submitted.",
                min_interval_seconds=15.0,
            )

            try: # getting cases table rows
                cases_div = driver.find_element(By.ID, "SUB:TableFrm:j_id826")
                cases_table = cases_div.find_element(By.ID, "SUB:TableFrm:TableID")
                cases_table_body = cases_table.find_element(By.XPATH, "./tbody")
                rows = cases_table_body.find_elements(By.XPATH, "./tr") if cases_table_body else []
            except Exception:
                rows = []
                return None

            rows_len = len(rows)
            if rows_len > 0:
                _post_ui_log("Found " + str(rows_len) + " Cases")
                for row in rows:
                    tds = row.find_elements(By.TAG_NAME, "td")
                    if len(tds) < 9:
                        continue
                    case_id = get_cell_text(tds[0])
                    if case_id == code_value:
                        matched_case_id = case_id
                        exam = get_cell_text(tds[2])
                        pt = get_cell_text(tds[5])
                        ref_doc = get_cell_text(tds[8])
                        break
            else:
                _post_ui_log("No Cases Found")
                return None

            if matched_case_id is None:
                _post_ui_log(f"Case not found for code: {code_value}")
                return None

            try: # getting ref doctor data
                normalized_name = normalize_rtl_text(ref_doc)
                click_element_safe(driver, wait, (By.ID, "j_id46:j_id156"))
                click_element_safe(driver, wait, (By.ID, "j_id46:j_id203"))
                time.sleep(1)
                title_span = wait.until(EC.presence_of_element_located((By.ID, "SUB:j_id771")))
                title_text = title_span.text.strip()
                _post_ui_log(f"Ref. Doctor page title: {title_text}")
                if "الأطباء المعالجين" in title_text:

                    try: # getting ref doctor data

                        try: # try reset search 1st
                            click_element_safe(driver, wait, (By.ID, "SUB:FormID:j_id852")) # new search
                            time.sleep(1)
                        except Exception:
                            _post_ui_log("new search failed")
                            return None

                        try: # try write ref doctor in search input
                            name_input = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:NameID")))
                            driver.execute_script(
                                "arguments[0].setAttribute('dir','rtl');"
                                "arguments[0].style.direction='rtl';"
                                "arguments[0].style.textAlign='right';",
                                name_input,
                            )
                            fill_input_text_safe(driver, name_input, normalized_name)
                        except Exception:
                            _post_ui_log("ref doctor name input failed")
                            return None
                        
                        try: # try click search button
                            click_element_safe(driver, wait, (By.ID, "SUB:FormID:j_id857"))
                            time.sleep(1)
                        except Exception:
                            _post_ui_log("ref doctor search click failed")
                            return None
                        
                        try: # try click doctor row
                            doctor_row = wait.until(EC.element_to_be_clickable((By.ID, "SUB:TableFrm:TableID:0")))
                            click_web_element_safe(driver, doctor_row)
                            time.sleep(1) 
                        except Exception:
                            _post_ui_log("ref doctor row click failed")
                            return None

                        try: # try extract doctor contact info
                            ref_email_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:EmailID"))).get_attribute("value") or ""
                            ref_phone_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:PhoneID"))).get_attribute("value") or ""
                            ref_mobile_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:MobileID"))).get_attribute("value") or ""
                            if ref_email_value == "":
                                ref_email_value = None
                            if ref_phone_value == "":
                                ref_phone_value = None
                            if ref_mobile_value == "":
                                ref_mobile_value = None
                        except Exception:
                            _post_ui_log("ref doctor contact extraction failed")
                            ref_email_value = None
                            ref_phone_value = None
                            ref_mobile_value = None
                            return None

                        try: # try extract notes
                            notes_element = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:j_id849")))
                            notes_value = notes_element.get_attribute("value") or notes_element.text or ""
                            if notes_value == "":
                                notes_value = None
                        except Exception:
                            _post_ui_log("ref doctor notes extraction failed")
                            notes_value = None
                            return None

                        _post_ui_log(f"Ref. Doctor data fetched for case {case_id}")
                        _post_ui_log(f"         ref_email_value: {ref_email_value}")
                        _post_ui_log(f"         ref_phone_value: {ref_phone_value}")
                        _post_ui_log(f"         ref_mobile_value: {ref_mobile_value}")
                        _post_ui_log(f"         notes_value: {notes_value}")
                    except Exception as e:
                        _post_ui_log(f"Ref. Doctor data ERROR: {e}")
                        return None      
            except Exception as e:
                _post_ui_log(f"Ref. Doctor page open ERROR: {e}")
                return None
            
            try: # getting pt data
                normalized_name = normalize_rtl_text(pt)
                click_element_safe(driver, wait, (By.ID, "j_id46:j_id156"))
                click_element_safe(driver, wait, (By.ID, "j_id46:j_id163"))
                time.sleep(1)
                title_span = wait.until(EC.presence_of_element_located((By.ID, "SUB:j_id771")))
                title_text = title_span.text.strip()
                _post_ui_log(f"Patient page title: {title_text}")
                if "المرضى" in title_text:

                    try: # getting pt data

                        try: # try reset search 1st
                            click_element_safe(driver, wait, (By.ID, "SUB:FormID:j_id849"))
                            time.sleep(1)
                        except Exception:
                            _post_ui_log("new pt search failed")
                            return None

                        try: # try write pt name in search input
                            pt_name_input = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:NameID")))
                            driver.execute_script(
                                "arguments[0].setAttribute('dir','rtl');"
                                "arguments[0].style.direction='rtl';"
                                "arguments[0].style.textAlign='right';",
                                pt_name_input,
                            )
                            fill_input_text_safe(driver, pt_name_input, normalized_name)
                        except Exception:
                            _post_ui_log("pt name input failed")
                            return None
                        
                        try: # try click search button
                            click_element_safe(driver, wait, (By.ID, "SUB:FormID:j_id854"))
                            time.sleep(1)
                        except Exception:
                            _post_ui_log("pt search button click failed")
                            return None

                        try: # try click pt row
                            pt_row = wait.until(EC.element_to_be_clickable((By.ID, "SUB:TableFrm:TableID:0")))
                            click_web_element_safe(driver, pt_row)
                            time.sleep(1)
                        except Exception:
                            _post_ui_log("pt row click failed")
                            return None

                        try: # try extract pt contact info
                            pt_email_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:EmailID"))).get_attribute("value") or ""
                            pt_phone_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:PhoneID"))).get_attribute("value") or ""
                            pt_mobile_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:MobileID"))).get_attribute("value") or ""

                            if pt_email_value == "":
                                pt_email_value = None
                            if pt_phone_value == "":
                                pt_phone_value = None
                            if pt_mobile_value == "":
                                pt_mobile_value = None
                        except Exception:
                            _post_ui_log("pt contact extraction failed")
                            pt_email_value = None
                            pt_phone_value = None
                            pt_mobile_value = None
                            return None

                        _post_ui_log(f"Patient data fetched for case {case_id}")
                        _post_ui_log(f"         pt_email_value: {pt_email_value}")
                        _post_ui_log(f"         pt_phone_value: {pt_phone_value}")
                        _post_ui_log(f"         pt_mobile_value: {pt_mobile_value}")
                    except Exception as e:
                        _post_ui_log(f"Patient data ERROR: {e}")
                        return None
            except Exception as e:
                _post_ui_log(f"Patient page open ERROR: {e}")
                return None

        except Exception as e:
            error_text = str(e)
            _post_ui_log(f"Search ERROR: {error_text}")
            lowered = error_text.lower()
            if "invalid session id" in lowered or "no such window" in lowered:
                try:
                    if driver is not None:
                        driver.quit()
                except Exception:
                    pass
                driver = None
                wait = None
                _post_ris_status(False)
                _post_ui_log("RIS browser session reset; re-login required.")
            return None
    
        results = {
                "pt": pt, 
                "exam": exam, 
                "ref_doc": ref_doc,
                "ref_email_value": ref_email_value,
                "ref_phone_value": ref_phone_value,
                "ref_mobile_value": ref_mobile_value,
                "pt_email_value": pt_email_value,
                "pt_phone_value": pt_phone_value,
                "pt_mobile_value": pt_mobile_value,
                }
        meaningful_values = [
            pt,
            exam,
            ref_doc,
            ref_email_value,
            ref_phone_value,
            ref_mobile_value,
            pt_email_value,
            pt_phone_value,
            pt_mobile_value,
        ]
        if not any((str(v).strip() if v is not None else "") for v in meaningful_values):
            _post_ui_log(f"Search returned empty data for case {code_value}; skipping update.")
            return None

        # logger.info("Final extracted data: %s", results)
        return results



def run_search_yesterday_case_by_code(case_code, log_widget=None):
    global driver, wait
    if not _ensure_selenium_ready():
        return None
    with driver_lock:
        if driver is None:
            _post_ui_log("Driver not available. Please login first.")
            return None
        try:
            code_value = str(case_code).strip()
            matched_case_id = None
            exam = None
            pt = None
            pt_email_value = None
            pt_phone_value = None
            pt_mobile_value = None
            ref_doc = None
            ref_email_value = None
            ref_phone_value = None
            ref_mobile_value = None

            if code_value:
                _post_ui_log_throttled(
                    key=f"search_case_id:{code_value}",
                    message=f"Searching Case ID: {code_value}",
                    min_interval_seconds=30.0,
                )
            else:
                _post_ui_log("Case ID empty. Loading all available cases...")

            _post_ui_log_throttled(
                key=f"opening_cases_page:{code_value or 'all'}",
                message="Opening Cases Page...",
                min_interval_seconds=15.0,
            )
            click_element_safe(driver, wait, (By.ID, "j_id46:j_id391"))
            click_element_safe(driver, wait, (By.ID, "j_id46:j_id400"))
            time.sleep(1)
            _post_ui_log_throttled(
                key=f"monitoring_alerts:{code_value or 'all'}",
                message="Monitoring for alert popups...",
                min_interval_seconds=15.0,
            )

            try:
                alert = WebDriverWait(driver, 5).until(EC.alert_is_present())
                _post_ui_log("Alert appeared: " + alert.text)
                alert.accept()
                _post_ui_log("Alert Removed.")
            except TimeoutException:
                _post_ui_log("No alert appeared")

            code_input = wait.until(EC.presence_of_element_located((By.ID, "SUB:searchFrm:j_id782")))
            code_input.clear()
            if code_value:
                code_input.send_keys(code_value)
            click_element_safe(driver, wait, (By.ID, "SUB:searchFrm:j_id824"))
            time.sleep(1)
            _post_ui_log_throttled(
                key=f"search_submitted:{code_value or 'all'}",
                message="Search submitted.",
                min_interval_seconds=15.0,
            )

            try: # getting cases table rows
                cases_div = driver.find_element(By.ID, "SUB:TableFrm:j_id826")
                cases_table = cases_div.find_element(By.ID, "SUB:TableFrm:TableID")
                cases_table_body = cases_table.find_element(By.XPATH, "./tbody")
                rows = cases_table_body.find_elements(By.XPATH, "./tr") if cases_table_body else []
            except Exception:
                rows = []
                return None

            rows_len = len(rows)
            if rows_len > 0:
                _post_ui_log("Found " + str(rows_len) + " Cases")
                for row in rows:
                    tds = row.find_elements(By.TAG_NAME, "td")
                    if len(tds) < 9:
                        continue
                    case_id = get_cell_text(tds[0])
                    if case_id == code_value:
                        matched_case_id = case_id
                        exam = get_cell_text(tds[2])
                        pt = get_cell_text(tds[5])
                        ref_doc = get_cell_text(tds[8])
                        break
            else:
                _post_ui_log("No Cases Found")
                return None

            if matched_case_id is None:
                _post_ui_log(f"Case not found for code: {code_value}")
                return None

            try: # getting ref doctor data
                normalized_name = normalize_rtl_text(ref_doc)
                click_element_safe(driver, wait, (By.ID, "j_id46:j_id156"))
                click_element_safe(driver, wait, (By.ID, "j_id46:j_id203"))
                time.sleep(1)
                title_span = wait.until(EC.presence_of_element_located((By.ID, "SUB:j_id771")))
                title_text = title_span.text.strip()
                _post_ui_log(f"Ref. Doctor page title: {title_text}")
                if "الأطباء المعالجين" in title_text:

                    try: # getting ref doctor data

                        try: # try reset search 1st
                            click_element_safe(driver, wait, (By.ID, "SUB:FormID:j_id852")) # new search
                            time.sleep(1)
                        except Exception:
                            _post_ui_log("new search failed")
                            return None

                        try: # try write ref doctor in search input
                            name_input = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:NameID")))
                            driver.execute_script(
                                "arguments[0].setAttribute('dir','rtl');"
                                "arguments[0].style.direction='rtl';"
                                "arguments[0].style.textAlign='right';",
                                name_input,
                            )
                            fill_input_text_safe(driver, name_input, normalized_name)
                        except Exception:
                            _post_ui_log("ref doctor name input failed")
                            return None
                        
                        try: # try click search button
                            click_element_safe(driver, wait, (By.ID, "SUB:FormID:j_id857"))
                            time.sleep(1)
                        except Exception:
                            _post_ui_log("ref doctor search click failed")
                            return None
                        
                        try: # try click doctor row
                            doctor_row = wait.until(EC.element_to_be_clickable((By.ID, "SUB:TableFrm:TableID:0")))
                            click_web_element_safe(driver, doctor_row)
                            time.sleep(1) 
                        except Exception:
                            _post_ui_log("ref doctor row click failed")
                            return None

                        try: # try extract doctor contact info
                            ref_email_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:EmailID"))).get_attribute("value") or ""
                            ref_phone_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:PhoneID"))).get_attribute("value") or ""
                            ref_mobile_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:MobileID"))).get_attribute("value") or ""
                            if ref_email_value == "":
                                ref_email_value = None
                            if ref_phone_value == "":
                                ref_phone_value = None
                            if ref_mobile_value == "":
                                ref_mobile_value = None
                        except Exception:
                            _post_ui_log("ref doctor contact extraction failed")
                            ref_email_value = None
                            ref_phone_value = None
                            ref_mobile_value = None
                            return None

                        try: # try extract notes
                            notes_element = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:j_id849")))
                            notes_value = notes_element.get_attribute("value") or notes_element.text or ""
                            if notes_value == "":
                                notes_value = None
                        except Exception:
                            _post_ui_log("ref doctor notes extraction failed")
                            notes_value = None
                            return None

                        _post_ui_log(f"Ref. Doctor data fetched for case {case_id}")
                        _post_ui_log(f"         ref_email_value: {ref_email_value}")
                        _post_ui_log(f"         ref_phone_value: {ref_phone_value}")
                        _post_ui_log(f"         ref_mobile_value: {ref_mobile_value}")
                        _post_ui_log(f"         notes_value: {notes_value}")
                    except Exception as e:
                        _post_ui_log(f"Ref. Doctor data ERROR: {e}")
                        return None      
            except Exception as e:
                _post_ui_log(f"Ref. Doctor page open ERROR: {e}")
                return None
            
            try: # getting pt data
                normalized_name = normalize_rtl_text(pt)
                click_element_safe(driver, wait, (By.ID, "j_id46:j_id156"))
                click_element_safe(driver, wait, (By.ID, "j_id46:j_id163"))
                time.sleep(1)
                title_span = wait.until(EC.presence_of_element_located((By.ID, "SUB:j_id771")))
                title_text = title_span.text.strip()
                _post_ui_log(f"Patient page title: {title_text}")
                if "المرضى" in title_text:

                    try: # getting pt data

                        try: # try reset search 1st
                            click_element_safe(driver, wait, (By.ID, "SUB:FormID:j_id849"))
                            time.sleep(1)
                        except Exception:
                            _post_ui_log("new pt search failed")
                            return None

                        try: # try write pt name in search input
                            pt_name_input = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:NameID")))
                            driver.execute_script(
                                "arguments[0].setAttribute('dir','rtl');"
                                "arguments[0].style.direction='rtl';"
                                "arguments[0].style.textAlign='right';",
                                pt_name_input,
                            )
                            fill_input_text_safe(driver, pt_name_input, normalized_name)
                        except Exception:
                            _post_ui_log("pt name input failed")
                            return None
                        
                        try: # try click search button
                            click_element_safe(driver, wait, (By.ID, "SUB:FormID:j_id854"))
                            time.sleep(1)
                        except Exception:
                            _post_ui_log("pt search button click failed")
                            return None

                        try: # try click pt row
                            pt_row = wait.until(EC.element_to_be_clickable((By.ID, "SUB:TableFrm:TableID:0")))
                            click_web_element_safe(driver, pt_row)
                            time.sleep(1)
                        except Exception:
                            _post_ui_log("pt row click failed")
                            return None

                        try: # try extract pt contact info
                            pt_email_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:EmailID"))).get_attribute("value") or ""
                            pt_phone_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:PhoneID"))).get_attribute("value") or ""
                            pt_mobile_value = wait.until(EC.presence_of_element_located((By.ID, "SUB:FormID:MobileID"))).get_attribute("value") or ""

                            if pt_email_value == "":
                                pt_email_value = None
                            if pt_phone_value == "":
                                pt_phone_value = None
                            if pt_mobile_value == "":
                                pt_mobile_value = None
                        except Exception:
                            _post_ui_log("pt contact extraction failed")
                            pt_email_value = None
                            pt_phone_value = None
                            pt_mobile_value = None
                            return None

                        _post_ui_log(f"Patient data fetched for case {case_id}")
                        _post_ui_log(f"         pt_email_value: {pt_email_value}")
                        _post_ui_log(f"         pt_phone_value: {pt_phone_value}")
                        _post_ui_log(f"         pt_mobile_value: {pt_mobile_value}")
                    except Exception as e:
                        _post_ui_log(f"Patient data ERROR: {e}")
                        return None
            except Exception as e:
                _post_ui_log(f"Patient page open ERROR: {e}")
                return None

        except Exception as e:
            error_text = str(e)
            _post_ui_log(f"Search ERROR: {error_text}")
            lowered = error_text.lower()
            if "invalid session id" in lowered or "no such window" in lowered:
                try:
                    if driver is not None:
                        driver.quit()
                except Exception:
                    pass
                driver = None
                wait = None
                _post_ris_status(False)
                _post_ui_log("RIS browser session reset; re-login required.")
            return None
    
        results = {
                "pt": pt, 
                "exam": exam, 
                "ref_doc": ref_doc,
                "ref_email_value": ref_email_value,
                "ref_phone_value": ref_phone_value,
                "ref_mobile_value": ref_mobile_value,
                "pt_email_value": pt_email_value,
                "pt_phone_value": pt_phone_value,
                "pt_mobile_value": pt_mobile_value,
                }
        meaningful_values = [
            pt,
            exam,
            ref_doc,
            ref_email_value,
            ref_phone_value,
            ref_mobile_value,
            pt_email_value,
            pt_phone_value,
            pt_mobile_value,
        ]
        if not any((str(v).strip() if v is not None else "") for v in meaningful_values):
            _post_ui_log(f"Search returned empty data for case {code_value}; skipping update.")
            return None

        # logger.info("Final extracted data: %s", results)
        return results


def run_logout(log_widget=None):
    """Logout and close the browser, clear driver state."""

    global driver, wait, attachment_viewer_driver, ref_driver, ref_wait, reception_driver, reception_wait
    with driver_lock:
        if driver is None:
            _post_ui_log("Driver not available.")
            return
        try:
            safe_logout(driver, _post_ui_log)
        except Exception as e:
            _post_ui_log(f"Logout helper failed: {e}")
        try:
            driver.quit()
        except Exception as e:
            _post_ui_log(f"Error quitting driver: {e}")
        try:
            if attachment_viewer_driver:
                attachment_viewer_driver.quit()
        except Exception as e:
            _post_ui_log(f"Error quitting attachment viewer driver: {e}")
            pass
        try:
            if ref_driver:
                ref_driver.quit()
        except Exception:
            pass
        try:
            if reception_driver:
                reception_driver.quit()
        except Exception:
            pass
        attachment_viewer_driver = None
        ref_driver = None
        ref_wait = None
        reception_driver = None
        reception_wait = None
        driver = None
        wait = None
        _post_ris_status(False)

def start_login():
    username = "Eslam"
    password = "INGODwetrust"
        
    threading.Thread(target=run_login_only, args=(username, password), daemon=True).start()

def start_logout():
    threading.Thread(target=run_logout, args=(), daemon=True).start()

def safe_logout(driver, log):
    try:
        log("Attempting logout...")
        logout_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "LangForm:logOutBtn"))
        )
        logout_button.click()
        time.sleep(2)
        log("Logout successful")
    except Exception as e:
        log(f"Logout failed or already logged out: {e}")

def on_close():
    global driver, attachment_viewer_driver, ref_driver, reception_driver
    with driver_lock:
        try:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
        except Exception as e:
            _post_ui_log(f"Error quitting driver on close: {e}")
