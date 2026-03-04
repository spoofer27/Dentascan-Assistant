from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import hashlib
import json
import os
import sys
import threading
import time
from urllib.parse import quote

import pydicom
import requests

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


@dataclass
class _TokenState:
    access_token: str = ""
    expires_at: float = 0.0


@dataclass(frozen=True)
class CaseCandidate:
    day_kind: str
    case_dir: Path
    orthanc_dir: Path
    details_path: Path
    details_payload: dict
    signature: str


class _ThrottledFile:
    def __init__(self, file_path: Path, max_upload_bps: int | None):
        self._file_path = file_path
        self._max_upload_bps = max_upload_bps if (max_upload_bps and max_upload_bps > 0) else None
        self._size = file_path.stat().st_size
        self._sent = 0
        self._started = time.monotonic()
        self._handle = file_path.open("rb")

    def __len__(self):
        return self._size

    def read(self, size: int = -1):
        chunk = self._handle.read(size)
        if not chunk:
            return chunk

        self._sent += len(chunk)
        if self._max_upload_bps:
            elapsed = time.monotonic() - self._started
            expected = self._sent / float(self._max_upload_bps)
            if expected > elapsed:
                time.sleep(expected - elapsed)
        return chunk

    def close(self) -> None:
        self._handle.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class UploadingLogic:
    _ACTIVE_CASES_LOCK = threading.Lock()
    _ACTIVE_CASES: set[str] = set()

    def __init__(
        self,
        staging_path: Path,
        base_url: str,
        token_url: str,
        client_id: str,
        client_secret: str,
        timeout: float = 20.0,
        max_upload_kbps: int | None = None,
    ):
        if not base_url:
            raise ValueError("PACS base_url is required")
        if not token_url:
            raise ValueError("PACS token_url is required")
        if not client_id:
            raise ValueError("PACS client_id is required")
        if not client_secret:
            raise ValueError("PACS client_secret is required")

        self.staging_path = Path(staging_path)
        self.base_url = base_url.rstrip("/")
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.max_upload_kbps = max_upload_kbps if (max_upload_kbps and max_upload_kbps > 0) else None

        self.session = requests.Session()
        self._token = _TokenState()
        self._processed_signatures: dict[str, str] = {}

    @classmethod
    def from_config(cls) -> "UploadingLogic":
        base_url = os.getenv("PACS_BASE_URL") or getattr(service_config, "PACS_BASE_URL", "")
        token_url = os.getenv("PACS_TOKEN_URL") or getattr(service_config, "PACS_TOKEN_URL", "")
        client_id = os.getenv("PACS_CLIENT_ID") or getattr(service_config, "PACS_CLIENT_ID", "")
        client_secret = os.getenv("PACS_CLIENT_SECRET") or getattr(service_config, "PACS_CLIENT_SECRET", "")

        raw_limit = os.getenv("PACS_MAX_UPLOAD_BPS")
        if raw_limit is None or str(raw_limit).strip() == "":
            raw_limit = getattr(service_config, "PACS_MAX_UPLOAD_BPS", None)
        try:
            max_upload_kbps = int(raw_limit) if (raw_limit is not None and str(raw_limit).strip() != "") else None
        except Exception:
            max_upload_kbps = None

        return cls(
            staging_path=Path(service_config.SERVICE_STAGING_PATH),
            base_url=base_url,
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            max_upload_kbps=max_upload_kbps,
        )

    def _post_ui_log(self, message: str, source: str = "UploadingLogic", level: str = "info") -> None:
        text = f"[{source}] {message}"
        if level == "error":
            logger.error(text)
        elif level == "warning":
            logger.warning(text)
        else:
            logger.info(text)

    def ensure_today_staging_folder(self) -> Path:
        return self._build_day_folder(datetime.now(), create=True)

    def ensure_yesterday_staging_folder(self) -> Path:
        return self._build_day_folder(datetime.now() - timedelta(days=1), create=True)

    def process_staging_folder(self, staging_folder: Path) -> dict:
        return self.process_day_folder(staging_folder=staging_folder, day_kind="today")

    def process_y_staging_folder(self, staging_folder: Path) -> dict:
        return self.process_day_folder(staging_folder=staging_folder, day_kind="yesterday")

    def find_cases_to_upload(self, staging_folder: Path) -> list[CaseCandidate]:
        return self._discover_cases(staging_folder=staging_folder, day_kind="today")

    def find_y_cases_to_upload(self, staging_folder: Path) -> list[CaseCandidate]:
        return self._discover_cases(staging_folder=staging_folder, day_kind="yesterday")

    def process_day_folder(self, staging_folder: Path, day_kind: str) -> dict:
        candidates = self._discover_cases(staging_folder=staging_folder, day_kind=day_kind)
        summary = {
            "day": day_kind,
            "found": len(candidates),
            "processed": 0,
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "labels_added": 0,
            "label_errors": 0,
        }

        for case in candidates:
            result = self.process_case(case)
            summary["processed"] += 1
            summary["uploaded"] += int(result.get("uploaded", 0))
            summary["skipped"] += int(result.get("skipped", 0))
            summary["failed"] += int(result.get("failed", 0))
            summary["labels_added"] += int(result.get("labels_added", 0))
            summary["label_errors"] += int(result.get("label_errors", 0))

        return summary

    def process_case(self, candidate: CaseCandidate) -> dict:
        case_key = self._case_key(candidate.case_dir)

        with self._ACTIVE_CASES_LOCK:
            if case_key in self._ACTIVE_CASES:
                return {
                    "case": candidate.case_dir.name,
                    "uploaded": 0,
                    "skipped": 0,
                    "failed": 0,
                    "labels_added": 0,
                    "label_errors": 0,
                }
            self._ACTIVE_CASES.add(case_key)

        try:
            if self._processed_signatures.get(case_key) == candidate.signature:
                return {
                    "case": candidate.case_dir.name,
                    "uploaded": 0,
                    "skipped": 0,
                    "failed": 0,
                    "labels_added": 0,
                    "label_errors": 0,
                }

            upload_result = self._upload_orthanc_folder(candidate.orthanc_dir, candidate.case_dir.name)
            label_result = {"added": 0, "errors": 0}

            if upload_result["failed"] == 0:
                labels = self._labels_from_details(candidate.details_payload)
                study_uid = self._study_uid_for_case(candidate)
                label_result = self._apply_labels(
                    study_uid=study_uid,
                    labels=labels,
                    case_name=candidate.case_dir.name,
                )
                self._processed_signatures[case_key] = candidate.signature

            return {
                "case": candidate.case_dir.name,
                "uploaded": upload_result["uploaded"],
                "skipped": upload_result["skipped"],
                "failed": upload_result["failed"],
                "labels_added": label_result["added"],
                "label_errors": label_result["errors"],
            }
        finally:
            with self._ACTIVE_CASES_LOCK:
                self._ACTIVE_CASES.discard(case_key)

    def _build_day_folder(self, target_dt: datetime, create: bool) -> Path:
        staging_root = self.staging_path / "Staging"
        day_folder = (
            staging_root
            / target_dt.strftime("%Y")
            / target_dt.strftime("%m-%Y")
            / target_dt.strftime("%d-%m-%Y")
        )
        if create:
            day_folder.mkdir(parents=True, exist_ok=True)
        return day_folder

    def _discover_cases(self, staging_folder: Path, day_kind: str) -> list[CaseCandidate]:
        if not staging_folder.exists():
            return []

        candidates: list[CaseCandidate] = []
        case_dirs = sorted([path for path in staging_folder.iterdir() if path.is_dir()], key=lambda p: p.name.lower())

        for case_dir in case_dirs:
            orthanc_dir = case_dir / "Orthanc"
            if not orthanc_dir.exists() or not orthanc_dir.is_dir():
                continue

            details_path = self._resolve_details_json(case_dir)
            if details_path is None:
                continue

            details_payload = self._read_json_file(details_path)
            if details_payload is None:
                continue

            signature = self._build_case_signature(orthanc_dir, details_path)
            if not signature:
                continue

            candidates.append(
                CaseCandidate(
                    day_kind=day_kind,
                    case_dir=case_dir,
                    orthanc_dir=orthanc_dir,
                    details_path=details_path,
                    details_payload=details_payload,
                    signature=signature,
                )
            )

        return candidates

    def _resolve_details_json(self, case_dir: Path) -> Path | None:
        preferred = case_dir / f"{case_dir.name}_details.json"
        if preferred.exists() and preferred.is_file():
            return preferred

        json_files = sorted([path for path in case_dir.glob("*.json") if path.is_file()], key=lambda p: p.name.lower())
        if not json_files:
            return None
        return json_files[0]

    def _read_json_file(self, file_path: Path) -> dict | None:
        try:
            with file_path.open("r", encoding="utf-8") as file_obj:
                payload = json.load(file_obj)
            return payload if isinstance(payload, dict) else None
        except Exception as exc:
            self._post_ui_log(f"Invalid case details JSON {file_path.name}: {exc}", level="warning")
            return None

    def _build_case_signature(self, orthanc_dir: Path, details_path: Path) -> str:
        dicom_files = self._collect_dicom_files(orthanc_dir)
        if not dicom_files:
            return ""

        hasher = hashlib.sha1()
        for file_path in dicom_files:
            try:
                relative = file_path.relative_to(orthanc_dir).as_posix()
            except Exception:
                relative = file_path.name

            stat = file_path.stat()
            hasher.update(relative.encode("utf-8", errors="ignore"))
            hasher.update(str(stat.st_size).encode("utf-8"))
            hasher.update(str(stat.st_mtime_ns).encode("utf-8"))

        try:
            hasher.update(str(details_path.stat().st_mtime_ns).encode("utf-8"))
        except Exception:
            pass

        return hasher.hexdigest()

    def _collect_dicom_files(self, folder: Path) -> list[Path]:
        return sorted(
            [
                path
                for path in folder.rglob("*.dcm")
                if path.is_file() and not path.name.startswith(".")
            ],
            key=lambda p: p.as_posix().lower(),
        )

    def _upload_orthanc_folder(self, orthanc_dir: Path, case_name: str) -> dict:
        files = self._collect_dicom_files(orthanc_dir)
        result = {
            "total": len(files),
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
        }

        if not files:
            return result

        self._post_ui_log(f"PACS upload scan for {case_name}: {len(files)} file(s)")

        for file_path in files:
            try:
                sop_uid = self._get_sop_instance_uid(file_path)
                if sop_uid and self._instance_exists_by_uid(sop_uid):
                    result["skipped"] += 1
                    continue

                self.upload_file(file_path)
                result["uploaded"] += 1
            except Exception as exc:
                result["failed"] += 1
                self._post_ui_log(
                    f"PACS upload failed for {case_name}/{file_path.name}: {exc}",
                    level="error",
                )

        if result["failed"]:
            self._post_ui_log(
                f"PACS upload completed for {case_name} with failures: "
                f"uploaded={result['uploaded']}, skipped={result['skipped']}, failed={result['failed']}",
                level="warning",
            )
        else:
            self._post_ui_log(
                f"PACS upload completed for {case_name}: "
                f"uploaded={result['uploaded']}, skipped={result['skipped']}",
            )

        return result

    def _runtime_max_upload_bps(self) -> int | None:
        if self.max_upload_kbps and self.max_upload_kbps > 0:
            return self.max_upload_kbps * 1024
        return None

    def upload_file(self, path: Path) -> dict:
        token = self._get_token()
        response = self._post_instance_file(path, token=token)

        if response.status_code == 401:
            self._token = _TokenState()
            token = self._get_token()
            response = self._post_instance_file(path, token=token)

        if response.status_code == 409:
            return {}

        response.raise_for_status()
        try:
            return response.json()
        except Exception:
            return {}

    def _post_instance_file(self, path: Path, token: str) -> requests.Response:
        max_upload_bps = self._runtime_max_upload_bps()
        with _ThrottledFile(path, max_upload_bps=max_upload_bps) as stream:
            return self.session.post(
                f"{self.base_url}/instances",
                data=stream,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/dicom",
                    "Content-Length": str(len(stream)),
                },
                timeout=2000,
            )

    def _request_with_auth(self, method: str, url: str, **kwargs) -> requests.Response:
        headers = dict(kwargs.pop("headers", {}) or {})

        token = self._get_token()
        headers["Authorization"] = f"Bearer {token}"
        response = self.session.request(method=method, url=url, headers=headers, **kwargs)

        if response.status_code == 401:
            self._token = _TokenState()
            token = self._get_token()
            headers["Authorization"] = f"Bearer {token}"
            response = self.session.request(method=method, url=url, headers=headers, **kwargs)

        return response

    def _fetch_token(self) -> _TokenState:
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        response = self.session.post(self.token_url, data=payload, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()

        access_token = data.get("access_token", "")
        expires_in = float(data.get("expires_in", 0))
        if not access_token:
            raise ValueError("PACS token response missing access_token")

        expires_at = time.time() + max(0.0, expires_in - 30.0)
        return _TokenState(access_token=access_token, expires_at=expires_at)

    def _get_token(self) -> str:
        if self._token.access_token and time.time() < self._token.expires_at:
            return self._token.access_token
        self._token = self._fetch_token()
        return self._token.access_token

    def _get_sop_instance_uid(self, path: Path) -> str | None:
        try:
            ds = pydicom.dcmread(path, stop_before_pixels=True)
            sop = ds.get("SOPInstanceUID", None)
            return str(sop) if sop else None
        except Exception:
            return None

    def _instance_exists_by_uid(self, sop_instance_uid: str | None) -> bool:
        if not sop_instance_uid:
            return False

        payload = {
            "Level": "Instance",
            "Query": {"SOPInstanceUID": sop_instance_uid},
            "Limit": 1,
        }

        response = self._request_with_auth(
            "POST",
            f"{self.base_url}/tools/find",
            json=payload,
            timeout=self.timeout,
        )

        if response.status_code == 404:
            return False

        response.raise_for_status()
        try:
            return bool(response.json())
        except Exception:
            return False

    def _study_uid_for_case(self, candidate: CaseCandidate) -> str | None:
        payload = candidate.details_payload or {}
        for key in ("study_uid", "StudyInstanceUID", "study_instance_uid"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return self._extract_study_uid_from_folder(candidate.orthanc_dir)

    def _extract_study_uid_from_folder(self, folder: Path) -> str | None:
        for path in self._collect_dicom_files(folder):
            try:
                ds = pydicom.dcmread(path, stop_before_pixels=True)
                study_uid = ds.get("StudyInstanceUID", None)
                if study_uid:
                    return str(study_uid)
            except Exception:
                continue
        return None

    def _labels_from_details(self, payload: dict) -> list[str]:
        labels: list[str] = []
        raw_labels = payload.get("case_labels")

        if isinstance(raw_labels, list):
            for label in raw_labels:
                if isinstance(label, str) and label.strip():
                    labels.append(label.strip())
        elif isinstance(raw_labels, str) and raw_labels.strip():
            labels.append(raw_labels.strip())

        exam = payload.get("exam")
        if isinstance(exam, str) and exam.strip():
            labels.append(exam.strip())

        deduped: list[str] = []
        seen: set[str] = set()
        for label in labels:
            key = label.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(label)
        return deduped

    def _apply_labels(self, study_uid: str | None, labels: list[str], case_name: str) -> dict:
        result = {"added": 0, "errors": 0}

        if not labels:
            return result

        if not study_uid:
            self._post_ui_log(
                f"Skipping labels for {case_name}: StudyInstanceUID not available",
                level="warning",
            )
            result["errors"] = len(labels)
            return result

        for label in labels:
            ok = self.add_label(study_uid=study_uid, label=label)
            if ok:
                result["added"] += 1
            else:
                result["errors"] += 1

        return result

    def add_label(self, study_uid: str, label: str) -> bool:
        if not study_uid or not label:
            return False

        orthanc_id = self._find_study_orthanc_id(study_uid)
        if not orthanc_id:
            self._post_ui_log(
                f"PACS study not found for StudyInstanceUID {study_uid}",
                level="warning",
            )
            return False

        encoded_label = quote(label, safe="")
        response = self._request_with_auth(
            "PUT",
            f"{self.base_url}/studies/{orthanc_id}/labels/{encoded_label}",
            timeout=self.timeout,
        )

        if response.status_code in (200, 201, 204, 409):
            self._post_ui_log(f"PACS label added for study {study_uid}: {label}")
            return True

        try:
            response.raise_for_status()
            self._post_ui_log(f"PACS label added for study {study_uid}: {label}")
            return True
        except Exception as exc:
            self._post_ui_log(
                f"PACS label failed for study {study_uid} ({label}): {exc}",
                level="error",
            )
            return False

    def _find_study_orthanc_id(self, study_uid: str) -> str | None:
        payload = {
            "Level": "Study",
            "Query": {"StudyInstanceUID": study_uid},
            "Limit": 1,
        }

        response = self._request_with_auth(
            "POST",
            f"{self.base_url}/tools/find",
            json=payload,
            timeout=self.timeout,
        )

        if response.status_code == 404:
            return None

        response.raise_for_status()
        try:
            data = response.json()
        except Exception:
            return None

        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, str) and first.strip():
                return first.strip()
        return None

    def _case_key(self, case_dir: Path) -> str:
        try:
            return os.path.normcase(str(case_dir.resolve()))
        except Exception:
            return os.path.normcase(str(case_dir))
