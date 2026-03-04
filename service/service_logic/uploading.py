import pydicom
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
import importlib


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

def _upload_pacs_folder(self, orthanc_folder: Path, case_name: str, labels: list[str] = None):
        if labels is None:
            labels = []
        try:
            # Try relative import first (when used as package), fallback to absolute (when service runs standalone)
            try:
                from .uploading_logic import UploadingLogic
            except ImportError:
                from uploading_logic import UploadingLogic
            uploader = UploadingLogic.from_config()
        except Exception as exc:
            self._post_ui_log(
                f"PACS upload skipped for {case_name}: {exc}",
                source="UploadingLogic",
            )
            return

        uploader.upload_folder_async(orthanc_folder, case_name, labels=labels)

def _is_case_uploaded_to_pacs(self, orthanc_folder: Path) -> bool:
        if not orthanc_folder.exists():
            return False
        try:
            # Try relative import first (when used as package), fallback to absolute (when service runs standalone)
            try:
                from .uploading_logic import UploadingLogic
                self._post_ui_log("Imported PacsUploader with relative import", source="FolderMonitor")
            except ImportError:
                from uploading_logic import UploadingLogic
                self._post_ui_log("Imported PacsUploader with absolute import", source="FolderMonitor")
            uploader = UploadingLogic.from_config()
        except Exception as exc:
            self._post_ui_log(f"Failed to create PacsUploader: {exc}", source="FolderMonitor")
            return False

        try:
            for dicom_path in orthanc_folder.rglob("*.dcm"):
                if dicom_path.name.startswith("."):
                    continue
                try:
                    ds = pydicom.dcmread(dicom_path, stop_before_pixels=True)
                    sop_uid = getattr(ds, "SOPInstanceUID", None)
                    series_uid = getattr(ds, "SeriesInstanceUID", None)
                    if sop_uid and series_uid:
                        if uploader._instance_exists_by_uid(sop_uid, series_uid):
                            return True
                except Exception as exc:
                    self._post_ui_log(f"Error checking DICOM file {dicom_path.name} for PACS upload: {exc}", source="FolderMonitor")
                    continue
        except Exception as exc:
            self._post_ui_log(f"Error iterating DICOM files in {orthanc_folder}: {exc}", source="FolderMonitor")
        return False

def _add_case_label(self, study_uid: str, label: str):
        try:
            # Try relative import first (when used as package), fallback to absolute (when service runs standalone)
            try:
                from .uploading_logic import UploadingLogic
            except ImportError:
                from uploading_logic import UploadingLogic
            uploader = UploadingLogic.from_config()
        except Exception as exc:
            self._post_ui_log(
                f"PACS label skipped for {study_uid}: {exc}",
                source="FolderMonitor",
            )
            return

        uploader.add_label(study_uid, label)

def _get_pacs_upload_progress(self, orthanc_folder: Path, case_folder: Path) -> dict:
        """
        Get PACS upload progress for a case.
        Returns dict with: {
            "total_files": int,
            "uploaded_files": int,
            "current_percent": int,
            "is_uploading": bool,
            "is_complete": bool
        }
        """
        global _uploaded_files_cache
        
        result = {
            "total_files": 0,
            "uploaded_files": 0,
            "current_percent": 0,
            "is_uploading": False,
            "is_complete": False
        }
        
        if not orthanc_folder.exists():
            return result
        
        # Step 1: Count total DICOM files in orthanc folder (recursive)
        try:
            dcm_files = list(orthanc_folder.rglob("*.dcm"))
            result["total_files"] = len(dcm_files)
        except Exception as exc:
            self._post_ui_log(f"Error counting DICOM files in {orthanc_folder}: {exc}", source="FolderMonitor")
            return result
        
        if result["total_files"] == 0:
            return result
        
        # Check if upload is in progress
        lock_file = orthanc_folder / ".pacs_uploading"
        progress_file = orthanc_folder / ".pacs_progress"
        
        result["is_uploading"] = lock_file.exists()
        
        # Step 2: If uploading, show progress from progress file
        if result["is_uploading"] and progress_file.exists():
            try:
                progress_text = progress_file.read_text(encoding="utf-8").strip()
                if progress_text:
                    # Format: "current_file,total_files,current_file_percent"
                    if "," in progress_text:
                        parts = progress_text.split(",")
                        if len(parts) >= 3:
                            current_file = int(parts[0])
                            file_percent = int(parts[2])
                            result["current_percent"] = file_percent
                    else:
                        result["current_percent"] = int(progress_text)
            except Exception:
                pass
            return result
        
        # Step 3: If NOT uploading, check PACS for each file and count uploaded
        # Use cache to avoid rechecking files that are already marked as uploaded
        uploaded_count = 0
        try:
            # Try relative import first (when used as package), fallback to absolute
            try:
                from .uploading_logic import UploadingLogic
            except ImportError:
                from uploading_logic import UploadingLogic
            
            uploader = UploadingLogic.from_config()
            
            # Loop through each DCM file and check if it exists in PACS
            for dcm_file in dcm_files:
                try:
                    file_path_str = str(dcm_file.resolve())
                    
                    # Check if file is already in cache (marked as uploaded previously)
                    if file_path_str in _uploaded_files_cache:
                        uploaded_count += 1
                        continue
                    
                    # Not in cache, query PACS
                    sop_uid = uploader._get_sop_instance_uid(dcm_file)
                    series_uid = uploader._get_series_instance_uid(dcm_file)
                    
                    # If both UIDs are available, check if file exists in PACS
                    if sop_uid and series_uid:
                        if uploader._instance_exists_by_uid(sop_uid, series_uid):
                            uploaded_count += 1
                            # Mark file as uploaded in cache
                            _uploaded_files_cache.add(file_path_str)
                except Exception:
                    # If error reading UIDs, skip this file
                    continue
            
            result["uploaded_files"] = uploaded_count
            result["is_complete"] = (uploaded_count == result["total_files"]) and (uploaded_count > 0)
            
        except Exception as exc:
            self._post_ui_log(f"Error checking PACS upload status: {exc}", source="FolderMonitor")
        
        return result

def find_cases_to_upload(self, case: Path, staging_folder: Path):
        """
        Process a single case: stage it and upload to PACS.
        This method contains the core staging logic extracted from find_cases.
        """
        case_staging_folder = staging_folder / case.name
        attachments_folder = case_staging_folder / "Attachments"
        dicoms_folder = case_staging_folder / "Dicoms"
        orthanc_folder = case_staging_folder / "Orthanc"
        
        attachments_folder.mkdir(parents=True, exist_ok=True)
        dicoms_folder.mkdir(parents=True, exist_ok=True)
        orthanc_folder.mkdir(parents=True, exist_ok=True)

        # Scan for PDFs, images, DICOMs (same as find_cases)
        IGNORED_SUBFOLDERS = {"planmeca romexis", "ondemand 3d"}
        PDF_EXTS = {".pdf"}
        IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}
        
        pdf_files = []
        image_files = []
        dicom_2d_files = []
        single_dicom_files = []
        project_files = []
        multi_series = {}
        study_info = None
        sop_uids = set()
        romexis = False
        
        # Scan for PDFs and images (skip IGNORED_SUBFOLDERS)
        try:
            stack = [case]
            while stack:
                current = stack.pop()
                for item in current.iterdir():
                    if item.is_dir():
                        if item.name.lower() in IGNORED_SUBFOLDERS:
                            continue
                        stack.append(item)
                        continue
                    
                    if not item.is_file():
                        continue
                    
                    ext = item.suffix.lower()
                    if ext in PDF_EXTS:
                        pdf_files.append(item)
                        try:
                            dest_path = attachments_folder / item.name
                            if not dest_path.exists():
                                shutil.copy2(item, dest_path)
                        except Exception:
                            pass
                    elif ext in IMAGE_EXTS:
                        image_files.append(item)
                        try:
                            dest_path = attachments_folder / item.name
                            if not dest_path.exists():
                                shutil.copy2(item, dest_path)
                        except Exception:
                            pass
        except Exception:
            pass
        
        # Scan for DICOMs (scan all folders including IGNORED_SUBFOLDERS)
        stack = [(case, None)]
        while stack:
            current, rel_path = stack.pop()
            for item in current.iterdir():
                if item.is_dir() and 'ondemand' in item.name.lower():
                    new_rel_path = f"{rel_path}/{item.name}" if rel_path else item.name
                    stack.append((item, new_rel_path))
                    continue
                
                if not item.is_file():
                    continue
                
                # Handle DICOMs
                if is_dicom(item):
                    try:
                        ds = pydicom.dcmread(item, stop_before_pixels=True, force=True)
                        
                        if study_info is None:
                            study_info = self._extract_study_info(ds)
                        
                        sop_uid = getattr(ds, "SOPInstanceUID", None)
                        if sop_uid and sop_uid not in sop_uids:
                            sop_uids.add(sop_uid)
                        else:
                            continue
                        
                        impl_version = getattr(getattr(ds, "file_meta", None), "ImplementationVersionName", "")
                        if not romexis and "ROMEXIS" in str(impl_version).upper():
                            romexis = True
                        
                        number_of_frames = getattr(ds, "NumberOfFrames", None)
                        modality = getattr(ds, "Modality", None)
                        is_from_ondemand = rel_path and "ondemand 3d" in rel_path.lower()
                        
                        if number_of_frames is not None:
                            if int(number_of_frames) > 1:
                                if modality and modality.upper() == "CT" and is_from_ondemand:
                                    single_dicom_files.append(item)
                            else:
                                if modality and modality.upper() == "CT" and is_from_ondemand:
                                    project_files.append(item)
                        else:
                            if modality and modality.upper() != "CT":
                                dicom_2d_files.append(item)
                            else:
                                series_uid = getattr(ds, "SeriesInstanceUID", None)
                                if not series_uid:
                                    series_uid = f"unknown-{case.name}"
                                multi_series.setdefault(series_uid, []).append(item)
                        
                        # Copy to dicoms folder
                        try:
                            dest_path = dicoms_folder / item.name
                            if not dest_path.exists():
                                shutil.copy2(item, dest_path)
                        except Exception:
                            pass
                            
                    except Exception:
                        pass
        
        # Stage DICOMs to Orthanc folder
        case_labels = []
        
        # Handle single DICOMs
        if single_dicom_files:
            case_labels.append("3D-DICOM")
            for dicom_path in single_dicom_files:
                try:
                    out_name = f"{dicom_path.stem} DCM {dicom_path.suffix or '.dcm'}"
                    out_path = orthanc_folder / out_name
                    if out_path.exists():
                        continue
                    
                    if romexis:
                        shutil.copy2(dicom_path, out_path)
                    else:
                        ds = pydicom.dcmread(dicom_path)
                        if not getattr(ds, "file_meta", None):
                            ds.file_meta = FileMetaDataset()
                        ds.file_meta.ImplementationVersionName = "ROMEXIS_10"
                        ds.InstitutionName = self.institution_name
                        ds.save_as(out_path, write_like_original=False)
                except Exception:
                    pass
        
        # Handle multi-file series
        elif multi_series:
            case_labels.append("3D-DICOM")
            multi_dicom_files = max(multi_series.values(), key=len)
            if multi_dicom_files:
                try:
                    out_name = f"{case.name} DCM.dcm"
                    out_path = orthanc_folder / out_name
                    if not out_path.exists():
                        self._convert_multi_file_to_multiframe(multi_dicom_files, out_path)
                except Exception:
                    pass
        
        # Handle projects
        if project_files:
            case_labels.append("OD3D")
            for project_path in project_files:
                try:
                    out_name = f"{project_path.stem} DCM {project_path.suffix or '.dcm'}"
                    out_path = orthanc_folder / out_name
                    if out_path.exists():
                        continue
                    shutil.copy2(project_path, out_path)
                except Exception:
                    pass
        
        # Handle 2D DICOMs
        if dicom_2d_files:
            case_labels.append("2D-DICOM")
            for dicom_path in dicom_2d_files:
                try:
                    out_name = f"{dicom_path.stem} DCM {dicom_path.suffix or '.dcm'}"
                    out_path = orthanc_folder / out_name
                    if out_path.exists():
                        continue
                    shutil.copy2(dicom_path, out_path)
                except Exception:
                    pass
        
        # Handle PDFs and images
        if pdf_files or image_files:
            if study_info is None:
                study_info = {"study_uid": generate_uid()}
            elif not study_info.get("study_uid"):
                study_info["study_uid"] = generate_uid()
            
            for pdf_path in pdf_files:
                try:
                    out_path = orthanc_folder / f"{pdf_path.stem} PDF.dcm"
                    if not out_path.exists():
                        self._create_pdf_dicom(pdf_path, out_path, study_info, case.name)
                        case_labels.append("PDF")
                except Exception:
                    pass
            
            for image_path in image_files:
                try:
                    out_path = orthanc_folder / f"{image_path.stem} IMG.dcm"
                    if not out_path.exists():
                        self._create_image_dicom(image_path, out_path, study_info, case.name)
                        case_labels.append("Image")
                except Exception:
                    pass
        
        # Upload to PACS
        case_labels.append("Yesterday-Recovery")
        self._upload_pacs_folder(orthanc_folder, case.name, labels=case_labels)
        
def main(stop_event):
    _post_ui_log("Uploading worker is starting...", source="ServiceLog")
    try:
        while not stop_event.is_set():
            try:
                uploader = UploadingLogic.from_config()
                today_staging_folder = uploader.ensure_today_staging_folder()
                yesterday_staging_folder = uploader.ensure_yesterday_staging_folder()
            except Exception as exc:
                _post_ui_log(f"Error in uploading logic: {exc}", source="ServiceLog", color="red")
                servicemanager.LogErrorMsg(f"Error in uploading logic: {exc}")
            
            if uploader is not None:
                try:
                    if uploader.find_cases_to_upload(today_staging_folder) is not None:
                        cases_to_upload = uploader.find_cases_to_upload(today_staging_folder)
                        uploader.process_staging_folder(today_staging_folder)
                except Exception as exc:
                    _post_ui_log(f"Error processing staging folder: {exc}", source="ServiceLog", color="red")
                    servicemanager.LogErrorMsg(f"Error processing staging folder: {exc}")

                try:
                    if uploader.find_y_cases_to_upload(yesterday_staging_folder) is not None:
                        y_cases_to_upload = uploader.find_y_cases_to_upload(yesterday_staging_folder)
                        uploader.process_y_staging_folder(yesterday_staging_folder)
                except Exception as exc:
                    _post_ui_log(f"Error processing staging folder: {exc}", source="ServiceLog", color="red")
                    servicemanager.LogErrorMsg(f"Error processing staging folder: {exc}")

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
        _post_ui_log(f"Error in uploading thread: {exc}", source="ServiceLog", color="red")
        servicemanager.LogErrorMsg(f"Error in uploading thread: {exc}")
        raise

if __name__ == "__main__":
    _run_console_debug()