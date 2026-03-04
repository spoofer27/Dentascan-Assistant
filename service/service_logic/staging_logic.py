from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import json
import os
import re
import sys
import time
import shutil
import threading
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.misc import is_dicom
import logging
import json
import importlib
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.encaps import encapsulate, generate_pixel_data_frame
from pydicom.uid import (
    ExplicitVRLittleEndian,
    EncapsulatedPDFStorage,
    SecondaryCaptureImageStorage,
    PYDICOM_IMPLEMENTATION_UID,
    generate_uid,
)

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

# global lock for json operations
file_lock = threading.Lock()
# file_path = Path("case_details.txt")

# Module-level cache to track uploaded DICOM files (persists until app closes)
_uploaded_files_cache = set()


@dataclass(frozen=True)
class StagingLogic:
    # Root path where the date folder will be created (e.g., Desktop).
    root_path: Path
    # Staging path for future use (e.g., temporary processing location).
    staging_path: Path
    # Format for the monitored folder name (default: dd-mm-YYYY).
    date_format: str = "%d-%m-%Y"
    # Institution name for the monitor.
    institution_name: str = ""
    # Enable RIS enrichment (Selenium search) while scanning cases.
    ris_enabled: bool = True


    def _post_ui_log(self, message: str, source: str = "StagingLogic"):
        logger.info("[%s] %s", source, message)
    
    @classmethod
    def from_config(cls, ris_enabled: bool = True) -> "StagingLogic":
        # Build the monitor using the root path from service_config.
        return cls(
            root_path=Path(service_config.SERVICE_ROOT_PATH),
            staging_path=Path(service_config.SERVICE_STAGING_PATH),
            institution_name=service_config.INSTITUTION_NAME,
            ris_enabled=ris_enabled,
        )
    
    def ensure_today_folder(self) -> Path:
        # Create (or find) today's folder under root_path and return its path.
        today_folder_name = datetime.now().strftime(self.date_format)
        today_folder = self.root_path / today_folder_name
        today_folder.mkdir(parents=True, exist_ok=True)
        return today_folder
    
    def ensure_today_staging_folder(self) -> Path:
        # Create (or find) today's staging folder under staging_path and return its path.
        now = datetime.now()
        staging_root = self.staging_path / "Staging"
        year_staging_folder = staging_root / now.strftime("%Y")
        month_staging_folder = year_staging_folder / now.strftime("%m-%Y")
        today_staging_folder = month_staging_folder / now.strftime("%d-%m-%Y")
        today_staging_folder.mkdir(parents=True, exist_ok=True)
        return today_staging_folder
    
    def ensure_yesterday_folder(self) -> Path:
        # Get yesterday's folder path under root_path (does not create it).
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_folder_name = yesterday.strftime(self.date_format)
        yesterday_folder = self.root_path / yesterday_folder_name
        return yesterday_folder
    
    def ensure_yesterday_staging_folder(self) -> Path:
        # Create (or find) yesterday's staging folder under staging_path and return its path.
        yesterday = datetime.now() - timedelta(days=1)
        staging_root = self.staging_path / "Staging"
        year_staging_folder = staging_root / yesterday.strftime("%Y")
        month_staging_folder = year_staging_folder / yesterday.strftime("%m-%Y")
        yesterday_staging_folder = month_staging_folder / yesterday.strftime("%d-%m-%Y")
        yesterday_staging_folder.mkdir(parents=True, exist_ok=True)
        return yesterday_staging_folder
    
    def _extract_study_info(self, ds) -> dict:
        return {
            "sop_uid": getattr(ds.file_meta, "MediaStorageSOPInstanceUID", None),
            "study_uid": getattr(ds, "StudyInstanceUID", None),
            "patient_name": getattr(ds, "PatientName", ""),
            "patient_id": getattr(ds, "PatientID", ""),
            "patient_birth_date": getattr(ds, "PatientBirthDate", ""),
            "patient_sex": getattr(ds, "PatientSex", ""),
            "study_date": getattr(ds, "StudyDate", ""),
            "study_time": getattr(ds, "StudyTime", ""),
            "accession_number": getattr(ds, "AccessionNumber", ""),
            "study_description": getattr(ds, "StudyDescription", ""),
        }
    
    def _build_file_meta(self, sop_class_uid, sop_instance_uid) -> FileMetaDataset:
        meta = FileMetaDataset()
        meta.FileMetaInformationVersion = b"\x00\x01"
        meta.MediaStorageSOPClassUID = sop_class_uid
        meta.MediaStorageSOPInstanceUID = sop_instance_uid
        meta.TransferSyntaxUID = ExplicitVRLittleEndian
        meta.ImplementationClassUID = PYDICOM_IMPLEMENTATION_UID
        meta.ImplementationVersionName = "ROMEXIS_10"
        return meta
    
    def _create_pdf_dicom(self, pdf_path: Path, out_path: Path, study_info: dict, case_name: str = "", labels: list[str] = []):
        labels = []
        now = datetime.now()
        patient_name = study_info.get("patient_name", None)
        with pdf_path.open("rb") as f:
            pdf_bytes = f.read()

        sop_instance_uid = study_info.get("sop_uid") or generate_uid()
        file_meta = self._build_file_meta(EncapsulatedPDFStorage, sop_instance_uid)
        ds = FileDataset(str(out_path), {}, file_meta=file_meta, preamble=b"\x00" * 128)
        ds.PatientName = patient_name if patient_name else case_name
        ds.PatientID = study_info.get("patient_id", "")
        ds.PatientBirthDate = study_info.get("patient_birth_date", "")
        ds.PatientSex = study_info.get("patient_sex", "")
        ds.StudyDate = study_info.get("study_date", "")
        ds.StudyTime = study_info.get("study_time", "")
        ds.AccessionNumber = study_info.get("accession_number", "")
        ds.StudyDescription = study_info.get("study_description", "")
        ds.SOPClassUID = EncapsulatedPDFStorage
        ds.SOPInstanceUID = sop_instance_uid
        ds.StudyInstanceUID = study_info.get("study_uid") or generate_uid()
        ds.SeriesInstanceUID = generate_uid()
        ds.Modality = "DOC"
        ds.InstitutionName = self.institution_name
        ds.SeriesNumber = 1
        ds.InstanceNumber = 1
        ds.ContentDate = now.strftime("%Y%m%d")
        ds.ContentTime = now.strftime("%H%M%S")
        ds.MIMETypeOfEncapsulatedDocument = "application/pdf"
        ds.EncapsulatedDocument = encapsulate([pdf_bytes])
        ds.EncapsulatedDocumentLength = len(pdf_bytes)
        ds.is_little_endian = True
        ds.is_implicit_VR = False
        ds.save_as(out_path, write_like_original=False)
        self._post_ui_log(f"Created PDF DICOM for {pdf_path.name} in Orthanc staging for case {case_name}")

    def _create_image_dicom(self, image_path: Path, out_path: Path, study_info: dict, case_name: str = ""):
        try:
            from PIL import Image  # type: ignore[import-not-found]
        except Exception:
            return

        image = Image.open(image_path).convert("RGB")
        pixel_bytes = image.tobytes()
        rows, cols = image.size[1], image.size[0]
        now = datetime.now()
        patient_name = study_info.get("patient_name", None)
        sop_instance_uid = study_info.get("sop_uid") or generate_uid()
        file_meta = self._build_file_meta(SecondaryCaptureImageStorage, sop_instance_uid)
        ds = FileDataset(str(out_path), {}, file_meta=file_meta, preamble=b"\x00" * 128)
        ds.PatientName = patient_name if patient_name else case_name
        ds.PatientID = study_info.get("patient_id", "")
        ds.PatientBirthDate = study_info.get("patient_birth_date", "")
        ds.PatientSex = study_info.get("patient_sex", "")
        ds.StudyDate = study_info.get("study_date", "")
        ds.StudyTime = study_info.get("study_time", "")
        ds.AccessionNumber = study_info.get("accession_number", "")
        ds.StudyDescription = study_info.get("study_description", "")
        ds.SOPClassUID = SecondaryCaptureImageStorage
        ds.SOPInstanceUID = sop_instance_uid
        ds.StudyInstanceUID = study_info.get("study_uid") or generate_uid()
        ds.SeriesInstanceUID = generate_uid()
        ds.Modality = "SC"
        ds.SeriesNumber = 1
        ds.InstanceNumber = 1
        ds.ContentDate = now.strftime("%Y%m%d")
        ds.ContentTime = now.strftime("%H%M%S")
        ds.InstitutionName = self.institution_name
        ds.SamplesPerPixel = 3
        ds.PhotometricInterpretation = "RGB"
        ds.PlanarConfiguration = 0
        ds.Rows = rows
        ds.Columns = cols
        ds.BitsAllocated = 8
        ds.BitsStored = 8
        ds.HighBit = 7
        ds.PixelRepresentation = 0
        ds.PixelData = pixel_bytes
        ds.is_little_endian = True
        ds.is_implicit_VR = False
        ds.save_as(out_path, write_like_original=False)
        self._post_ui_log(f"Created image DICOM for {image_path.name} in Orthanc staging for case {case_name}")

    def _convert_multi_file_to_multiframe(self, dicom_paths, out_path):
            try:
                import numpy as np
            except Exception as e:
                self._post_ui_log(f"NumPy import failed: {e}")
            if not dicom_paths:
                raise ValueError("dicom_paths cannot be empty")
            # Load all DICOMs
            try:
                datasets = [pydicom.dcmread(p) for p in dicom_paths]
            except Exception as e:
                self._post_ui_log(f"Failed to read DICOM files: {e}")
                raise
            # Sort by InstanceNumber if present (important!)
            try:
                datasets.sort(key=lambda d: getattr(d, "InstanceNumber", 0))
            except Exception as e:
                self._post_ui_log(f"Failed to sort DICOM files: {e}")
                raise
            try:
                first_ds = datasets[0]
            except Exception as e:
                self._post_ui_log(f"Failed to get first DICOM dataset: {e}")
                raise
            # Stack pixel data into (num_frames, rows, cols)
            try:
                pixel_arrays = [ds.pixel_array for ds in datasets]
            except Exception as e:
                self._post_ui_log(f"Failed to extract pixel data: {e}")
                raise
            try:
                pixel_stack = np.stack(pixel_arrays, axis=0)
            except Exception as e:
                self._post_ui_log(f"Failed to stack pixel data: {e}")
                raise
            # Create new dataset based on first DICOM
            try:
                multi_ds = first_ds.copy()
            except Exception as e:
                self._post_ui_log(f"Failed to copy first DICOM dataset: {e}")
                raise
            # Update required multi-frame attributes
            try:
                multi_ds.NumberOfFrames = pixel_stack.shape[0]
                multi_ds.PixelData = pixel_stack.tobytes()
                multi_ds.InstitutionName = self.institution_name
            except Exception as e:
                self._post_ui_log(f"Failed to set multi-frame attributes: {e}")
                raise
            # Generate new UIDs
            try:
                multi_ds.SOPInstanceUID = first_ds.get("SOPInstanceUID", generate_uid())
                multi_ds.file_meta.MediaStorageSOPInstanceUID = multi_ds.SOPInstanceUID
            except Exception as e:
                self._post_ui_log(f"Failed to generate new UIDs: {e}")
                raise
            # Remove single-frame–specific attributes if present
            if "InstanceNumber" in multi_ds:
                del multi_ds.InstanceNumber
            # Functional Groups (basic — can be expanded if needed)
            if hasattr(multi_ds, "PerFrameFunctionalGroupsSequence"):
                del multi_ds.PerFrameFunctionalGroupsSequence
            # Save as multi-frame DICOM
            multi_ds.save_as(out_path, write_like_original=False)
            self._post_ui_log(f"Saved multi-frame DICOM to {out_path}")

    def _format_case_date(self, ts: float) -> str:
        return datetime.fromtimestamp(ts).strftime("%d-%m-%Y")
    
    def _format_case_time(self, ts: float) -> str:
        dt = datetime.fromtimestamp(ts)
        hour = dt.strftime("%I").lstrip("0") or "12"
        minute = dt.strftime("%M")
        suffix = dt.strftime("%p").lower()
        return f"{hour}:{minute}{suffix}"
    
    def _is_case_staged(self, case_name: str, staging_folder: Path) -> bool:
        case_staging_folder = staging_folder / case_name
        orthanc_folder = case_staging_folder / "Orthanc"
        if not orthanc_folder.exists():
            return False
        try:
            return any(orthanc_folder.iterdir())
        except Exception:
            return False

    def write_case(self, case, file_path: Path):
        temp_file = file_path.with_suffix(".tmp")

        # Convert cases to JSON string FIRST
        new_content = json.dumps(
            case,
            indent=4,
            ensure_ascii=False
        )

        with file_lock:  # 🔒 LOCK START
            if file_path.exists(): # If file exists → compare content
                with file_path.open("r", encoding="utf-8") as f: # reed it
                    current_content = f.read()
                if current_content == new_content: # 🚫 No change → skip writing
                    return False  # nothing updated

            file_path.parent.mkdir(parents=True, exist_ok=True)

            with temp_file.open("w", encoding="utf-8") as f: # ✅ Something changed → write temp and replace
                f.write(new_content)

            temp_file.replace(file_path)
            logger.info("Case %s details written", case.get("name", ""))
        # 🔓 LOCK END

    def find_cases(self):            
        today_staging_folder = self.ensure_today_staging_folder()
        today_folder_name = datetime.now().strftime(self.date_format)
        today_folder = self.root_path / today_folder_name
        
        if not today_folder.exists():
            return 0, []

        EXCLUDED_NAMES = {"cbct", "new folder"}
        cases = []
        
        ris_enabled = self.ris_enabled and ris_logic is not None
        # if ris_enabled:
        #     try:
        #         ris_logic.start_login()
        #         time.sleep(5)
        #     except Exception as exc:
        #         self._post_ui_log(f"Error during RIS login: {exc}", source="StagingLogic")
        # else:
        #     self._post_ui_log("RIS module unavailable; skipping RIS login", source="StagingLogic")

        for case in today_folder.iterdir():
            
            if case.is_dir():
                # exclude generic names
                folder_name = case.name.strip()
                folder_name_lower = folder_name.lower()
                excluded = folder_name_lower in EXCLUDED_NAMES
                real = " " in folder_name_lower
                if not excluded and real: # if folder not generic and has space in name...
                    
                    try: # checking has content ?
                        has_contents = any(case.iterdir())
                    except Exception:
                        has_contents = False # empty folder

                    if has_contents: # not empty folder
                        
                        # collecting main info...
                        try:  # trying date and last modified time
                            stat = case.stat()
                            case_date = self._format_case_date(stat.st_ctime)
                            case_time = self._format_case_time(stat.st_mtime)
                        except Exception:
                            self._post_ui_log(f"Found case folder: {case.name} - Date/Time info unavailable", source="StagingLogic")
                            case_date = case_time = ""
                        
                        # vars 
                        IGNORED_SUBFOLDERS = {"planmeca romexis", "ondemand 3d"}
                        DICOM_FOLDER = "OnDemand"
                        PDF_EXTS = {".pdf"}
                        IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}
                        pdf_count = 0
                        image_count = 0
                        pdf_files = []
                        image_files = []
                        single_dicom_count = 0
                        multiple_dicom_count = 0 
                        project_count = 0
                        romexis = False
                        sop_uids = set()
                        study_info = None
                        dicom_files = []
                        dicom_2d_files = []
                        dicom_2d_count = 0
                        single_dicom_files = []
                        project_files = []
                        multi_series = {}
                        case_staging_folder = today_staging_folder / case.name
                        attachments_folder = case_staging_folder / "Attachments"
                        dicoms_folder = case_staging_folder / "Dicoms"
                        orthanc_folder = case_staging_folder / "Orthanc"
                        orthanc_folder.mkdir(parents=True, exist_ok=True)
                        case_labels = []
                        case_id = None
                        ris= None
                        exam = None
                        pt = None
                        pt_email_value = None
                        pt_phone_value = None
                        pt_mobile_value = None
                        ref_doc = None
                        ref_email_value = None
                        ref_phone_value = None
                        ref_mobile_value = None

                        try: # main checking
                            stack = [case]
                            while stack:
                                current = stack.pop()
                                for item in current.iterdir():
                                    if item.is_dir(): # found a folder
                                        if DICOM_FOLDER.lower() in item.name.lower(): # if it's dicom folder, search dicoms inside
                                            stack.append(item)
                                            for file in item.rglob("*"): # searching dicoms in dicom folder and subfolders
                                                if file.is_file() and is_dicom(file): # found dicom file
                                                    try: # trying to read dicom
                                                        ds = pydicom.dcmread(file, stop_before_pixels=True, force=True)
                                                        full_ds = pydicom.dcmread(file, force=True)
                                                        impl_version = getattr(getattr(ds, "file_meta", None), "ImplementationVersionName", "")
                                                        romexis = "ROMEXIS" in str(impl_version).upper() if not romexis else romexis
                                                        number_of_frames = getattr(ds, "NumberOfFrames", None)
                                                        modality = getattr(ds, "Modality", None)
                                                        patient_name = str(ds.get("PatientName", "")).strip()
                                                        patient_id = str(ds.get("PatientID", "")).strip()
                                                        
                                                        if case_id is None: # try to get case id from patient name or patient id
                                                            id_in_patient_name = re.search(r"\d+", patient_name)
                                                            if id_in_patient_name:
                                                                case_id = id_in_patient_name.group(0)
                                                            else:
                                                                case_id = patient_id if patient_id else None
                                                        
                                                        if ris_enabled and ris is None and case_id is not None: # if case_id found and ris not searched yet, search in RIS
                                                            try: # search case by code in RIS
                                                                ris = ris_logic.run_search_case_by_code(case_id)

                                                            except Exception as exc:
                                                                self._post_ui_log(f"Error searching case by code {case_id}: {exc}")
                                                        else:
                                                            skip_reason = "case_id is None" if case_id is None else ("RIS unavailable" if not ris_enabled else "RIS data already found")
                                                        
                                                        study_info = self._extract_study_info(ds) if study_info is None else study_info
                                                        if number_of_frames is not None: # number_of_frames exist stage all dicoms

                                                            try: # copy dicom to staging dicom folder
                                                                if not dicoms_folder.exists(): # create dicoms folder
                                                                    dicoms_folder.mkdir(parents=True, exist_ok=True)
                                                                dest_path = dicoms_folder / file.name
                                                                if dest_path.exists(): # check item exists
                                                                    if dest_path.stat().st_size == file.stat().st_size: # check size
                                                                        pass # skip
                                                                else: # copy file then continue
                                                                    shutil.copy2(file, dest_path)
                                                            except Exception as exc: # error copying
                                                                self._post_ui_log(f"Failed to copy DICOM file {file.name} to dicoms for case {case.name}: {exc}")
                                                            
                                                            # orthanc staging...............
                                                            out_path = orthanc_folder / file.name # orthanc staging path
                                                            if out_path.exists(): # check item exists
                                                                pass # skip
                                                            if not getattr(ds, "file_meta", None): # ensure file_meta exists
                                                                full_ds.file_meta = FileMetaDataset()
                                                            if not romexis: # check and update ImplementationVersionName to romexis_10 
                                                                 full_ds.file_meta.ImplementationVersionName = "ROMEXIS_10"
                                                            full_ds.InstitutionName = self.institution_name # update institution name
                                                            full_ds.ReferringPhysicianName = ref_doc # update referring physician's name
                                                            full_ds.save_as(out_path, write_like_original=False) # save to orthanc staging
                                                            if int(number_of_frames) > 1:  # single dicom with multiple frames
                                                                single_dicom_count += 1 # count
                                                                single_dicom_files.append(file) # add
                                                                dicom_files.append(file) # add to dicom list
                                                                case_labels.append("3D-DICOM")
                                                            else:  # project (multi-frame)
                                                                project_count += 1 # count
                                                                project_files.append(file) # add
                                                                dicom_files.append(file) # add to dicom list
                                                                case_labels.append("OD3D")
                                                    except InvalidDicomError as exc: # invalid dicom, skip
                                                        self._post_ui_log(f"Invalid DICOM file {file.name} in {item.name}, skipping: {exc}")
                                                        continue
                                            continue
                                        else: # other folder
                                            if item.name.lower() in IGNORED_SUBFOLDERS: # Viewers
                                                continue
                                            else: # not ignored folder, skip
                                                stack.append(item)
                                                continue
                                    else: # not a folder
                                        if item.is_file() and "ondemand" not in item.parent.name.lower(): # found a file
                                            ext = item.suffix.lower() # get extention
                                            if is_dicom(item): # if dicom file, try to read, if readable add to dicom list (for counting later), then copy to staging dicom folder
                                                
                                                try: # trying to read dicom
                                                    ds = pydicom.dcmread(item, stop_before_pixels=True, force=True)
                                                    full_ds = pydicom.dcmread(item, force=True)
                                                    impl_version = getattr(getattr(ds, "file_meta", None), "ImplementationVersionName", "")
                                                    romexis = "ROMEXIS" in str(impl_version).upper() if not romexis else romexis
                                                    number_of_frames = getattr(ds, "NumberOfFrames", None)
                                                    modality = getattr(ds, "Modality", None)
                                                    study_info = self._extract_study_info(ds) if study_info is None else study_info
                                                    patient_name = str(ds.get("PatientName", "")).strip()
                                                    patient_id = str(ds.get("PatientID", "")).strip()
                                                    
                                                    if case_id is None: # try to get case id from patient name or patient id
                                                        id_in_patient_name = re.search(r"\d+", patient_name)
                                                        if id_in_patient_name:
                                                            case_id = id_in_patient_name.group(0)
                                                        else:
                                                            case_id = patient_id if patient_id else None

                                                    if ris_enabled and ris is None and case_id is not None: # if case_id found and ris not searched yet, search in RIS
                                                        try: # search case by code in RIS
                                                            ris = ris_logic.run_search_case_by_code(case_id)
                                                        except Exception as exc:
                                                            self._post_ui_log(f"Error searching case by code {case_id}: {exc}")
                                                    else:
                                                        skip_reason = "case_id is None" if case_id is None else ("RIS unavailable" if not ris_enabled else "RIS data already found")

                                                    if not number_of_frames: # number_of_frames doesn't exist, stage all dicoms
                                                        try: # copy dicom to staging dicom folder
                                                            if not dicoms_folder.exists(): # create dicoms folder
                                                                dicoms_folder.mkdir(parents=True, exist_ok=True)
                                                            dest_path = dicoms_folder / item.name
                                                            if dest_path.exists(): # check item exists
                                                                if dest_path.stat().st_size == item.stat().st_size: # check size
                                                                    continue # skip
                                                            else: # copy file then continue
                                                                shutil.copy2(item, dest_path)
                                                                pass
                                                        except Exception as exc: # error copying
                                                            self._post_ui_log(f"Failed to copy DICOM file {item.name} to dicoms for case {case.name}: {exc}")
                                                            pass
                                                        if modality.upper() != "CT":  # 2D dicom
                                                            self._post_ui_log(f"2D DIICOM")
                                                            dicom_2d_count += 1 # count
                                                            dicom_2d_files.append(item) # add
                                                            dicom_files.append(item) # add to dicom list
                                                            case_labels.append("2D-DICOM")
                                                            # orthanc staging...............
                                                            out_path = orthanc_folder / item.name # orthanc staging path
                                                            if out_path.exists(): # check item exists
                                                                    if out_path.stat().st_size == item.stat().st_size: # check size
                                                                        self._post_ui_log(f"exist......")
                                                                        continue # skip
                                                            if not getattr(ds, "file_meta", None): # ensure file_meta exists
                                                                full_ds.file_meta = FileMetaDataset()
                                                            if not romexis: # check and update ImplementationVersionName to romexis_10 
                                                                 full_ds.file_meta.ImplementationVersionName = "ROMEXIS_10"
                                                            full_ds.InstitutionName = self.institution_name # update institution name
                                                            full_ds.ReferringPhysicianName = ref_doc # update referring physician's name
                                                            full_ds.save_as(out_path, write_like_original=False) # save to orthanc staging
                                                        else:  # multi-file dicom
                                                            self._post_ui_log(f"Multi-file DICOM")
                                                            series_uid = getattr(ds, "SeriesInstanceUID", None)
                                                            if not series_uid:
                                                                series_uid = f"unknown-{case.name}"
                                                            multi_series.setdefault(series_uid, []).append(item)
                                                            has_multiple_dicom = True
                                                except InvalidDicomError as exc: # invalid dicom, skip
                                                    self._post_ui_log(f"Invalid DICOM file {item.name} in {current.name}, skipping: {exc}")
                                                    continue
                                            
                                            elif ext in PDF_EXTS: # if pdf
                                                pdf_count += 1 # count pdf
                                                pdf_files.append(item) # add to pdf list
                                                case_labels.append("PDF")

                                                try: # check item, then copy, then continue
                                                    if not attachments_folder.exists(): # create attachments folder
                                                        attachments_folder.mkdir(parents=True, exist_ok=True)
                                                    dest_path = attachments_folder / item.name
                                                    if dest_path.exists(): # check item exists
                                                        if dest_path.stat().st_size == item.stat().st_size: # check size
                                                            pass # skip
                                                    else: # copy file then continue
                                                        shutil.copy2(item, dest_path)
                                                        continue
                                                except Exception as exc: # check failed or copy failed
                                                    self._post_ui_log(f"Failed to copy PDF file {item.name}: {exc}")
                                                    logger.exception("Failed to copy PDF file %s", item.name)
                                                    pass
                                            
                                                if study_info is None:  #  create study_info
                                                    study_info = {"study_uid": generate_uid()}
                                                elif not study_info.get("study_uid"):
                                                    study_info["study_uid"] = generate_uid()
                                                # orthanc staging...............
                                                out_path = orthanc_folder / f"{item.stem}- PDF.dcm" # orthanc staging path
                                                if out_path.exists(): # check item exists
                                                        if out_path.stat().st_size == item.stat().st_size: # check size
                                                            continue # skip
                                                else: # create pdf dicom and save to orthanc staging
                                                    self._create_pdf_dicom(item, out_path, study_info, case.name) # dicomize and save to orthanc staging

                                            elif ext in IMAGE_EXTS: # if image
                                                image_count += 1 # count image
                                                image_files.append(item) # add to image list
                                                case_labels.append("Image")

                                                try: # check item, then copy, then continue
                                                    if not attachments_folder.exists(): # create attachments folder
                                                        attachments_folder.mkdir(parents=True, exist_ok=True)
                                                    dest_path = attachments_folder / item.name
                                                    if dest_path.exists(): # check item exists
                                                        if dest_path.stat().st_size == item.stat().st_size: # check size
                                                            pass # skip
                                                    else: # copy file then continue
                                                        shutil.copy2(item, dest_path)
                                                        continue
                                                except Exception as exc: # check failed or copy failed
                                                    self._post_ui_log(f"Failed to copy image file {item.name}: {exc}")
                                                    pass
                                                
                                                if study_info is None:  #  create study_info
                                                    study_info = {"study_uid": generate_uid()}
                                                elif not study_info.get("study_uid"):
                                                    study_info["study_uid"] = generate_uid()

                                                # orthanc staging...............
                                                out_path = orthanc_folder / f"{item.stem}- IMG.dcm" # orthanc staging path
                                                if out_path.exists(): # check item exists
                                                        if out_path.stat().st_size == item.stat().st_size: # check size
                                                            continue # skip
                                                else: # create image dicom and save to orthanc staging
                                                    self._create_image_dicom(item, out_path, study_info, case.name) # dicomize and save
                        except Exception as exc: # main checking exc
                            self._post_ui_log(f"Error while scanning case {case.name}: {exc}")
                            pdf_count = 0
                            image_count = 0
                            single_dicom_count = 0
                            multiple_dicom_count = 0
                            project_count = 0
                            has_single_dicom = False
                            has_multiple_dicom = False
                            has_project = False
                            romexis = False
                        
                        has_single_dicom = single_dicom_count > 0
                        has_multiple_dicom = multiple_dicom_count > 0
                        has_project = project_count > 0
                        has_pdf = pdf_count > 0
                        has_images = image_count > 0
                        multi_dicom_files = []
                        if has_multiple_dicom:
                            multiple_dicom_count = len(multi_series)  
                        if multi_series:
                            multi_dicom_files = max(multi_series.values(), key=len)
                            ds = pydicom.dcmread(multi_dicom_files[0], stop_before_pixels=True, force=True)
                            if ds.Modality.upper() == "CT": # cheking if it's CBCT or 2D dicom
                                try:
                                    out_name = f"{case.name}.dcm"
                                    out_path = orthanc_folder / out_name
                                    if out_path.exists(): # check item exists
                                        if out_path.stat().st_size == item.stat().st_size: # check size
                                            continue # skip
                                    self._convert_multi_file_to_multiframe(multi_dicom_files, out_path) # convert and save
                                except Exception as exc:
                                    self._post_ui_log(f"Error while converting multi-file DICOM(s) for case {case.name}: {exc}")
                    else:# empty folder, skip
                        continue
                else: # generic folder, skip
                    continue
            else: # if not a folder, skip
                continue
            
            is_staged = self._is_case_staged(case.name, today_staging_folder) # Check if case is already staged
            case_labels = list(dict.fromkeys(case_labels))  # dedupe, keep order
            if ris is not None:
                pt = ris.get("pt", "")
                exam = ris.get("exam", "")  
                pt_email_value = ris.get("pt_email_value", "")
                pt_phone_value = ris.get("pt_phone_value", "")
                pt_mobile_value = ris.get("pt_mobile_value", "")
                ref_doc = ris.get("ref_doc", "")
                ref_email_value = ris.get("ref_email_value", "")
                ref_phone_value = ris.get("ref_phone_value", "")
                ref_mobile_value = ris.get("ref_mobile_value", "")

            case_info = {
                "case_id": case_id,
                "name": case.name, 
                "date": case_date, 
                "time": case_time,
                "has_pdf": has_pdf,
                "pdf_count": pdf_count,
                "has_images": has_images,
                "image_count": image_count,
                "has_single_dicom": has_single_dicom,
                "single_dicom_count": single_dicom_count,
                "has_multiple_dicom": has_multiple_dicom,
                "multiple_dicom_count": multiple_dicom_count,
                "dicom_2d_count": dicom_2d_count,
                "romexis": romexis,
                "has_project": has_project,
                "project_count": project_count,
                'is_staged': is_staged,
                "pt": pt, 
                "exam": exam, 
                "ref_doc": ref_doc,
                "ref_email_value": ref_email_value,
                "ref_phone_value": ref_phone_value,
                "ref_mobile_value": ref_mobile_value,
                "pt_email_value": pt_email_value,
                "pt_phone_value": pt_phone_value,
                "pt_mobile_value": pt_mobile_value,
                'case_labels': case_labels,
                }
            self._post_ui_log(f"dicom_2d_count: {case_info['dicom_2d_count']}", source="StagingLogic")
            case_info_path = today_staging_folder / case.name / f"{case.name}_details.json"
            self.write_case(case_info, case_info_path)

            cases.append({
                "case_id": case_id,
                "name": case.name, 
                "date": case_date, 
                "time": case_time,
                "has_pdf": has_pdf,
                "pdf_count": pdf_count,
                "has_images": has_images,
                "image_count": image_count,
                "has_single_dicom": has_single_dicom,
                "single_dicom_count": single_dicom_count,
                "has_multiple_dicom": has_multiple_dicom,
                "multiple_dicom_count": multiple_dicom_count,
                "dicom_2d_count": dicom_2d_count,
                "romexis": romexis,
                "has_project": has_project,
                "project_count": project_count,
                'is_staged': is_staged,
                "pt": pt, 
                "exam": exam, 
                "ref_doc": ref_doc,
                "ref_email_value": ref_email_value,
                "ref_phone_value": ref_phone_value,
                "ref_mobile_value": ref_mobile_value,
                "pt_email_value": pt_email_value,
                "pt_phone_value": pt_phone_value,
                "pt_mobile_value": pt_mobile_value,
                'case_labels': case_labels
                })

        return len(cases), cases

    def find_yesterday_cases(self):
        yesterday_folder = self.ensure_yesterday_folder()
        yesterday_staging_folder = self.ensure_yesterday_staging_folder()
        
        if not yesterday_folder.exists():
            return 0, []

        EXCLUDED_NAMES = {"cbct", "new folder"}
        processed_cases = []

        ris_enabled = self.ris_enabled and ris_logic is not None
        # if ris_enabled:
        #     try:
        #         ris_logic.start_login()
        #         time.sleep(5)
        #     except Exception as exc:
        #         self._post_ui_log(f"Error during RIS login: {exc}", source="StagingLogic")
        # else:
        #     self._post_ui_log("RIS module unavailable; skipping RIS login", source="StagingLogic")

        
        for case in yesterday_folder.iterdir():

            if case.is_dir():
                # exclude generic names
                folder_name = case.name.strip()
                folder_name_lower = folder_name.lower()
                excluded = folder_name_lower in EXCLUDED_NAMES
                real = " " in folder_name_lower
                if not excluded and real: # if folder not generic and has space in name...
                    
                    try: # checking has content ?
                        has_contents = any(case.iterdir())
                    except Exception:
                        has_contents = False # empty folder

                    if has_contents: # not empty folder
                        
                        # collecting main info...
                        try:  # trying date and last modified time
                            stat = case.stat()
                            case_date = self._format_case_date(stat.st_ctime)
                            case_time = self._format_case_time(stat.st_mtime)
                        except Exception:
                            self._post_ui_log(f"Found case folder: {case.name} - Date/Time info unavailable")
                            case_date = case_time = ""
                        
                        # vars 
                        IGNORED_SUBFOLDERS = {"planmeca romexis", "ondemand 3d"}
                        DICOM_FOLDER = "OnDemand"
                        PDF_EXTS = {".pdf"}
                        IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}
                        pdf_count = 0
                        image_count = 0
                        pdf_files = []
                        image_files = []
                        single_dicom_count = 0
                        multiple_dicom_count = 0 
                        project_count = 0
                        romexis = False
                        sop_uids = set()
                        study_info = None
                        dicom_files = []
                        dicom_2d_files = []
                        dicom_2d_count = 0
                        single_dicom_files = []
                        project_files = []
                        multi_series = {}
                        case_staging_folder = yesterday_staging_folder / case.name
                        attachments_folder = case_staging_folder / "Attachments"
                        dicoms_folder = case_staging_folder / "Dicoms"
                        orthanc_folder = case_staging_folder / "Orthanc"
                        orthanc_folder.mkdir(parents=True, exist_ok=True)
                        case_labels = []
                        case_id = None
                        ris= None
                        exam = None
                        pt = None
                        pt_email_value = None
                        pt_phone_value = None
                        pt_mobile_value = None
                        ref_doc = None
                        ref_email_value = None
                        ref_phone_value = None
                        ref_mobile_value = None

                        try: # main checking
                            stack = [case]
                            while stack:
                                current = stack.pop()
                                for item in current.iterdir():
                                    if item.is_dir(): # found a folder
                                        if DICOM_FOLDER.lower() in item.name.lower(): # if it's dicom folder, search dicoms inside
                                            stack.append(item)
                                            for file in item.rglob("*"): # searching dicoms in dicom folder and subfolders
                                                if file.is_file() and is_dicom(file): # found dicom file
                                                    try: # trying to read dicom
                                                        ds = pydicom.dcmread(file, stop_before_pixels=True, force=True)
                                                        full_ds = pydicom.dcmread(file, force=True)
                                                        impl_version = getattr(getattr(ds, "file_meta", None), "ImplementationVersionName", "")
                                                        romexis = "ROMEXIS" in str(impl_version).upper() if not romexis else romexis
                                                        number_of_frames = getattr(ds, "NumberOfFrames", None)
                                                        modality = getattr(ds, "Modality", None)
                                                        patient_name = str(ds.get("PatientName", "")).strip()
                                                        patient_id = str(ds.get("PatientID", "")).strip()
                                                        
                                                        if case_id is None: # try to get case id from patient name or patient id
                                                            id_in_patient_name = re.search(r"\d+", patient_name)
                                                            if id_in_patient_name:
                                                                case_id = id_in_patient_name.group(0)
                                                            else:
                                                                case_id = patient_id if patient_id else None

                                                        if ris_enabled and ris is None and case_id is not None: # if case_id found and ris not searched yet, search in RIS
                                                            try: # search case by code in RIS
                                                                ris = ris_logic.run_search_case_by_code(case_id)

                                                            except Exception as exc:
                                                                self._post_ui_log(f"Error searching case by code {case_id}: {exc}")
                                                        else:
                                                            skip_reason = "case_id is None" if case_id is None else ("RIS unavailable" if not ris_enabled else "RIS data already found")
                                                        

                                                        study_info = self._extract_study_info(ds) if study_info is None else study_info
                                                        if number_of_frames is not None: # number_of_frames exist stage all dicoms

                                                            try: # copy dicom to staging dicom folder
                                                                if not dicoms_folder.exists(): # create dicoms folder
                                                                    dicoms_folder.mkdir(parents=True, exist_ok=True)
                                                                dest_path = dicoms_folder / file.name
                                                                if dest_path.exists(): # check item exists
                                                                    if dest_path.stat().st_size == file.stat().st_size: # check size
                                                                        pass # skip
                                                                else: # copy file then continue
                                                                    shutil.copy2(file, dest_path)
                                                            except Exception as exc: # error copying
                                                                self._post_ui_log(f"Failed to copy DICOM file {file.name} to dicoms for case {case.name}: {exc}")

                                                            # orthanc staging...............
                                                            out_path = orthanc_folder / file.name # orthanc staging path
                                                            if out_path.exists(): # check item exists
                                                                pass # skip
                                                            if not getattr(ds, "file_meta", None): # ensure file_meta exists
                                                                full_ds.file_meta = FileMetaDataset()
                                                            if not romexis: # check and update ImplementationVersionName to romexis_10 
                                                                 full_ds.file_meta.ImplementationVersionName = "ROMEXIS_10"
                                                            full_ds.InstitutionName = self.institution_name # update institution name
                                                            full_ds.ReferringPhysicianName = ref_doc # update referring physician's name
                                                            full_ds.save_as(out_path, write_like_original=False) # save to orthanc staging

                                                            if int(number_of_frames) > 1:  # single dicom with multiple frames
                                                                single_dicom_count += 1 # count
                                                                single_dicom_files.append(file) # add
                                                                dicom_files.append(file) # add to dicom list
                                                                case_labels.append("3D-DICOM")
                                                            else:  # project (multi-frame)
                                                                project_count += 1 # count
                                                                project_files.append(file) # add
                                                                dicom_files.append(file) # add to dicom list
                                                                case_labels.append("OD3D")
                                                    except InvalidDicomError as exc: # invalid dicom, skip
                                                        self._post_ui_log(f"Invalid DICOM file {file.name} in {item.name}, skipping: {exc}")
                                                        continue
                                            continue
                                        else: # other folder
                                            if item.name.lower() in IGNORED_SUBFOLDERS: # Viewers
                                                continue
                                            else: # not ignored folder, skip
                                                stack.append(item)
                                                continue
                                    else: # not a folder
                                        if item.is_file() and "ondemand" not in item.parent.name.lower(): # found a file
                                            ext = item.suffix.lower() # get extention
                                            if is_dicom(item): # if dicom file, try to read, if readable add to dicom list (for counting later), then copy to staging dicom folder
                                                
                                                try: # trying to read dicom
                                                    ds = pydicom.dcmread(item, stop_before_pixels=True, force=True)
                                                    full_ds = pydicom.dcmread(item, force=True)
                                                    impl_version = getattr(getattr(ds, "file_meta", None), "ImplementationVersionName", "")
                                                    romexis = "ROMEXIS" in str(impl_version).upper() if not romexis else romexis
                                                    number_of_frames = getattr(ds, "NumberOfFrames", None)
                                                    modality = getattr(ds, "Modality", None)
                                                    patient_name = str(ds.get("PatientName", "")).strip()
                                                    patient_id = str(ds.get("PatientID", "")).strip()
                                                    
                                                    if case_id is None: # try to get case id from patient name or patient id
                                                        id_in_patient_name = re.search(r"\d+", patient_name)
                                                        if id_in_patient_name:
                                                            case_id = id_in_patient_name.group(0)
                                                        else:
                                                            case_id = patient_id if patient_id else None

                                                    if ris_enabled and ris is None and case_id is not None: # if case_id found and ris not searched yet, search in RIS
                                                        try: # search case by code in RIS
                                                            ris = ris_logic.run_search_case_by_code(case_id)
                                                        except Exception as exc:
                                                            self._post_ui_log(f"Error searching case by code {case_id}: {exc}")
                                                    else:
                                                        skip_reason = "case_id is None" if case_id is None else ("RIS unavailable" if not ris_enabled else "RIS data already found")

                                                    study_info = self._extract_study_info(ds) if study_info is None else study_info

                                                    if not number_of_frames: # number_of_frames doesn't exist, stage all dicoms
                                                        try: # copy dicom to staging dicom folder
                                                            if not dicoms_folder.exists(): # create dicoms folder
                                                                dicoms_folder.mkdir(parents=True, exist_ok=True)
                                                            dest_path = dicoms_folder / item.name
                                                            if dest_path.exists(): # check item exists
                                                                if dest_path.stat().st_size == item.stat().st_size: # check size
                                                                    continue # skip
                                                            else: # copy file then continue
                                                                shutil.copy2(item, dest_path)
                                                                pass
                                                        except Exception as exc: # error copying
                                                            self._post_ui_log(f"Failed to copy DICOM file {item.name} to dicoms for case {case.name}: {exc}")
                                                            pass
                                                        if modality.upper() != "CT":  # 2D dicom
                                                            dicom_2d_count += 1 # count
                                                            dicom_2d_files.append(item) # add
                                                            dicom_files.append(item) # add to dicom list
                                                            case_labels.append("2D-DICOM")
                                                            # orthanc staging...............
                                                            out_path = orthanc_folder / item.name # orthanc staging path
                                                            if out_path.exists(): # check item exists
                                                                    if out_path.stat().st_size == item.stat().st_size: # check size
                                                                        continue # skip
                                                            if not getattr(ds, "file_meta", None): # ensure file_meta exists
                                                                full_ds.file_meta = FileMetaDataset()
                                                            if not romexis: # check and update ImplementationVersionName to romexis_10 
                                                                 full_ds.file_meta.ImplementationVersionName = "ROMEXIS_10"
                                                            full_ds.InstitutionName = self.institution_name # update institution name
                                                            full_ds.ReferringPhysicianName = ref_doc # update referring physician's name
                                                            full_ds.save_as(out_path, write_like_original=False) # save to orthanc staging
                                                        else:  # multi-file dicom
                                                            series_uid = getattr(ds, "SeriesInstanceUID", None)
                                                            if not series_uid:
                                                                series_uid = f"unknown-{case.name}"
                                                            multi_series.setdefault(series_uid, []).append(item)
                                                            has_multiple_dicom = True
                                                except InvalidDicomError as exc: # invalid dicom, skip
                                                    self._post_ui_log(f"Invalid DICOM file {item.name} in {current.name}, skipping: {exc}")
                                                    continue
                                            
                                            elif ext in PDF_EXTS: # if pdf
                                                pdf_count += 1 # count pdf
                                                pdf_files.append(item) # add to pdf list
                                                case_labels.append("PDF")

                                                try: # check item, then copy, then continue
                                                    if not attachments_folder.exists(): # create attachments folder
                                                        attachments_folder.mkdir(parents=True, exist_ok=True)
                                                    dest_path = attachments_folder / item.name
                                                    if dest_path.exists(): # check item exists
                                                        if dest_path.stat().st_size == item.stat().st_size: # check size
                                                            pass # skip
                                                    else: # copy file then continue
                                                        shutil.copy2(item, dest_path)
                                                        continue
                                                except Exception as exc: # check failed or copy failed
                                                    self._post_ui_log(f"Failed to copy PDF file {item.name}: {exc}")
                                                    logger.exception("Failed to copy PDF file %s", item.name)
                                                    pass
                                            
                                                if study_info is None:  #  create study_info
                                                    study_info = {"study_uid": generate_uid()}
                                                elif not study_info.get("study_uid"):
                                                    study_info["study_uid"] = generate_uid()
                                                # orthanc staging...............
                                                out_path = orthanc_folder / f"{item.stem}- PDF.dcm" # orthanc staging path
                                                if out_path.exists(): # check item exists
                                                        if out_path.stat().st_size == item.stat().st_size: # check size
                                                            continue # skip
                                                else: # create pdf dicom and save to orthanc staging
                                                    self._create_pdf_dicom(item, out_path, study_info, case.name) # dicomize and save to orthanc staging

                                            elif ext in IMAGE_EXTS: # if image
                                                image_count += 1 # count image
                                                image_files.append(item) # add to image list
                                                case_labels.append("Image")

                                                try: # check item, then copy, then continue
                                                    if not attachments_folder.exists(): # create attachments folder
                                                        attachments_folder.mkdir(parents=True, exist_ok=True)
                                                    dest_path = attachments_folder / item.name
                                                    if dest_path.exists(): # check item exists
                                                        if dest_path.stat().st_size == item.stat().st_size: # check size
                                                            pass # skip
                                                    else: # copy file then continue
                                                        shutil.copy2(item, dest_path)
                                                        continue
                                                except Exception as exc: # check failed or copy failed
                                                    self._post_ui_log(f"Failed to copy image file {item.name}: {exc}")
                                                    pass
                                                
                                                if study_info is None:  #  create study_info
                                                    study_info = {"study_uid": generate_uid()}
                                                elif not study_info.get("study_uid"):
                                                    study_info["study_uid"] = generate_uid()

                                                # orthanc staging...............
                                                out_path = orthanc_folder / f"{item.stem}- IMG.dcm" # orthanc staging path
                                                if out_path.exists(): # check item exists
                                                        if out_path.stat().st_size == item.stat().st_size: # check size
                                                            continue # skip
                                                else: # create image dicom and save to orthanc staging
                                                    self._create_image_dicom(item, out_path, study_info, case.name) # dicomize and save
                        except Exception as exc: # main checking exc
                            self._post_ui_log(f"Error while scanning case {case.name}: {exc}")
                            pdf_count = 0
                            image_count = 0
                            single_dicom_count = 0
                            multiple_dicom_count = 0
                            project_count = 0
                            has_single_dicom = False
                            has_multiple_dicom = False
                            has_project = False
                            romexis = False
                        
                        has_single_dicom = single_dicom_count > 0
                        has_multiple_dicom = multiple_dicom_count > 0
                        has_project = project_count > 0
                        has_pdf = pdf_count > 0
                        has_images = image_count > 0
                        multi_dicom_files = []
                        if has_multiple_dicom:
                            multiple_dicom_count = len(multi_series)  
                        if multi_series:
                            multi_dicom_files = max(multi_series.values(), key=len)
                            ds = pydicom.dcmread(multi_dicom_files[0], stop_before_pixels=True, force=True)
                            if ds.Modality.upper() == "CT": # cheking if it's CBCT or 2D dicom
                                try:
                                    out_name = f"{case.name}.dcm"
                                    out_path = orthanc_folder / out_name
                                    if out_path.exists(): # check item exists
                                        if out_path.stat().st_size == item.stat().st_size: # check size
                                            continue # skip
                                    self._convert_multi_file_to_multiframe(multi_dicom_files, out_path) # convert and save
                                except Exception as exc:
                                    self._post_ui_log(f"Error while converting multi-file DICOM(s) for case {case.name}: {exc}")
                    else:# empty folder, skip
                        continue
                else: # generic folder, skip
                    continue
            else: # if not a folder, skip
                continue
            
            is_staged = self._is_case_staged(case.name, yesterday_staging_folder) # Check if case is already staged
            case_labels = list(dict.fromkeys(case_labels))  # dedupe, keep order
            if ris is not None:
                pt = ris.get("pt", "")
                exam = ris.get("exam", "")  
                pt_email_value = ris.get("pt_email_value", "")
                pt_phone_value = ris.get("pt_phone_value", "")
                pt_mobile_value = ris.get("pt_mobile_value", "")
                ref_doc = ris.get("ref_doc", "")
                ref_email_value = ris.get("ref_email_value", "")
                ref_phone_value = ris.get("ref_phone_value", "")
                ref_mobile_value = ris.get("ref_mobile_value", "")

            processed_case_info = {
                "case_id": case_id,
                "name": case.name, 
                "date": case_date, 
                "time": case_time,
                "has_pdf": has_pdf,
                "pdf_count": pdf_count,
                "has_images": has_images,
                "image_count": image_count,
                "has_single_dicom": has_single_dicom,
                "single_dicom_count": single_dicom_count,
                "has_multiple_dicom": has_multiple_dicom,
                "multiple_dicom_count": multiple_dicom_count,
                "dicom_2d_count": dicom_2d_count,
                "romexis": romexis,
                "has_project": has_project,
                "project_count": project_count,
                'is_staged': is_staged,
                "pt": pt, 
                "exam": exam, 
                "ref_doc": ref_doc,
                "ref_email_value": ref_email_value,
                "ref_phone_value": ref_phone_value,
                "ref_mobile_value": ref_mobile_value,
                "pt_email_value": pt_email_value,
                "pt_phone_value": pt_phone_value,
                "pt_mobile_value": pt_mobile_value,
                'case_labels': case_labels
                }
            processed_case_info_path = yesterday_staging_folder / case.name / f"{case.name}_details.json"
            self.write_case(processed_case_info, processed_case_info_path)

            processed_cases.append({
                "case_id": case_id,
                "name": case.name, 
                "date": case_date, 
                "time": case_time,
                "has_pdf": has_pdf,
                "pdf_count": pdf_count,
                "has_images": has_images,
                "image_count": image_count,
                "has_single_dicom": has_single_dicom,
                "single_dicom_count": single_dicom_count,
                "has_multiple_dicom": has_multiple_dicom,
                "multiple_dicom_count": multiple_dicom_count,
                "dicom_2d_count": dicom_2d_count,
                "romexis": romexis,
                "has_project": has_project,
                "project_count": project_count,
                'is_staged': is_staged,
                "pt": pt, 
                "exam": exam, 
                "ref_doc": ref_doc,
                "ref_email_value": ref_email_value,
                "ref_phone_value": ref_phone_value,
                "ref_mobile_value": ref_mobile_value,
                "pt_email_value": pt_email_value,
                "pt_phone_value": pt_phone_value,
                "pt_mobile_value": pt_mobile_value,
                'case_labels': case_labels
                })

        if processed_cases:
            # self._post_ui_log(f"Yesterday processing: {len(processed_cases)} case(s) processed", source="FolderMonitor")
            pass
        return len(processed_cases), processed_cases

    def get_cases_for_ui(self) -> dict:
        today_count, today_cases = self.find_cases()
        yesterday_count, yesterday_cases = self.find_yesterday_cases()
        
        return {
            "today": today_cases,
            "yesterday": yesterday_cases
        }

def _run_console_debug(poll_seconds: int = 5):
    stop_event = threading.Event()
    logger.info("Starting staging_logic console debug mode...")
    logger.info("Press Ctrl+C to stop.")

    try:
        monitor = StagingLogic.from_config()
        while not stop_event.is_set():
            try:
                monitor.ensure_today_folder()
                monitor.ensure_today_staging_folder()
                case_count, _ = monitor.find_cases()
                now = datetime.now().strftime("%d-%m-%Y %I.%M%p").lower()
                logger.info("%s - Found %s Cases", now, case_count)
            except Exception as exc:
                logger.exception("Error in staging_logic debug loop")

            if stop_event.wait(timeout=poll_seconds):
                break
    except KeyboardInterrupt:
        stop_event.set()
        logger.info("Keyboard interrupt received. Stopping staging_logic debug mode...")


if __name__ == "__main__":
    _run_console_debug()
