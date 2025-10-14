import logging
from pydicom.uid import generate_uid, ExplicitVRLittleEndian
from pydicom.dataset import FileDataset, FileMetaDataset
from pynetdicom.sop_class import (
    CTImageStorage
)
from config import *
from queue import Queue

received_datasets = Queue()


def check_anonymization(dataset):
    """Check if the dataset is anonymized"""
    anonymized_fields = [
        "PatientName",
        "IssuerOfPatientID",
        "PatientBirthDate",
        "PatientSex",
        "PatientAddress",
        "PatientTelephoneNumbers",
        "AccessionNumber",
        "InstitutionName",
        "InstitutionAddress",
        "InstitutionCodeSequence",
        "ReferringPhysicianName",
        "ReferringPhysicianTelephoneNumbers"
    ]
    
    for field in anonymized_fields:
        if getattr(dataset, field, ""):
            logging.warning(f"Field {field} is not anonymized")
            return False
    return True


def check_pseudonymization(dataset, original_dataset):
    """Check if the dataset is pseudonymized"""
    pseudonymized_fields = [
        "PatientID",
        "StudyID",
        "SOPInstanceUID",
        "StudyInstanceUID",
        "SeriesInstanceUID"
    ]
    
    for field in pseudonymized_fields:
        if hasattr(dataset, field):
            if getattr(dataset, field) == getattr(original_dataset, field, ""):
                logging.warning(f"Field {field} is not pseudonymized")
                return False
    return True


def create_test_dicom():
    """Generate a basic DICOM file with identifiable patient data"""
    logging.info("Creating test DICOM file")

    filename = f"test.dcm"
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = CTImageStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid(entropy_srcs=["media_storage_sop_instance_uid"])
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian

    ds = FileDataset(filename, {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.PatientName = "John^Doe"
    ds.PatientID = "123456"
    ds.PatientBirthDate = "19900101"
    ds.Modality = "CT"
    ds.StudyInstanceUID = generate_uid(entropy_srcs=["study_instance_uid"])
    ds.SeriesInstanceUID = generate_uid(entropy_srcs=["series_instance_uid"])
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = CTImageStorage


    ds.is_little_endian = True
    ds.is_implicit_VR = False

    return ds