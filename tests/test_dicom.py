import logging

import pytest
from pydicom.dataset import Dataset
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove,
    PatientRootQueryRetrieveInformationModelFind,
    PatientRootQueryRetrieveInformationModelMove,
)

from utils_for_tests import *


@pytest.fixture(scope="module")
def pseudonymized_ids():
    return {"study_uid": None, "patient_id": None}


def test_c_echo(association):
    """Send a C-ECHO request to the DICOM server"""
    logging.info("Testing C-ECHO using pynetdicom")
    status = association.send_c_echo()
    assert status.Status == 0x0000, f"C-ECHO failed with status: {hex(status.Status)}"


@pytest.mark.dependency(name="test_c_store")
def test_c_store(association, test_dicom):
    """Send the DICOM file via C-STORE"""
    logging.info("Testing C-STORE using pynetdicom")
    status = association.send_c_store(test_dicom)

    # Check if the status indicates success
    assert status.Status == 0x0000, f"C-STORE failed with status: {hex(status.Status)}"


@pytest.mark.dependency(depends=["test_c_store"], name="test_c_find_study_level")
def test_c_find_study_level(association, test_dicom, pseudonymized_ids):
    """Search for the DICOM file via C-FIND and check for anonymization"""
    logging.info("Testing C-FIND using pynetdicom")
    ds = Dataset()
    ds.StudyInstanceUID = ""
    ds.QueryRetrieveLevel = "STUDY"

    responses = association.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)

    for (status, identifier) in responses:
        if status and status.Status in (0xFF00, 0xFF01):
            # Check for anonymization
            assert check_anonymization(identifier), "StudyInstanceUID is not anonymized"
            assert check_pseudonymization(identifier, test_dicom), "StudyInstanceUID is not pseudonymized"
            pseudonymized_ids["study_uid"] = identifier.StudyInstanceUID
            return  # Stop after first valid result

    pytest.fail("No StudyInstanceUIDs found")


@pytest.mark.dependency(depends=["test_c_store"])
def test_c_find_specific_study(association, test_dicom):
    """Search for the DICOM file via C-FIND and check for anonymization"""
    logging.info("Testing C-FIND using pynetdicom")
    ds = Dataset()
    ds.StudyInstanceUID = test_dicom.StudyInstanceUID
    ds.QueryRetrieveLevel = "STUDY"

    responses = association.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)

    found = False
    for (status, identifier) in responses:
        if status and status.Status in (0xFF00, 0xFF01):
            found = True

    assert not found, "StudyInstanceUID found, although it should be anonymized"


@pytest.mark.dependency(depends=["test_c_store"], name="test_c_find_patient_level")
def test_c_find_patient_level(association, test_dicom, pseudonymized_ids):
    """Search for the DICOM file via C-FIND and check for anonymization"""
    logging.info("Testing C-FIND using pynetdicom")
    ds = Dataset()
    ds.PatientID = ""
    ds.QueryRetrieveLevel = "PATIENT"

    responses = association.send_c_find(ds, PatientRootQueryRetrieveInformationModelFind)

    found = False
    for (status, identifier) in responses:
        if status and status.Status in (0xFF00, 0xFF01):
            found = True

            # Check for anonymization
            assert check_anonymization(identifier), "StudyInstanceUID is not anonymized"
            assert check_pseudonymization(identifier, test_dicom), "StudyInstanceUID is not pseudonymized"
            pseudonymized_ids["patient_id"] = identifier.PatientID
            return  # Stop after first valid result

    pytest.fail("No PatientID found")


@pytest.mark.dependency(depends=["test_c_store"])
def test_c_find_specific_patient(association, test_dicom):
    """Search for the DICOM file via C-FIND and check for anonymization"""
    logging.info("Testing C-FIND using pynetdicom")
    ds = Dataset()
    ds.PatientID = test_dicom.PatientID
    ds.QueryRetrieveLevel = "PATIENT"

    responses = association.send_c_find(ds, PatientRootQueryRetrieveInformationModelFind)

    found = False
    for (status, identifier) in responses:
        if status and status.Status in (0xFF00, 0xFF01):
            found = True

    assert not found, "PatientID found, although it should be anonymized"


@pytest.mark.dependency(depends=["test_c_find_study_level"])
def test_c_move_with_mock_server_study_level(association, test_dicom, pseudonymized_ids):
    """Test the C-MOVE command using a mock server."""
    logging.info("Testing C-MOVE with mock server")
    study_uid = pseudonymized_ids["study_uid"]
    assert study_uid, "No pseudonymized StudyInstanceUID available"

    # Create query dataset
    ds = Dataset()
    ds.StudyInstanceUID = study_uid
    ds.QueryRetrieveLevel = "STUDY"

    responses = association.send_c_move(ds, MOCK_SERVER_AET, StudyRootQueryRetrieveInformationModelMove)

    found = False
    for (status, identifier) in responses:
        if status:
            if status.Status not in (0x0000, 0xFF00, 0xFF01):
                continue

            if status.Status == 0x0000:
                break
    
            logging.info(f"C-MOVE response status: {status.Status:#04x}")
            assert status.Status in (0x0000, 0xFF00, 0xFF01), \
                f"C-MOVE failed with status: {status.Status:#04x}"
            found = True

    assert found, "C-MOVE response status is None"
    assert received_datasets.qsize() > 0, "No datasets received by the mock server"
    assert check_anonymization(received_datasets.get()), "Received dataset is not anonymized"


@pytest.mark.dependency(depends=["test_c_find_patient_level"])
def test_c_move_with_mock_server_patient_level(association, test_dicom, pseudonymized_ids):
    """Test the C-MOVE command using a mock server."""
    logging.info("Testing C-MOVE with mock server")
    patient_id = pseudonymized_ids["patient_id"]
    assert patient_id, "No pseudonymized PatientID available"


    # Create query dataset
    ds = Dataset()
    ds.PatientID = patient_id
    ds.QueryRetrieveLevel = "PATIENT"

    responses = association.send_c_move(ds, MOCK_SERVER_AET, PatientRootQueryRetrieveInformationModelMove)

    found = False
    for (status, identifier) in responses:
        
        if status:
            if status.Status not in (0x0000, 0xFF00, 0xFF01):
                continue
                
            logging.info(f"C-MOVE response status: {status.Status:#04x}")
            assert status.Status in (0x0000, 0xFF00, 0xFF01), \
                f"C-MOVE failed with status: {status.Status:#04x}"
            found = True

    
    _received_datasets = []
    while received_datasets.qsize() > 0:
        _received_datasets.append(received_datasets.get())
        logging.info(f"Received dataset with PatientID: {_received_datasets[-1].PatientID}")

    assert found, "C-MOVE response status is None"
    assert len(_received_datasets) > 0, "No datasets received by the mock server"
    assert check_anonymization(_received_datasets[0]), "Received dataset is not anonymized"

