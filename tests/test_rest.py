import requests
import pytest
import urllib3
import logging

from utils_for_tests import *


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_json(url):
    logging.info(f"GET {url}")
    r = requests.get(url, verify=VERIFY_SSL)
    #r.raise_for_status()
    if r.status_code in [204, 404]:
        return None
    
    return r.json()

@pytest.mark.dependency(name="test_c_store")
def test_c_store(association, test_dicom):
    """Send the DICOM file via C-STORE"""
    logging.info("Testing C-STORE using pynetdicom")
    status = association.send_c_store(test_dicom)

    # Check if the status indicates success
    assert status.Status == 0x0000, f"C-STORE failed with status: {hex(status.Status)}"


@pytest.mark.dependency(depends=["test_c_store"])
def test_rest_api(test_dicom):
    # 1. Fetch /studies
    studies = get_json(f"{REST_ROOT}/studies")
    assert isinstance(studies, list)

    logging.warning(test_dicom)

    class MockDataset:
        """Mock dataset for testing anonymization"""
        pass
    mock_dataset = MockDataset()
    
    pseudo_study_uid = None
    # Try to match something unique in your data (here PatientID, tag 00100020)
    for st in studies:
        pt_id = st.get("00100020", {}).get("Value", [None])[0]
        if pt_id:
            pseudo_study_uid = st["0020000D"]["Value"][0]
            mock_dataset.PatientID = pt_id
            break
    assert pseudo_study_uid, "Pseudonymized study not found in /studies"
    mock_dataset.StudyInstanceUID = pseudo_study_uid

    # 2. Fetch series of the study and extract pseudonymized SeriesInstanceUID
    series_list = get_json(f"{REST_ROOT}/studies/{pseudo_study_uid}/series")
    assert isinstance(series_list, list) and len(series_list) > 0
    pseudo_series_uid = series_list[0]["0020000E"]["Value"][0]
    mock_dataset.SeriesInstanceUID = pseudo_series_uid


    assert check_anonymization(mock_dataset), "Received dataset is not anonymized"
    assert check_pseudonymization(mock_dataset, test_dicom), "Received dataset is not pseudonymized"


    # THIS INSTANCE ID IS TEMPORARY, TODO
    # unknown how the API handles this, as all IDs return the same values
    instance_id = 12345

    # 3. Fetch instances of that series and extract SOPInstanceUID
    get_json(f"{REST_ROOT}/studies/{pseudo_study_uid}/series/{pseudo_series_uid}/instances/{instance_id}")
    ds = received_datasets.get()

    assert check_anonymization(ds), "Received dataset is not anonymized"
    assert check_pseudonymization(ds, test_dicom), "Received dataset is not pseudonymized"