
import pytest
import time
import threading

from pynetdicom import AE, VerificationPresentationContexts
from pynetdicom.sop_class import (
    CTImageStorage,
    MRImageStorage,
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove,
    StudyRootQueryRetrieveInformationModelGet,
    PatientRootQueryRetrieveInformationModelFind,
    PatientRootQueryRetrieveInformationModelMove,
)
from utils_for_tests import *
import demo_pacs

def start_pacs():
    demo_pacs.start()
threading.Thread(target=start_pacs, daemon=True).start()

@pytest.fixture(scope="module")
def test_dicom():
    return create_test_dicom()


@pytest.fixture(scope="module")
def ae():
    """Create and return an Application Entity"""
    logging.info("Creating AE")
    ae = AE(ae_title=SCU_AET)
    ae.requested_contexts = VerificationPresentationContexts
    ae.add_requested_context(PatientRootQueryRetrieveInformationModelFind)
    ae.add_requested_context(PatientRootQueryRetrieveInformationModelMove)
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelGet)
    ae.add_requested_context(CTImageStorage)
    ae.add_requested_context(MRImageStorage)

    return ae

@pytest.fixture(scope="function")
def association(ae):
    """Establish and yield association for each test, release when done"""
    logging.info("Establishing association")
    assoc = ae.associate(
        HOST, 
        PORT, 
        ae_title=AET,
        max_pdu=16382,
    )
    
    if not assoc.is_established:
        pytest.skip(f"Could not establish association with SCP at {HOST}:{PORT}")
    
    yield assoc
    
    if assoc.is_established:
        assoc.release()
        time.sleep(0.1)  # Small delay for clean release