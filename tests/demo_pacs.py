from pynetdicom import AE, evt, AllStoragePresentationContexts
from pynetdicom.sop_class import (
    CTImageStorage,
    MRImageStorage,
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove,
    StudyRootQueryRetrieveInformationModelGet,
    PatientRootQueryRetrieveInformationModelFind,
    PatientRootQueryRetrieveInformationModelMove,
)
import logging
import yaml
from utils_for_tests import received_datasets

with open("../DicomShield/proxy/configs/config.yml") as f:
    config = yaml.safe_load(f)


def handle_store(event):
    """Handle incoming C-STORE requests."""
    ds = event.dataset
    ds.file_meta = event.file_meta
    received_datasets.put(ds)
    logging.warning(f"Received C-STORE request for SOP Instance UID: {ds.SOPInstanceUID}")
    return 0x0000  # Success status


def start():
    ae = AE(ae_title="CLIENT-PACS")
    for context in AllStoragePresentationContexts:
        ae.add_supported_context(context.abstract_syntax)
    ae.add_supported_context(CTImageStorage)
    ae.add_supported_context(MRImageStorage)
    ae.add_supported_context(PatientRootQueryRetrieveInformationModelFind)
    ae.add_supported_context(PatientRootQueryRetrieveInformationModelMove)
    ae.add_supported_context(StudyRootQueryRetrieveInformationModelFind)
    ae.add_supported_context(StudyRootQueryRetrieveInformationModelMove)
    ae.add_supported_context(StudyRootQueryRetrieveInformationModelGet)
    handlers = [(evt.EVT_C_STORE, handle_store)]
    ae.start_server(("0.0.0.0", config["TARGET_PACS_PORT"]), block=True, evt_handlers=handlers)