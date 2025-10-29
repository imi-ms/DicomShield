import logging
import threading
from pynetdicom import AE, evt, StoragePresentationContexts, AllStoragePresentationContexts
from pynetdicom.sop_class import (
    Verification,
    CTImageStorage,
    XRayAngiographicImageStorage,
    MRImageStorage,
    StudyRootQueryRetrieveInformationModelGet,
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove,
    PatientRootQueryRetrieveInformationModelFind,
    PatientRootQueryRetrieveInformationModelMove,
    Verification
)

from c_handlers import *
from utils import shared_queue, shield_anonymizer

# Configure logging
logging.basicConfig(level=logging.WARNING)


def run_ae_server():
    # Initialize the Application Entity
    local_port = config["INGRESS"]["PORT"]
    ae_title = config["INGRESS"]["AET"]
    logging.info(f"Starting DicomShield with AE Title='{ae_title}' at port {local_port}...")

    ae = AE(ae_title=ae_title)

    # Add all necessary SOP Classes (associations this SCU/SCP will accept)

    for context in AllStoragePresentationContexts:
        ae.add_supported_context(context.abstract_syntax, scu_role=True, scp_role=True)

    ae.add_supported_context(StudyRootQueryRetrieveInformationModelGet)
    ae.add_supported_context(StudyRootQueryRetrieveInformationModelFind)
    ae.add_supported_context(PatientRootQueryRetrieveInformationModelFind)
    ae.add_supported_context(StudyRootQueryRetrieveInformationModelMove)
    ae.add_supported_context(PatientRootQueryRetrieveInformationModelMove)

    # ae.requested_contexts = StoragePresentationContexts

    # Define handlers for DICOM events
    handlers = [
        (evt.EVT_ESTABLISHED, handle_established),
        (evt.EVT_C_STORE, handle_store),
        (evt.EVT_C_FIND, handle_find),
        (evt.EVT_C_GET, handle_get),
        (evt.EVT_C_MOVE, handle_move),
        (evt.EVT_C_ECHO, handle_echo),
    ]

    ae.start_server(('0.0.0.0', local_port), evt_handlers=handlers, block=True)


def handle_established(evt):
    # for context in evt.assoc.accepted_contexts:
    #     print(f"Accepted: {context.abstract_syntax} with {context.transfer_syntax}")
    pass  # uncomment for debugging


def run_internal_server():
    """Since C-MOVE triggers a C-STORE, we need to redirect the C-STORE to us, pseudonymize and send the result back"""
    local_ae = config["C_STORE_ENDPOINT"]["AET"]
    local_port = config["C_STORE_ENDPOINT"]["PORT"]

    # 1. Define the C-STORE SCP callback that anonymizes and forwards
    def proxy_store(internal_event):
        logging.info(f"proxy-store(...) was called: {internal_event}")
        ds = internal_event.dataset
        ds.file_meta = internal_event.file_meta

        # Anonymize
        ds = shield_anonymizer.shield_retrieve(ds)

        shared_queue.put(ds)
        logging.info(f"dataset was put in the queue {internal_event}")
        return 0x0000

    handlers = [(evt.EVT_C_STORE, proxy_store), (evt.EVT_C_ECHO, handle_echo)]
    ae = AE(ae_title=local_ae)

    for context in AllStoragePresentationContexts:
        ae.add_supported_context(context.abstract_syntax)

    ae.add_supported_context(Verification)

    ae = ae.start_server(('0.0.0.0', local_port), block=False, evt_handlers=handlers)

    # server = threading.Thread(target=ae.start_server, args=(('0.0.0.0', local_port),), kwargs={'block': True, 'evt_handlers': handlers})
    # server.start()
    logging.info(f"Started C-STORE SCP server on AE title '{local_ae}' at port {local_port}")
    return ae


def verify_proxy_connection():
    ip, port = config["UPSTREAM"]["IP"], config["UPSTREAM"]["PORT"]
    logging.info(f"Testing proxy target ({ip}:{port}) using C-ECHO...")
    ae = AE(ae_title="DICOMSHIELD")
    ae.add_requested_context(Verification)
    assoc = ae.associate(ip, port, ae_title=config["UPSTREAM"].get("AET", "ANY-SCP"))
    if assoc.is_established:
        status = assoc.send_c_echo()
        logging.info(f"C-ECHO response status: {hex(status.Status)}")
        assert status.Status == 0x0000, f"C-ECHO failed with status: {hex(status.Status)}"
        assoc.release()
    else:
        logging.info(f"Association to target could not be established.")
        exit(1)


if __name__ == '__main__':
    print("""   _ _                   _   _     _   _ 
 _| |_|___ ___ _____ ___| |_|_|___| |_| |
| . | |  _| . |     |_ -|   | | -_| | . |
|___|_|___|___|_|_|_|___|_|_|_|___|_|___|""")

    # test upstream connection
    verify_proxy_connection()

    # test gPAS-connection
    shield_anonymizer.pseudonym_client.test_connection()

    forward_ae = run_internal_server()
    run_ae_server()
