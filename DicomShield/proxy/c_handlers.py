import logging
import time
from typing import Tuple

from pynetdicom.events import Event
from pynetdicom import AE, evt
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelGet,
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove,
    PatientRootQueryRetrieveInformationModelFind,
    PatientRootQueryRetrieveInformationModelMove,
    MRImageStorage,
    CTImageStorage
)

from pydicom import Dataset
import yaml

from utils import shared_queue, shield_anonymizer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)



with open("configs/config.yml") as f:
    config = yaml.safe_load(f)


retrieveMoveMap = {
    "STUDY": StudyRootQueryRetrieveInformationModelMove,
    "SERIES": StudyRootQueryRetrieveInformationModelMove,
    "INSTANCES": StudyRootQueryRetrieveInformationModelMove,
    "PATIENT": PatientRootQueryRetrieveInformationModelMove,
}

retrieveFindMap = {
    "STUDY": StudyRootQueryRetrieveInformationModelFind,
    "SERIES": StudyRootQueryRetrieveInformationModelFind,
    "PATIENT": PatientRootQueryRetrieveInformationModelFind,
}
		

def handle_store(event):
    """Callback function to handle and forward C-STORE."""
    ds = event.dataset
    ds.file_meta = event.file_meta

    # Perform anonymization
    anonymized_ds = shield_anonymizer.shield_store(ds)

    # Create association to forward anonymized dataset
    ae: AE = AE("DICOMSHIELD")
    ae.add_requested_context(MRImageStorage)
    ae.add_requested_context(CTImageStorage)

    try:
        assoc = ae.associate(config["UPSTREAM"]["IP"], config["UPSTREAM"]["IP"])
    except Exception as e:
        logging.error(f"Association with PACS failed: {e}")
        return 0xC000
    
    if assoc.is_established:
        # Send the anonymized data to the target PACS
        status = assoc.send_c_store(anonymized_ds)
        assoc.release()

        return status.Status  # Return the status of the C-STORE operation
    else:
        assoc.release()
        return 0xC000 # Failure



def handle_event(dataset: Dataset, event_context, action="FIND"):
    if 'QueryRetrieveLevel' not in dataset:
        raise Exception("QueryRetrieveLevel not valid")
    queryRetrieveLevel = dataset.QueryRetrieveLevel

    match action:
        case "FIND":
            queryRetrieveLevel = retrieveFindMap.get(queryRetrieveLevel)
        case "MOVE" | "MOVE_SCP":
            queryRetrieveLevel = retrieveMoveMap.get(queryRetrieveLevel)

    # Create an identical association for query retrieval
    ae: AE = AE("DICOMSHIELD")
    ae.add_requested_context(event_context.abstract_syntax, event_context.transfer_syntax)
    ae.add_requested_context(MRImageStorage)
    ae.add_requested_context(CTImageStorage)
    #ae.add_requested_context(StudyRootQueryRetrieveInformationModelGet)
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
    ae.add_requested_context(PatientRootQueryRetrieveInformationModelFind)
    ae.add_requested_context(PatientRootQueryRetrieveInformationModelMove)

    match action:
        case "FIND":
            association = ae.associate(config["UPSTREAM"]["IP"], config["UPSTREAM"]["PORT"], ae_title=config["UPSTREAM"].get("AET", "ANY-SCP"))
        case "MOVE":
            association = ae.associate("dicom-rst", 7001)
        case "MOVE_SCP":
            association = ae.associate("0.0.0.0", 11112)
        case _:
            raise Exception(f"Unknown action '{action}'!")
    

    if association.is_established:
        return association, queryRetrieveLevel
    else:
        return None
        

def handle_find(event: Event):
    """Callback function to handle and forward C-FIND."""
    identifier = event.identifier
    ae = handle_event(identifier, event.context)

    if ae is None:
        yield 0xC000, None # Failure
        return
    else:
        (assoc, queryRetrieveLevel) = ae

    logging.info("Handling C-FIND request")
    # First depseudonymize the identifier for internal querying
    identifier: Dataset = shield_anonymizer.shield_query(identifier)
    logging.info(f"Anonymized identifier for FIND: {identifier}")
    
    # Forward the C-FIND request and yield results
    responses = assoc.send_c_find(identifier, queryRetrieveLevel)
    
    # Then re-pseudonomize the identifier for data return
    for (status, identifier_resp) in responses:
        if identifier_resp is not None:
            identifier_resp = shield_anonymizer.shield_retrieve(identifier_resp)
        
        yield status, identifier_resp
    
    assoc.release()

    

def handle_move(event):
    logging.info("Handling C-MOVE request")
    handle_move_internally(event)
    received_items_cnt = shared_queue.qsize()
    logging.info(f"Received {received_items_cnt} datasets from internal MOVE SCP handler")

    #if received_items_cnt == 0:
    #    yield None, None

    source_ip = event.assoc.requestor.address
    source_port = event.assoc.requestor.port
    logging.info(f"handle_move-move-destination='{event.move_destination}' source_ip={source_ip}:{source_port}")

    target = config["ALLOWED_AET"][event.move_destination]
    target_ip, target_port = target

    logging.info(f"Forwarding {received_items_cnt} datasets to original client {target_ip}:{target_port}")
    # Forward received datasets to the original client
    yield target_ip, target_port
    yield received_items_cnt

    while shared_queue.qsize() > 0:
        yield 0xFF00, shared_queue.get()  # Pending status

    logging.info(f"Handling of C-MOVE request finished")
    yield 0x0000, None  # Success


def handle_move_internally(event):
    logging.info("Handling internal C-MOVE request")

    identifier = shield_anonymizer.shield_query(event.identifier)
    logging.info(f"Anonymized identifier '{event.identifier}' for MOVE: {identifier}")
    #logging.info(f"Event Context {event.context}")

    # Setup AE for move, request all required contexts
    result = handle_event(identifier, event.context, action="FIND")
    if result is None:
        logging.info("Failed to establish internal association for C-MOVE")
        return None # Failure
    else:
        (assoc, queryRetrieveLevel) = result

    # C-MOVE to our local AE (the running C-STORE-SCP server)
    responses = assoc.send_c_move(identifier, config["C_STORE_ENDPOINT"]["AET"], queryRetrieveLevel)
    logging.info(f"C-MOVE sent to SCP server: {assoc.dul.socket.socket.getpeername()}")

    for (status, ds) in responses:
        logging.warning(status)
        if status.Status != 0x0000:
            continue
        else:
            break

    #time.sleep(1)  # Allow time for the mock server to process the request

    assoc.release()
    return None


def handle_echo(event):
    """Callback function to handle and respond to C-ECHO."""
    return 0x0000  # Success
