
from pydicom.dataset import Dataset
import yaml

from pseudonym_clients import MIIClient, gPASClient

with open("configs/config.yml")as f:
    pseudonym_config = yaml.safe_load(f)["PSEUDONYMIZATION_SERVER"]


class Anonymizer:
    def __init__(self):
        client_version = pseudonym_config["CLIENT_TYPE"]
        match client_version:
            case "gPAS":
                self.pseudonym_client = gPASClient()
            case "MII":
                self.pseudonym_client = MIIClient()
            case _:
                raise Exception(f"No such CLIENT_TYPE={client_version} is supported!")
            
        # Fields that will be swapped by pseudonym value
        self.pseudonymize_fields = ['PatientID', 'StudyID', 'SOPInstanceUID', 'StudyInstanceUID', 'SeriesInstanceUID']

        # Fields that will be cleared
        self.anonymize_fields = [
            'PatientName',
            'IssuerOfPatientID',
            'PatientBirthDate',
            'PatientSex',
            'PatientAddress',
            'PatientTelephoneNumbers',
            'AccessionNumber',
            'InstitutionName',
            'InstitutionAddress',
            'InstitutionCodeSequence',
            'ReferringPhysicianName',
            'ReferringPhysicianTelephoneNumbers'
        ]



    def shield_query(self, dataset):
        dataset = self._anonymize(dataset)
        dataset = self._depseudonymize(dataset)
        return dataset
    
    def shield_retrieve(self, dataset):
        dataset = self._anonymize(dataset)
        dataset = self._pseudonymize(dataset)
        return dataset
    
    
    def shield_store(self, dataset):
        return dataset
    

    def _anonymize(self, dataset: Dataset):
        for field in self.anonymize_fields:
            if hasattr(dataset, field):
                setattr(dataset, field, '')

        dataset.ananomized = True

        return dataset

    def _pseudonymize(self, dataset: Dataset):
        to_pseudo_attrs = {}
        for attr in self.pseudonymize_fields:
            if attr in dataset:
                value = getattr(dataset, attr)
                if value == "": continue
                to_pseudo_attrs[attr] = value

        pseudo_attrs = self.pseudonym_client.pseudonomize(to_pseudo_attrs)

        for attr in to_pseudo_attrs:
            val = str(pseudo_attrs.get(to_pseudo_attrs[attr], None))
            setattr(dataset, attr, val)

        return dataset
    
    def _depseudonymize(self, dataset: Dataset):
        to_depseudo_attrs = {}
        for attr in self.pseudonymize_fields:
            if attr in dataset:
                value = getattr(dataset, attr)
                if value == "": continue
                to_depseudo_attrs[attr] = value

        depseudo_attrs = self.pseudonym_client.depseudonomize(to_depseudo_attrs)

        for attr in to_depseudo_attrs:
            val = str(depseudo_attrs.get(to_depseudo_attrs[attr], None))
            setattr(dataset, attr, val)

        return dataset