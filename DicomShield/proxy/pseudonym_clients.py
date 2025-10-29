import logging
import requests
from requests.auth import HTTPBasicAuth
import yaml

from xml.etree import ElementTree
import xmltodict

with open("configs/config.yml") as f:
    pseudonym_config = yaml.safe_load(f)["PSEUDONYMIZATION_SERVER"]


class PseudonymClient:
    def __init__(self):
        self.base_url = pseudonym_config["ENDPOINT_URL"]
        self.domain = pseudonym_config["DOMAIN"]

        self.auth = None if pseudonym_config["USER"] is None else (pseudonym_config["USER"],
                                                                   pseudonym_config["PASSWORD"])

    class PseudonymMapper:
        def __init__(self, xml):
            self.ns = {"f": "http://hl7.org/fhir"}
            self.tree = xml
            self.mapping = self._extract_mappings()

        def _extract_mappings(self):
            result = []
            for param in self.tree.findall('f:parameter', self.ns):
                orig = None
                pseudonym = None
                for part in param.findall('f:part', self.ns):
                    name = part.find('f:name', self.ns).get('value')
                    if name == "original":
                        orig = part.find('f:valueIdentifier/f:value', self.ns).get('value')
                    elif name == "pseudonym":
                        pseudonym = part.find('f:valueIdentifier/f:value', self.ns).get('value')
                if orig and pseudonym:
                    result.append((orig, pseudonym))
            return result

        def make_pseudonym_map(self):
            """Returns {original: pseudonym}"""
            return {orig: p for orig, p in self.mapping}

        def make_original_map(self):
            """Returns {pseudonym: original}"""
            return {p: orig for orig, p in self.mapping}

    def get(self, endpoint):
        url = f"{self.base_url}/{endpoint}"

        try:
            response = requests.get(url, auth=self.auth)
            response.raise_for_status()
            return xmltodict.parse(ElementTree.fromstring(response.content))
        except Exception as e:
            logging.exception(f"An error occurred: {e}")
            return None

    def test_connection(self):
        logging.info(f"Testing connection to PSEUDONYMIZATION_SERVER='{self.base_url}'")
        url = f"{self.base_url}/metadata"
        response = requests.get(url, auth=self.auth)
        response.raise_for_status()

    def post(self, endpoint, data=None):
        url = f"{self.base_url}/{endpoint}"

        try:
            response = requests.post(url, data=data, headers={'Content-Type': 'application/fhir+xml'}, auth=self.auth)
            response.raise_for_status()
            return ElementTree.fromstring(response.content)
        except Exception as e:
            logging.exception(f"An error occurred: {e}")
            return None


class MIIClient(PseudonymClient):
    def __init__(self):
        super().__init__()

    def pseudonomize(self, identifier: dict):
        if identifier == {} or identifier is None:
            return {}

        fhir_body_parameters = []
        for value in identifier.values():
            fhir_body_parameters.append(
                f"""
                <parameter>
                    <name value="original" />
                    <valueString value="{value}" />
                </parameter>
                """
            )

        fhir_body = f"""
            <Parameters xmlns="http://hl7.org/fhir">
                <id value="Pseudonymization-DicomShield" />
                <parameter>
                    <name value="target" />
                    <valueString value="{self.domain}" />
                </parameter>
                <parameter>
                    <name value="allowCreate" />
                    <valueString value="true" />
                </parameter>
                {"".join([param for param in fhir_body_parameters])}
            </Parameters>
        """

        values = self.post(endpoint="$pseudonymize", data=fhir_body)
        pseudonyms = self.PseudonymMapper(values).make_pseudonym_map()
        return pseudonyms

    def depseudonomize(self, identifier: dict):
        if identifier == {} or identifier is None:
            return {}

        fhir_body_parameters = []
        for value in identifier.values():
            fhir_body_parameters.append(
                f"""
                <parameter>
                    <name value="pseudonym" />
                    <valueString value="{value}" />
                </parameter>
                """
            )

        fhir_body = f"""
            <Parameters xmlns="http://hl7.org/fhir">
                <id value="Pseudonymization-DicomShield" />
                <parameter>
                    <name value="target" />
                    <valueString value="{self.domain}" />
                </parameter>
                {"".join([param for param in fhir_body_parameters])}
            </Parameters>
        """

        values = self.post(endpoint="$de-pseudonymize", data=fhir_body)
        originals = self.PseudonymMapper(values).make_original_map()
        return originals


class gPASClient(PseudonymClient):
    def __init__(self):
        super().__init__()

    def pseudonomize(self, identifier: dict):
        if identifier == {} or identifier is None:
            return {}

        fhir_body_parameters = []
        for value in identifier.values():
            fhir_body_parameters.append(
                f"""
                <parameter>
                    <name value="original" />
                    <valueString value="{value}" />
                </parameter>
                """
            )

        fhir_body = f"""
            <Parameters xmlns="http://hl7.org/fhir">
                <id value="Pseudonymization-DicomShield" />
                <parameter>
                    <name value="target" />
                    <valueString value="{self.domain}" />
                </parameter>
                {"".join([param for param in fhir_body_parameters])}
            </Parameters>
        """

        values = self.post(endpoint="$pseudonymizeAllowCreate", data=fhir_body)
        pseudonyms = self.PseudonymMapper(values).make_pseudonym_map()
        return pseudonyms

    def depseudonomize(self, identifier: dict):
        if identifier == {} or identifier is None:
            return {}

        fhir_body_parameters = []
        for value in identifier.values():
            fhir_body_parameters.append(
                f"""
                <parameter>
                    <name value="pseudonym" />
                    <valueString value="{value}" />
                </parameter>
                """
            )

        fhir_body = f"""
            <Parameters xmlns="http://hl7.org/fhir">
                <id value="Pseudonymization-DicomShield" />
                <parameter>
                    <name value="target" />
                    <valueString value="{self.domain}" />
                </parameter>
                {"".join([param for param in fhir_body_parameters])}
            </Parameters>
        """

        values = self.post(endpoint="$dePseudonymize", data=fhir_body)
        originals = self.PseudonymMapper(values).make_original_map()
        return originals
