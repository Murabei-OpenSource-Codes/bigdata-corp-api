"""Integration tests for BigDataCorp CPF dataset fetches."""
import os
import unittest
from bigdatacorp_api.data import BigDataCorpAPI
from bigdatacorp_api.exceptions import (
    BigDataCorpAPIInvalidDocumentException)


BIGDATA_AUTH_TOKEN = os.environ.get("BIGDATA_AUTH_TOKEN")
TEST_CPF = os.environ.get("TEST_CPF")


class TestBigDataCorpCPFAPI(unittest.TestCase):
    """Tests for ``get_cpf_dataset`` CPF queries."""

    def test__ok(self) -> None:
        """Fetch ``basic_data`` for a valid CPF."""
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        bigdata_api.get_cpf_dataset(
            cpf=TEST_CPF, dataset="basic_data")

    def test__processes(self) -> None:
        """Fetch ``processes`` without lawsuit updates."""
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        process_data = bigdata_api.get_cpf_dataset(
            cpf=TEST_CPF, dataset="processes",
            query_params=", returnupdates{false}, partieslimit{0}",
            dataset_params="{NextPageId,Lawsuits.Number,Lawsuits.Type,Lawsuits.MainSubject,Lawsuits.CourtName,Lawsuits.CourtLevel,Lawsuits.CourtType,Lawsuits.CourtDistrict,Lawsuits.State}.limit(500)") # NOQA
        lawsuits = process_data['Result'][0]['Processes']['Lawsuits']
        lawsuits_data = lawsuits[0]
        self.assertNotIn("Updates", lawsuits_data.keys())

    def test__invalid(self) -> None:
        """Raise when the CPF document number is invalid."""
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        with self.assertRaises(
                BigDataCorpAPIInvalidDocumentException) as context:
            bigdata_api.get_cpf_dataset(
                cpf=TEST_CPF + "0", dataset="basic_data")
        self.assertEqual(
            context.exception.message, "cpf [{cpf}] não é válido")
