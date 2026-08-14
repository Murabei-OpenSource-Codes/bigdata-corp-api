"""Integration tests for BigDataCorp CNPJ dataset fetches."""
import os
import unittest
from bigdatacorp_api.data import BigDataCorpAPI
from bigdatacorp_api.exceptions import (
    BigDataCorpAPIException, BigDataCorpAPIInvalidDocumentException,
    BigDataCorpAPIMaxRetryException)


BIGDATA_AUTH_TOKEN = os.environ.get("BIGDATA_AUTH_TOKEN")
TEST_CPF = os.environ.get("TEST_CPF")
TEST_CNPJ = os.environ.get("TEST_CNPJ")


class TestBigDataCorpCNPJAPI(unittest.TestCase):
    """Tests for ``get_cnpj_dataset`` CNPJ queries."""

    def test__process(self) -> None:
        """Fetch processes without Updates, with a field-limited suffix."""
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        process_data = bigdata_api.get_cnpj_dataset(
            cnpj=TEST_CNPJ,
            dataset="processes",
            query_params=", returnupdates{false}, partieslimit{0}",
            dataset_params="{NextPageId,Lawsuits.Number,Lawsuits.Type,Lawsuits.MainSubject,Lawsuits.CourtName,Lawsuits.CourtLevel,Lawsuits.CourtType,Lawsuits.CourtDistrict,Lawsuits.State}.limit(500)") # NOQA

        result = process_data['Result'][0]
        lawsuits = result['Lawsuits']
        lawsuits_entry = lawsuits['Lawsuits']
        lawsuits_data = lawsuits_entry[0]
        self.assertNotIn("Updates", lawsuits_data.keys())