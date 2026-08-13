"""Integration tests for BigDataCorp CPF dataset fetches."""
import os
import unittest
from bigdatacorp_api.data import BigDataCorpAPI
from bigdatacorp_api.exceptions import (
    BigDataCorpAPIException, BigDataCorpAPIInvalidDocumentException,
    BigDataCorpAPIMaxRetryException)


BIGDATA_AUTH_TOKEN = os.environ.get("BIGDATA_AUTH_TOKEN")
TEST_CPF = os.environ.get("TEST_CPF")
TEST_CNPJ = os.environ.get("TEST_CNPJ")


class TestBigDataCorpCPFAPI(unittest.TestCase):
    """Tests for ``get_cpf_dataset`` CPF queries."""

    def test__ok(self) -> None:
        """Fetch ``basic_data`` for a valid CPF."""
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        bigdata_api.get_cpf_dataset(
            cpf=TEST_CPF, dataset="basic_data")

    def test__invalid(self) -> None:
        """Raise when the CPF document number is invalid."""
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        with self.assertRaises(
                BigDataCorpAPIInvalidDocumentException) as context:
            bigdata_api.get_cpf_dataset(
                cpf=TEST_CPF + "0", dataset="basic_data")
        self.assertEqual(context.exception.message, 'cpf is invalid')
