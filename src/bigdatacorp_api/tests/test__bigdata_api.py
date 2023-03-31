"""Test BigDataCorpAPI."""
import os
import unittest
from bigdatacorp_api.data import BigDataCorpAPI
from bigdatacorp_api.bigdata_api import (
    BigDataCorpAPIException, BigDataCorpAPIInvalidDocumentException,
    BigDataCorpAPIMaxRetryException)


BIGDATA_AUTH_TOKEN = os.environ.get("BIGDATA_AUTH_TOKEN")
TEST_CPF = os.environ.get("TEST_CPF")


class TestBigDataCorpAPI(unittest.TestCase):
    """Test process Calendar input data."""

    def test__ok(self):
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=CONFIG["BIGDATA_AUTH_TOKEN"])
        bigdata_api.get_cpf_dataset(
            cpf=TEST_CPF, dataset="basic_data")

    def test__invalid(self):
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=CONFIG["BIGDATA_AUTH_TOKEN"])
        with self.assertRaises(
                BigDataCorpAPIInvalidDocumentException) as context:
            bigdata_api.get_cpf_dataset(
                cpf=TEST_CPF + "0", dataset="basic_data")
        self.assertEqual(context.exception.message, 'cpf is invalid')
