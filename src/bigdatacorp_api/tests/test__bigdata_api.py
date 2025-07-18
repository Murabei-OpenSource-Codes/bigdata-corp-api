"""Test BigDataCorpAPI."""
import os
import unittest
from bigdatacorp_api.data import BigDataCorpAPI
from bigdatacorp_api.exceptions import (
    BigDataCorpAPIException, BigDataCorpAPIInvalidInputException,
    BigDataCorpAPIMaxRetryException, BigDataCorpAPIEmptyEnrichedProcessException)


BIGDATA_AUTH_TOKEN = os.environ.get("BIGDATA_AUTH_TOKEN")
TEST_CPF = os.environ.get("TEST_CPF")
TEST_CNPJ = os.environ.get("TEST_CNPJ")
TEST_PROCESS = os.environ.get("TEST_PROCESS")


class TestBigDataCorpAPI(unittest.TestCase):
    """Test process Calendar input data."""

    def test__cpf_a_single_dataset(self):
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        bigdata_api.get_cpf_dataset(
            cpf=TEST_CPF, dataset="basic_data")

    def test__cpf_two_datasets(self):
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        bigdata_api.get_cpf_datasets(
            cpf=TEST_CPF, datasets=[
                "basic_data", "first_level_relatives_lawsuit_data"])

    def test__cpf_invalid(self):
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        with self.assertRaises(
                BigDataCorpAPIInvalidInputException) as context:
            bigdata_api.get_cpf_dataset(
                cpf=TEST_CPF + "0", dataset="basic_data")
        expected_error_msg = "error related to input data"
        self.assertEqual(context.exception.message, expected_error_msg)

    def test__cnpj_a_single_dataset(self):
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        bigdata_api.get_cnpj_dataset(
            cnpj=TEST_CNPJ, dataset="registration_data")

    def test__cnpj_two_datasets(self):
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        bigdata_api.get_cnpj_datasets(
            cnpj=TEST_CNPJ, datasets=[
                "registration_data", "relationships"])

    def test__cnpj_invalid(self):
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        with self.assertRaises(
                BigDataCorpAPIInvalidInputException) as context:
            bigdata_api.get_cnpj_dataset(
                cnpj=TEST_CNPJ + "0", dataset="registration_data")
        expected_error_msg = "error related to input data"
        self.assertEqual(context.exception.message, expected_error_msg)

    def test__process_a_single_dataset(self):
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        bigdata_api.get_process_dataset(
            process=TEST_PROCESS, dataset="basic_data")

    def test__process_invalid(self):
        bigdata_api = BigDataCorpAPI(
            bigdata_auth_token=BIGDATA_AUTH_TOKEN)
        with self.assertRaises(
                BigDataCorpAPIEmptyEnrichedProcessException) as context:
            bigdata_api.get_process_dataset(
                process="0", dataset="basic_data")
        expected_error_msg = "no process data returned"
        self.assertEqual(context.exception.message, expected_error_msg)
