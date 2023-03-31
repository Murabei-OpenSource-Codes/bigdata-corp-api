"""BigDataCorp Python API."""
import os
import time
import json
import time
import datetime
import requests
from bigdatacorp_api.exceptions import (
    BigDataCorpAPIException, BigDataCorpAPIInvalidDocumentException,
    BigDataCorpAPIMinorDocumentException,
    BigDataCorpAPIMaxRetryException)



class BigDataCorpAPI:
    CPF_DATABASES = [
        "election_candidate_data",
        "circles_college_class",
        "circles_coworkers",
        "circles_household",
        "circles_relatives",
        "circles_lawsuit_parties",
        "circles_partners",
        "circles_neighbors",
        "circles_building",
        "basic_data",
        "occupation_data",
        "electoral_donors",
        "related_people_emails",
        "addresses_extended",
        "related_people_addresses",
        "media_profile_and_exposure",
        "company_group_employed",
        "company_group_family_ownership",
        "company_group_sued",
        "company_group_ownership",
        "historical_basic_data",
        "financial_data",
        "demographic_data",
        "licenses_and_authorizations",
        "life_stages",
        "collections",
        "electoral_providers",
        "indebtedness_question",
        "processes",
        "first_level_relatives_lawsuit_data",
        "business_relationships",
        "related_people",
        "phones_extended",
        "related_people_phones",
        "vehicles"]

    def __init__(self, bigdata_auth_token: str):
        """
        __init__.

        Args:
            bigdata_auth_token [str]: Authentication token for BigData API.
        """
        self._bigdata_auth_token = bigdata_auth_token

    def list_cpf_dataset(self) -> list:
        """
        Return avaiable BigData Datasets.

        Args:
            No Args
        Kwargs:
            No Kwargs
        Return:
            Return a list with avaiable datasets.
        """
        return self.CPF_DATABASES

    def get_cpf_dataset(self, cpf: str, dataset: str) -> dict:
        """
        Call BigData API to fecth a database for a CPF.

        Retry for 5 times sleeping 1 second when errors are raised.

        Args:
            cpf [str]: Person's CPF.
            dataset [str]: Dataset on BigData that user should be fetched.
        Return [dict]:
            Information avaiable on BigData.
        Raise:
            BigDataCorpAPIException: Raise if errors in API occour.
        """
        if dataset not in self.CPF_DATABASES:
            msg = (
                "dataset [{dataset}] not avaiable on bigboost for CPF, "
                "avaiable datasets:\n{datasets}").format(
                dataset=dataset, datasets=", ".join(self.CPF_DATABASES))
            raise BigDataCorpAPIException(msg)

        url = "https://bigboost.bigdatacorp.com.br/peoplev2"
        payload = {
            "Datasets": dataset,
            "q": "doc{" + cpf + "}",
            "Limit": 1}
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "AccessToken": self._bigdata_auth_token}

        error_msgs = []
        for i in range(5):
            try:
                response = requests.post(url, json=payload, headers=headers)
                response.raise_for_status()
                response_json = response.json()
                status_data = response_json['Status']

                # Treat minor validation error
                birth_validation = status_data.get('date_of_birth_validation')
                if birth_validation is not None:
                    raise BigDataCorpAPIMinorDocumentException(
                        message="this cpf belongs to a minor",
                        payload=birth_validation[0])

                # Check if the CPF has a match
                status = status_data[dataset][0]
                if status['Code'] == 0:
                    return response.json()
                else:
                    raise BigDataCorpAPIInvalidDocumentException(
                        message="cpf is invalid",
                        payload={
                            'bigdata_status': status,
                            'cpf': cpf
                        })

            # Raise if document is invalid
            except BigDataCorpAPIException as e:
                raise e

            # Raise if document is from an under age (age < 18)
            except BigDataCorpAPIMinorDocumentException as e:
                raise e

            except Exception as e:
                error_msgs.append(str(e))
                print("!!Error fetching BigData API:", str(e))

        msg = (
            "Untreated error on API with max 5 retries:{}\n".format(
                "\n".join(error_msgs)))
        raise BigDataCorpAPIMaxRetryException(
            message=msg, payload={"errors": error_msgs})

    def get_cpf_datasets(self, cpf: str, datasets: list,
                         verbosity: bool = False) -> dict:
        """
        Fetch a list of datasets and return a dictionary with all info.

        Args:
            cpf [str]: Person's CPF.
            datasets [list[str]]: List of all datasets to be fetched.
        Kwargs:
            verbosity [bool]: If set true will print a msg for each dataset
                fetch.
        Returns [dict]:
            Return a dictionary with all dataset information, with keys
            corresponding to dataset name.
        """

        response_dict = {}
        for db in datasets:
            if verbosity:
                print("Fetching dataset:", db)
            response_dict[db] = self.get_cpf_dataset(
                cpf=cpf, dataset=db)
        return response_dict
