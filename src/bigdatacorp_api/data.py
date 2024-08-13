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
    BigDataCorpAPIInvalidDatabaseException,
    BigDataCorpAPIMaxRetryException,
    BigDataCorpAPIInvalidInputException,
    BigDataCorpAPILoginProblemException,
    BigDataCorpAPIProblemAPIException,
    BigDataCorpAPIOnDemandQueriesException,
    BigDataCorpAPIMonitoringAPIException,
    BigDataCorpAPIUnmappedErrorException)


class BigDataCorpAPI:
    CPF_DATABASES = [
        "government_debtors",
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

    CNPJ_DATABASES = [
        "partner_murabei_credit_score_company",
        "government_debtors",
        "syndicate_agreements",
        "investment_fund_data",
        'electoral_donors',
        'owners_electoral_donors',
        'company_evolution',
        'activity_indicators',
        'interests_and_behaviors',
        'licenses_and_authorizations',
        "financial_market",
        'electoral_providers',
        'industrial_property',
        'employees_industrial_property',
        'owners_industrial_property',
        'domains',
        'domains_extended',
        'emails_extended',
        'related_people_emails',
        'addresses_extended',
        'related_people_addresses',
        'phones_extended',
        'related_people_phones',
        'basic_data',
        'history_basic_data',
        'media_profile_and_exposure',
        'kyc_dtec_flex_news',
        'kyc',
        'economic_group_kyc',
        'employees_kyc',
        'owners_kyc',
        'online_ads',
        'marketplace_data',
        'apps_networks_and_platforms',
        'collections',
        'owners_lawsuits',
        'processes',
        'reputations_and_reviews',
        "social_conscience",
        "awards_and_certifications",
        'circles_employees',
        'circles_legal_representatives',
        'circles_first_level_owners',
        'economic_group_full_extended',
        'economic_group_first_level_extended',
        'economic_group_second_level_extended',
        'economic_group_third_level_extended',
        'company_group_household_activity',
        'company_group_rfcontact',
        'company_group_household',
        'company_group_tradename',
        'company_group_tradename_city',
        'company_group_building',
        'company_group_documentroot',
        'company_group_officialname',
        'company_group_legal_representative',
        'company_group_owners',
        'company_group_household_owners_surname',
        'relationships',
        'economic_group_relationships']

    def __init__(self, bigdata_auth_token: str):
        """
        __init__.

        Args:
            bigdata_auth_token [str]: Authentication token for BigData API.
        """
        self._bigdata_auth_token = bigdata_auth_token

    def list_cpf_dataset(self) -> list:
        """
        Return avaiable BigData CPF Datasets.

        Args:
            No Args
        Kwargs:
            No Kwargs
        Return:
            Return a list with avaiable datasets.
        """
        return self.CPF_DATABASES

    def list_cnpj_dataset(self) -> list:
        """
        Return avaiable BigData CNPJ Datasets.

        Args:
            No Args
        Kwargs:
            No Kwargs
        Return:
            Return a list with avaiable datasets.
        """
        return self.CNPJ_DATABASES

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

                login_entry = status_data.get("login")
                if login_entry is not None:
                    login_return = login_entry[0]
                    if login_return["Code"] == -101:
                        msg = "BigBoost user has expired"
                        raise BigDataCorpAPILoginProblemException(msg)

                # Check if the CPF has a match
                status = status_data[dataset][0]
                if status['Code'] == 0:
                    return response.json()

                elif status['Code'] >= -202 and status['Code'] <= -100:
                    raise BigDataCorpAPIInvalidInputException(
                        message="error related to input data",
                        payload={
                            'bigdata_status': status,
                            'cpf': cpf})

                elif status['Code'] >= -1002 and status['Code'] <= -1000:
                    raise BigDataCorpAPILoginProblemException(
                        message="error related to login problem",
                        payload={
                            'bigdata_status': status,
                            'cpf': cpf
                        })
                elif status['Code'] >= -2999 and status['Code'] <= -2000:
                    raise BigDataCorpAPIProblemAPIException(
                        message="error related to internal problems in APIs "
                                "or services",
                        payload={
                            'bigdata_status': status,
                            'cpf': cpf
                        })
                elif status['Code'] >= -1999 and status['Code'] <= -1200:
                    raise BigDataCorpAPIOnDemandQueriesException(
                        message="error related to on-demand queries",
                        payload={
                            'bigdata_status': status,
                            'cpf': cpf
                        })
                elif status['Code'] <= -3000:
                    raise BigDataCorpAPIMonitoringAPIException(
                        message="error related to problems in the Monitoring "
                                "API or Asynchronous Calls",
                        payload={
                            'bigdata_status': status,
                            'cpf': cpf
                        })
                else:
                    raise BigDataCorpAPIUnmappedErrorException(
                        message="unmapped error",
                        payload={
                            'bigdata_status': status,
                            'cpf': cpf
                        })

            # Raise if document is invalid
            except BigDataCorpAPIException as e:
                raise e

            # Raise if document is invalid
            except BigDataCorpAPILoginProblemException as e:
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

    def get_cnpj_dataset(self, cnpj: str, dataset: str) -> dict:
        """
        Call BigData API to fecth a database for a CNPJ.

        Retry for 5 times sleeping 1 second when errors are raised.

        Args:
            cnpj [str]: Company CNPJ.
            dataset [str]: Dataset on BigData that user should be fetched.
        Return [dict]:
            Information avaiable on BigData.
        Raise:
            BigDataCorpAPIException: Raise if errors in API occour.
        """
        if dataset not in self.CNPJ_DATABASES:
            msg = (
                "dataset [{dataset}] not avaiable on bigboost for CNPJ, "
                "avaiable datasets:\n{datasets}").format(
                dataset=dataset, datasets=", ".join(self.CNPJ_DATABASES))
            raise BigDataCorpAPIException(msg)

        url = "https://bigboost.bigdatacorp.com.br/companies"
        payload = {
            "Datasets": dataset,
            "q": "doc{" + cnpj + "}",
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

                login_entry = status_data.get("login")
                if login_entry is not None:
                    login_return = login_entry[0]
                    if login_return["Code"] == -101:
                        msg = "BigBoost user has expired"
                        raise BigDataCorpAPILoginProblemException(msg)

                # Check if the CNPJ has a match
                status = status_data[dataset][0]
                if status['Code'] == 0:
                    return response.json()
                elif status['Code'] >= -202 and status['Code'] <= -100:
                    raise BigDataCorpAPIInvalidInputException(
                        message="error related to input data",
                        payload={
                            'bigdata_status': status,
                            'cnpj': cnpj
                        })
                elif status['Code'] >= -1002 and status['Code'] <= -1000:
                    raise BigDataCorpAPILoginProblemException(
                        message="error related to login problem",
                        payload={
                            'bigdata_status': status,
                            'cnpj': cnpj
                        })
                elif status['Code'] >= -2999 and status['Code'] <= -2000:
                    raise BigDataCorpAPIProblemAPIException(
                        message="error related to internal problems in APIs "
                                "or services",
                        payload={
                            'bigdata_status': status,
                            'cnpj': cnpj
                        })
                elif status['Code'] >= -1999 and status['Code'] <= -1200:
                    raise BigDataCorpAPIOnDemandQueriesException(
                        message="error related to on-demand queries",
                        payload={
                            'bigdata_status': status,
                            'cnpj': cnpj
                        })
                elif status['Code'] <= -3000:
                    raise BigDataCorpAPIMonitoringAPIException(
                        message="error related to problems in the Monitoring "
                                "API or Asynchronous Calls",
                        payload={
                            'bigdata_status': status,
                            'cnpj': cnpj
                        })
                else:
                    raise BigDataCorpAPIUnmappedErrorException(
                        message="unmapped error",
                        payload={
                            'bigdata_status': status,
                            'cnpj': cnpj
                        })

            # Raise if document is invalid
            except BigDataCorpAPIException as e:
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

    def get_cnpj_datasets(self, cnpj: str, datasets: list,
                          verbosity: bool = False) -> dict:
        """
        Fetch a list of datasets and return a dictionary with all info.

        Args:
            cnpj [str]: Company cnpj.
            datasets [list[str]]: List of all datasets to be fetched.
        Kwargs:
            verbosity [bool]: If set true will print a msg for each dataset
                fetch.
        Returns [dict]:
            Return a dictionary with all dataset information, with keys
            corresponding to dataset name.
        """
        cnpj = cnpj.replace(".", "").replace("/", "").replace("-", "")
        response_dict = {}
        for db in datasets:
            if verbosity:
                print("Fetching dataset:", db)
            response_dict[db] = self.get_cnpj_dataset(
                cnpj=cnpj, dataset=db)
        return response_dict

    def get_usage(self, initial_date: str, final_date: str):
        """
        Retrieves usage data for a specified date range.

        Parameters:
        - initial_date (str): The initial date of the range in the format
            'yyyy-MM-dd'.
        - final_date (str): The final date of the range in the format
            'yyyy-MM-dd'.

        Returns:
        - results (list): A list of dictionaries containing the usage data for
            each API and endpoint.
          Each dictionary has the following keys:
            - 'api_type' (str): The type of API ('people' or 'companies').
            - 'end_point' (str): The endpoint of the API.
            - 'successful_requests' (int): The total number of successful
                requests made.
            - 'requests_with_error' (int): The total number of requests
                with errors.
            - 'queries_charged' (int): The total number of queries charged.
            - 'queries_not_charged' (int): The total number of queries
                not charged.
            - 'estimated_price' (float): The total estimated price
                for the usage.

        """
        results = []
        url = "https://plataforma.bigdatacorp.com.br/usage"

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "AccessToken": self._bigdata_auth_token,
        }

        payload = {
            "InitialReferenceDate": initial_date,
            "FinalReferenceDate": final_date,
            "DateFormat": "yyyy-MM-dd"
        }

        for api in self.CPF_DATABASES:
            payload["Api"] = "people"
            payload["Datasets"] = api
            try:
                response = requests.post(url, headers=headers, json=payload)
                if response.status_code == 500:
                    response.raise_for_status()

                if response.status_code != 200:
                    raise BigDataCorpAPIException(
                        response.json()['Status']['Message'])

                results.append({
                    'api_type': "people",
                    'end_point': api,
                    "successful_requests": response.json()["UsageData"]
                                            ["TotalSuccessfulRequests"],
                    "requests_with_error": response.json()["UsageData"]
                                            ["TotalRequestsWithError"],
                    "queries_charged": response.json()["UsageData"]
                                            ["TotalQueriesCharged"],
                    "queries_not_charged": response.json()["UsageData"]
                                            ["TotalQueriesNotCharged"],
                    "estimated_price": response.json()["UsageData"]
                                            ["TotalEstimatedPrice"],
                })

            except Exception as err:
                print(err)

        for api in self.CNPJ_DATABASES:
            payload["Api"] = "companies"
            payload["Datasets"] = api
            try:
                response = requests.post(url, headers=headers, json=payload)

                if response.status_code == 500:
                    response.raise_for_status()

                if response.status_code != 200:
                    raise BigDataCorpAPIException(
                        response.json()['Status']['Message'])

                results.append({
                    'api_type': "companies",
                    'end_point': api,
                    "successful_requests": response.json()["UsageData"]
                                            ["TotalSuccessfulRequests"],
                    "requests_with_error": response.json()["UsageData"]
                                            ["TotalRequestsWithError"],
                    "queries_charged": response.json()["UsageData"]
                                            ["TotalQueriesCharged"],
                    "queries_not_charged": response.json()["UsageData"]
                                            ["TotalQueriesNotCharged"],
                    "estimated_price": response.json()["UsageData"]
                                        ["TotalEstimatedPrice"],
                })

            except Exception as err:
                print(err)

        return results
