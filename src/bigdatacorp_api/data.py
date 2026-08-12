"""BigDataCorp REST API client.

Exposes ``BigDataCorpAPI`` for fetching CPF, CNPJ, and process datasets,
aggregating paginated responses, reporting usage, and downloading result
files from on-demand certificate endpoints.
"""
from __future__ import annotations

import requests
from loguru import logger

# Configs
from bigdatacorp_api.config import BIGDATACORP__REQUEST_TIMEOUT

# Local imports
from bigdatacorp_api.exceptions import (
    BigDataCorpAPIEmptyEnrichedProcessException,
    BigDataCorpAPIException,
    BigDataCorpAPIInvalidDatabaseException,
    BigDataCorpAPIInvalidDocumentException,
    BigDataCorpAPIInvalidInputException,
    BigDataCorpAPILoginProblemException,
    BigDataCorpAPIMaxRetryException,
    BigDataCorpAPIMinorDocumentException,
    BigDataCorpAPIMonitoringAPIException,
    BigDataCorpAPIOnDemandQueriesException,
    BigDataCorpAPIProblemAPIException,
    BigDataCorpAPIUnmappedErrorException,)


class BigDataCorpAPI:
    """Client for BigDataCorp REST API endpoints.

    Wraps authentication, dataset discovery, paginated fetches, usage
    reporting, and certificate file downloads behind typed methods with
    domain-specific exceptions.
    """

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
        "vehicles",
        "registration_data",
        "ondemand_pgfn_person",
        "ondemand_cert_debt_absence_by_state_person"]

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
        'economic_group_relationships',
        "registration_data",
        "ondemand_pgfn_company",
        "ondemand_cert_debt_absence_by_state_company"]

    MARKETPLACE_DATABASES = [
        "partner_murabei_credit_score_company"
    ]

    PROCESS_DATABASES = [
        'basic_data',
        'cade_processes_data'
    ]

    ONDEMAND_DATABASES = [
        "ondemand_pgfn_person",
        "ondemand_cert_debt_absence_by_state_person",
        "ondemand_pgfn_company",
        "ondemand_cert_debt_absence_by_state_company"]

    def __init__(self, bigdata_auth_token: str) -> None:
        """Initialize the API client.

        Args:
            bigdata_auth_token (str):
                BigDataCorp access token sent as the ``AccessToken`` header.
        """
        self._bigdata_auth_token = bigdata_auth_token

    def _send_request(self, url: str, payload: dict, headers: dict,
                      dataset: str, query_type: str, query_val: str) -> dict:
        """Sends a request to BigData API with retry logic and error handling.

        Args:
            url (str): Target API URL.
            payload (dict): Request payload.
            headers (dict): Request headers.
            dataset (str): Requested dataset name.
            query_type (str): Key for the query value in exception payloads.
                Options: 'cpf', 'cnpj', 'process_number'.
            query_val (str): The query identifier value.

        Returns:
            dict: The response JSON dictionary.

        Raises:
            BigDataCorpAPIMinorDocumentException: If the CPF is of a minor.
            BigDataCorpAPILoginProblemException: If login fails or expires.
            BigDataCorpAPIEmptyEnrichedProcessException: If process data is
                empty.
            BigDataCorpAPIInvalidInputException: If there's an input error.
            BigDataCorpAPIProblemAPIException: If there is an internal API
                problem.
            BigDataCorpAPIOnDemandQueriesException: If on-demand query fails.
            BigDataCorpAPIMonitoringAPIException: If monitoring API fails.
            BigDataCorpAPIUnmappedErrorException: For any other API error.
            BigDataCorpAPIMaxRetryException: If all retries fail.
        """
        error_msgs = []

        for i in range(5):
            try:
                response = requests.post(
                    url, json=payload, headers=headers,
                    timeout=BIGDATACORP__REQUEST_TIMEOUT)
                response.raise_for_status()
                response_json = response.json()
                status_data = response_json['Status']

                # Treat minor validation error (CPF only)
                birth_validation = status_data.get(
                    'date_of_birth_validation')

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

                # For process dataset, verify if there is any data
                status = status_data[dataset][0]
                if query_type == "process_number":
                    result_data = (
                        response_json.get('Result', [{}])[0]
                        .get('BasicData', {})
                    )
                    if status['Code'] == 0 and result_data:
                        return response_json
                    elif not result_data:
                        raise BigDataCorpAPIEmptyEnrichedProcessException(
                            message="no process data returned",
                            payload={
                                'bigdata_status': status,
                                'process_number': query_val,
                                'dataset': dataset})

                else:
                    if status['Code'] == 0:
                        return response_json

                payload_err = {
                    'bigdata_status': status,
                    query_type: query_val,
                    'dataset': dataset}

                if -202 <= status['Code'] <= -100:
                    raise BigDataCorpAPIInvalidInputException(
                        message="error related to input data",
                        payload=payload_err)

                elif -1002 <= status['Code'] <= -1000:
                    raise BigDataCorpAPILoginProblemException(
                        message="error related to login problem",
                        payload=payload_err)

                elif -2999 <= status['Code'] <= -2000:
                    raise BigDataCorpAPIProblemAPIException(
                        message="error related to internal problems in APIs "
                                "or services",
                        payload=payload_err)

                elif -1999 <= status['Code'] <= -1200:
                    raise BigDataCorpAPIOnDemandQueriesException(
                        message="error related to on-demand queries",
                        payload=payload_err)

                elif status['Code'] <= -3000:
                    raise BigDataCorpAPIMonitoringAPIException(
                        message="error related to problems in the Monitoring "
                                "API or Asynchronous Calls",
                        payload=payload_err)

                else:
                    raise BigDataCorpAPIUnmappedErrorException(
                        message="unmapped error",
                        payload=payload_err)

            except BigDataCorpAPIException as e:
                raise e

            except Exception as e:
                error_msgs.append(str(e))
                logger.info(
                    "Error fetching BigData API: {error}", error=str(e))

        msg = "Untreated error on API with max 5 retries:{}\n"\
            .format("\n".join(error_msgs))
        raise BigDataCorpAPIMaxRetryException(
            message=msg, payload={"errors": error_msgs})

    def _paginate(self, url: str, payload: dict, headers: dict, dataset: str,
                  query_type: str, query_val: str) -> dict:
        """Helper function to iterate through paginated responses.

        Args:
            url (str): Target API URL.
            payload (dict): Request payload.
            headers (dict): Request headers.
            dataset (str): Requested dataset name.
            query_type (str): Key for the query value in exception payloads.
                Options: 'cpf', 'cnpj', 'process_number'.
            query_val (str): The query identifier value.

        Returns:
            dict: The complete aggregated response dictionary.
        """
        first_page = self._send_request(
            url, payload, headers, dataset, query_type, query_val)

        result_list = first_page.get("Result", [])

        if not result_list:
            return first_page

        result_obj = result_list[0]
        dataset_key = "".join(word.capitalize() for word in dataset.split("_"))

        if dataset_key not in result_obj:
            # Fallback search
            for key, val in result_obj.items():
                if key.lower() != "matchkeys" and isinstance(val, dict):
                    dataset_key = key
                    break

        dataset_dict = result_obj.get(dataset_key)
        if not dataset_dict or not isinstance(dataset_dict, dict):
            return first_page

        while True:
            next_page_id = dataset_dict.get("NextPageId")

            if not next_page_id:
                break

            # Build payload for next page
            next_payload = payload.copy()
            next_payload["Datasets"] = f"{dataset}.next({next_page_id})"

            # Send request for next page
            next_page = self._send_request(
                url, next_payload, headers, dataset, query_type, query_val)

            # Get next dataset dict
            next_result_list = next_page.get("Result", [])
            if not next_result_list:
                break

            next_result_obj = next_result_list[0]
            next_dataset_dict = next_result_obj.get(dataset_key)
            if (not next_dataset_dict
                or not isinstance(next_dataset_dict, dict)):
                break

            # Merge lists from next_dataset_dict into dataset_dict
            for key, val in next_dataset_dict.items():
                if isinstance(val, list):
                    if (key in dataset_dict
                        and isinstance(dataset_dict[key], list)):
                        dataset_dict[key].extend(val)

                    else:
                        dataset_dict[key] = val

            # Update next page ID
            if "NextPageId" in next_dataset_dict:
                dataset_dict["NextPageId"] = next_dataset_dict["NextPageId"]
                logger.info(
                    "Next page ID: {next_page_id}",
                    next_page_id=next_dataset_dict["NextPageId"])

            else:
                dataset_dict.pop("NextPageId", None)

        return first_page

    def list_cpf_dataset(self) -> list[str]:
        """Return available BigData CPF dataset names.

        Returns:
            list[str]: Supported dataset identifiers for CPF queries.
        """
        return self.CPF_DATABASES

    def list_cnpj_dataset(self) -> list[str]:
        """Return available BigData CNPJ dataset names.

        Returns:
            list[str]: Supported dataset identifiers for CNPJ queries.
        """
        return self.CNPJ_DATABASES

    def list_process_dataset(self) -> list[str]:
        """Return available BigData process dataset names.

        Returns:
            list[str]: Supported dataset identifiers for process queries.
        """
        return self.PROCESS_DATABASES

    def get_cpf_dataset(self, cpf: str, dataset: str,
                        query_params: str = "",
                        request_parameters: dict = {}) -> dict:
        """Fetch a single CPF dataset from BigDataCorp.

        Retries up to five times on transient HTTP errors. Paginated
        responses are merged automatically.

        Args:
            cpf (str): Person CPF document number.
            dataset (str): Dataset name; must be in ``CPF_DATABASES``.
            query_params (str):
                Optional suffix appended to the ``q`` query string.

        Returns:
            dict: Raw JSON response from the BigDataCorp API.

        Raises:
            BigDataCorpAPIException:
                If ``dataset`` is not supported for CPF queries.
            BigDataCorpAPIMinorDocumentException:
                If the CPF belongs to a minor.
            BigDataCorpAPILoginProblemException:
                If authentication fails or the token expired.
            BigDataCorpAPIInvalidInputException:
                If the API reports an input validation error.
            BigDataCorpAPIProblemAPIException:
                If the API reports an internal service error.
            BigDataCorpAPIOnDemandQueriesException:
                If an on-demand query fails.
            BigDataCorpAPIMonitoringAPIException:
                If the monitoring or async API call fails.
            BigDataCorpAPIUnmappedErrorException:
                If the API returns an unmapped status code.
            BigDataCorpAPIMaxRetryException:
                If all retry attempts are exhausted.
        """
        if dataset not in self.CPF_DATABASES:
            msg = (
                "dataset [{dataset}] not avaiable on bigboost for CPF, "
                "avaiable datasets:\n{datasets}").format(
                dataset=dataset, datasets=", ".join(self.CPF_DATABASES))
            raise BigDataCorpAPIException(msg)

        if dataset in self.ONDEMAND_DATABASES:
            url = "https://plataforma.bigdatacorp.com.br/ondemand"
        else:
            url = "https://bigboost.bigdatacorp.com.br/peoplev2"

        payload = {
            "Datasets": dataset,
            "q": "doc{" + cpf + "}" + query_params,
            "Limit": 1}

        # Add custom request parameters if provided
        payload.update(request_parameters)

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "AccessToken": self._bigdata_auth_token}

        return self._paginate(
            url, payload, headers, dataset, "cpf", cpf)

    def get_cnpj_dataset(self, cnpj: str, dataset: str,
                         query_params: str = "",
                         request_parameters: dict = {}) -> dict:
        """Fetch a single CNPJ dataset from BigDataCorp.

        Retries up to five times on transient HTTP errors. Paginated
        responses are merged automatically.

        Args:
            cnpj (str): Company CNPJ document number.
            dataset (str): Dataset name; must be in ``CNPJ_DATABASES``.
            query_params (str):
                Optional suffix appended to the ``q`` query string.

        Returns:
            dict: Raw JSON response from the BigDataCorp API.

        Raises:
            BigDataCorpAPIException:
                If ``dataset`` is not supported for CNPJ queries.
            BigDataCorpAPILoginProblemException:
                If authentication fails or the token expired.
            BigDataCorpAPIInvalidInputException:
                If the API reports an input validation error.
            BigDataCorpAPIProblemAPIException:
                If the API reports an internal service error.
            BigDataCorpAPIOnDemandQueriesException:
                If an on-demand query fails.
            BigDataCorpAPIMonitoringAPIException:
                If the monitoring or async API call fails.
            BigDataCorpAPIUnmappedErrorException:
                If the API returns an unmapped status code.
            BigDataCorpAPIMaxRetryException:
                If all retry attempts are exhausted.
        """
        if dataset not in self.CNPJ_DATABASES:
            msg = (
                "dataset [{dataset}] not avaiable on bigboost for CNPJ, "
                "avaiable datasets:\n{datasets}").format(
                dataset=dataset, datasets=", ".join(self.CNPJ_DATABASES))
            raise BigDataCorpAPIException(msg)

        if dataset in self.MARKETPLACE_DATABASES:
            url = "https://plataforma.bigdatacorp.com.br/marketplace"
        elif dataset in self.ONDEMAND_DATABASES:
            url = "https://plataforma.bigdatacorp.com.br/ondemand"
        else:
            url = "https://bigboost.bigdatacorp.com.br/companies"

        payload = {
            "Datasets": dataset,
            "q": "doc{" + cnpj + "}" + query_params,
            "Limit": 1}

        # Add custom request parameters if provided
        payload.update(request_parameters)

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "AccessToken": self._bigdata_auth_token}

        return self._paginate(
            url, payload, headers, dataset, "cnpj", cnpj)

    def get_process_dataset(self, process: str, dataset: str,
                            request_parameters: dict = {}) -> dict:
        """Fetch a single process dataset from BigDataCorp.

        Retries up to five times on transient HTTP errors. Paginated
        responses are merged automatically.

        Args:
            process (str): Judicial process number.
            dataset (str): Dataset name; must be in ``PROCESS_DATABASES``.

        Returns:
            dict: Raw JSON response from the BigDataCorp API.

        Raises:
            BigDataCorpAPIException:
                If ``dataset`` is not supported for process queries.
            BigDataCorpAPIEmptyEnrichedProcessException:
                If the process query returns no enriched data.
            BigDataCorpAPILoginProblemException:
                If authentication fails or the token expired.
            BigDataCorpAPIInvalidInputException:
                If the API reports an input validation error.
            BigDataCorpAPIProblemAPIException:
                If the API reports an internal service error.
            BigDataCorpAPIOnDemandQueriesException:
                If an on-demand query fails.
            BigDataCorpAPIMonitoringAPIException:
                If the monitoring or async API call fails.
            BigDataCorpAPIUnmappedErrorException:
                If the API returns an unmapped status code.
            BigDataCorpAPIMaxRetryException:
                If all retry attempts are exhausted.
        """
        if dataset not in self.PROCESS_DATABASES:
            msg = (
                "dataset [{dataset}] not avaiable on bigboost for process, "
                "avaiable datasets:\n{datasets}").format(
                dataset=dataset, datasets=", ".join(self.PROCESS_DATABASES))
            raise BigDataCorpAPIException(msg)

        url = "https://plataforma.bigdatacorp.com.br/processos"

        payload = {
            "Datasets": dataset,
            "q": "processnumber{" + process + "}",
            "Limit": 1}

        # Add custom request parameters if provided
        payload.update(request_parameters)

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "AccessToken": self._bigdata_auth_token}

        return self._paginate(
            url, payload, headers, dataset, "process_number", process)

    def _validate_request_parameters(self, datasets: list[str],
                                     request_parameters: dict) -> None:
        """Validate the request parameters.

        Args:
            datasets (list[str]):
                Dataset names to fetch sequentially.
            request_parameters (dict):
                Optional request parameters to be added to the request
                payload.
        """
        parameters_keys_set = set(request_parameters.keys())
        datasets_keys_set = set(datasets)
        not_only_datasets_keys = parameters_keys_set - datasets_keys_set
        if not_only_datasets_keys:
            msg = (
                "The request parameters keys must correspond to the "
                "dataset names: {not_only_datasets_keys}").format(
                    not_only_datasets_keys=not_only_datasets_keys)
            raise ValueError(msg)

    def get_cpf_datasets(self, cpf: str, datasets: list[str],
                         verbosity: bool = False, query_params: str = "",
                         skip_errors: bool = False,
                         request_parameters: dict = None) -> dict[str, dict]:
        """Fetch multiple CPF datasets and return a keyed response dict.

        Args:
            cpf (str): Person CPF document number.
            datasets (list[str]): Dataset names to fetch sequentially.
            verbosity (bool):
                When True, logs progress for each dataset.
            query_params (str):
                Optional suffix appended to the ``q`` query string.
            skip_errors (bool):
                When True, skips errors and adds them to the response dict
                with the dataset name as the key.
                If False, raises an exception if an error occurs.
                Default is False.
            request_parameters (dict):
                Optional request parameters to be added to the request
                payload. Each key must correspond to a dataset name
                that is in the ``datasets`` list.

        Returns:
            dict[str, dict]:
                Mapping of dataset name to API response dictionary.

        Raises:
            BigDataCorpAPIException: See ``get_cpf_dataset`` for API errors.
        """
        if request_parameters is None:
            request_parameters = {}

        self._validate_request_parameters(
            datasets=datasets, request_parameters=request_parameters)

        response_dict = {}
        fetch_error = {}
        for db in datasets:
            if verbosity:
                logger.info("Fetching dataset: {dataset}", dataset=db)
            try:
                response_dict[db] = self.get_cpf_dataset(
                    cpf=cpf, dataset=db, query_params=query_params)
            except BigDataCorpAPIInvalidInputException as e:
                raise e

            except BigDataCorpAPILoginProblemException as e:
                raise e

            except Exception as e:
                if skip_errors:
                    fetch_error[db] = str(e)
                else:
                    raise e

        if fetch_error:
            response_dict['__errors__'] = fetch_error
        return response_dict

    def get_cnpj_datasets(self, cnpj: str, datasets: list[str],
                          verbosity: bool = False,
                          skip_errors: bool = False,
                          query_params: str = "",
                          request_parameters: dict = None) -> dict[str, dict]:
        """Fetch multiple CNPJ datasets and return a keyed response dict.

        Strips punctuation from ``cnpj`` before querying.

        Args:
            cnpj (str): Company CNPJ document number.
            datasets (list[str]): Dataset names to fetch sequentially.
            verbosity (bool):
                When True, logs progress for each dataset.
            query_params (str):
                Optional suffix appended to the ``q`` query string.

        Returns:
            dict[str, dict]:
                Mapping of dataset name to API response dictionary.

        Raises:
            BigDataCorpAPIException: See ``get_cnpj_dataset`` for API errors.
        """
        if request_parameters is None:
            request_parameters = {}

        self._validate_request_parameters(
            datasets=datasets, request_parameters=request_parameters)

        cnpj = cnpj.replace(".", "").replace("/", "").replace("-", "")
        response_dict = {}
        fetch_error = {}
        for db in datasets:
            if verbosity:
                logger.info("Fetching dataset: {dataset}", dataset=db)
            try:
                parameters = request_parameters.get(db, {})
                response_dict[db] = self.get_cnpj_dataset(
                    cnpj=cnpj, dataset=db, query_params=query_params,
                    request_parameters=parameters)
            except BigDataCorpAPIInvalidInputException as e:
                raise e

            except BigDataCorpAPILoginProblemException as e:
                raise e

            except Exception as e:
                if skip_errors:
                    fetch_error[db] = str(e)
                else:
                    raise e

        if fetch_error:
            response_dict['__errors__'] = fetch_error
        return response_dict

    def get_process_datasets(self, process: str, datasets: list[str],
                             verbosity: bool = False,
                             skip_errors: bool = False,
                             request_parameters: dict = None) -> dict[str, dict]:
        """Fetch multiple process datasets and return a keyed response dict.

        Strips punctuation from ``process`` before querying.

        Args:
            process (str):
                Judicial process number.
            datasets (list[str]):
                Dataset names to fetch sequentially.
            verbosity (bool):
                When True, logs progress for each dataset.
            skip_errors (bool):
                When True, skips errors and adds them to the response dict
                with the dataset name as the key.
                If False, raises an exception if an error occurs.
                Default is False.

        Returns:
            dict[str, dict]:
                Mapping of dataset name to API response dictionary.

        Raises:
            BigDataCorpAPIException:
                See ``get_process_dataset`` for API errors.
        """
        if request_parameters is None:
            request_parameters = {}

        self._validate_request_parameters(
            datasets=datasets, request_parameters=request_parameters)

        process = process.replace(".", "").replace("/", "").replace("-", "")
        response_dict = {}
        fetch_error = {}
        for db in datasets:
            if verbosity:
                logger.info("Fetching dataset: {dataset}", dataset=db)
            try:
                parameters = request_parameters.get(db, {})
                response_dict[db] = self.get_process_dataset(
                    process=process, dataset=db,
                    request_parameters=parameters)
            except Exception as e:
                if db in skip_errors:
                    fetch_error[db] = str(e)
                else:
                    raise e
        if fetch_error:
            response_dict['__errors__'] = fetch_error
        return response_dict

    def get_usage(
            self, initial_date: str, final_date: str) -> list[dict]:
        """Retrieve usage metrics for a date range across all datasets.

        Queries the usage endpoint once per CPF and CNPJ dataset. Failed
        requests are skipped silently and omitted from the result list.

        Args:
            initial_date (str):
                Range start date in ``yyyy-MM-dd`` format.
            final_date (str):
                Range end date in ``yyyy-MM-dd`` format.

        Returns:
            list[dict]:
                One entry per successful dataset query. Each dict contains
                ``api_type``, ``end_point``, ``successful_requests``,
                ``requests_with_error``, ``queries_charged``,
                ``queries_not_charged``, and ``estimated_price``.

        Raises:
            BigDataCorpAPIException:
                Raised inside the per-dataset loop but caught and logged;
                callers may receive a partial result list.
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
                logger.exception(
                    "Usage query failed for dataset {dataset}: {error}",
                    dataset=api, error=str(err))

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
                logger.exception(
                    "Usage query failed for dataset {dataset}: {error}",
                    dataset=api, error=str(err))

        return results

    def get_result_file(
            self, dataset: str, json_data: dict) -> dict[str, str | bytes]:
        """Download a certificate or result file from a prior API response.

        Args:
            dataset (str):
                Top-level key in ``json_data`` that holds the certificate
                payload.
            json_data (dict):
                Response dictionary returned by an on-demand dataset fetch.

        Returns:
            dict[str, str | bytes]:
                ``file_type`` (str) and ``file_content`` (bytes).

        Raises:
            BigDataCorpAPIException:
                If no file URL is found or the download request fails.
        """
        certificate_data = (json_data.get(dataset)
                            .get('Result')[0]
                            .get('OnlineCertificates')[0]
                            .get('AdditionalOutputData'))

        file_url = certificate_data.get("RawResultFile")
        file_type = certificate_data.get("RawResultFileType", "").lower()
        has_file = file_url is not None and file_url != ""

        if not has_file:
            raise BigDataCorpAPIException(
                message="No file URL found for provided data.",
                payload={"dataset": dataset,
                         "json_data": json_data})

        try:
            response = requests.get(
                file_url, headers={"accept": "*/*"}, timeout=30)
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            raise BigDataCorpAPIException(message=str(e))

        result = {'file_type': file_type,
                  'file_content': response.content}

        # Return raw content
        return result
