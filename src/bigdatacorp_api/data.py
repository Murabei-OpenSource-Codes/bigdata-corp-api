"""BigDataCorp REST API client.

Exposes ``BigDataCorpAPI`` for fetching CPF, CNPJ, and process datasets,
aggregating paginated responses, reporting usage, and downloading result
files from on-demand certificate endpoints.
"""
from __future__ import annotations

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable
from loguru import logger

# Configs
from bigdatacorp_api.config import (
    BIGDATACORP__N_PARALLEL, BIGDATACORP__REQUEST_TIMEOUT)

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

    CPF_DATABASES: list[str] = [
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
    """Supported dataset identifiers for CPF queries."""

    CNPJ_DATABASES: list[str] = [
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
    """Supported dataset identifiers for CNPJ queries."""

    MARKETPLACE_DATABASES: list[str] = [
        "partner_murabei_credit_score_company"
    ]
    """CNPJ datasets routed to the marketplace endpoint."""

    PROCESS_DATABASES: list[str] = [
        'basic_data',
        'cade_processes_data'
    ]
    """Supported dataset identifiers for process queries."""

    ONDEMAND_DATABASES: list[str] = [
        "ondemand_pgfn_person",
        "ondemand_cert_debt_absence_by_state_person",
        "ondemand_pgfn_company",
        "ondemand_cert_debt_absence_by_state_company"]
    """Datasets routed to the on-demand certificate endpoint."""

    def __init__(self, bigdata_auth_token: str) -> None:
        """Initialize the API client.

        Args:
            bigdata_auth_token (str):
                BigDataCorp access token sent as the ``AccessToken`` header.
        """
        self._bigdata_auth_token = bigdata_auth_token

    def _send_request(self, url: str, payload: dict, headers: dict,
                      dataset: str, query_type: str, query_val: str) -> dict:
        """Send a POST request to BigDataCorp with retries.

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
                  query_type: str, query_val: str,
                  dataset_params: str = "") -> dict:
        """Iterate paginated responses and merge list fields.

        Args:
            url (str): Target API URL.
            payload (dict): Request payload.
            headers (dict): Request headers.
            dataset (str): Requested dataset name.
            query_type (str): Key for the query value in exception payloads.
                Options: 'cpf', 'cnpj', 'process_number'.
            query_val (str): The query identifier value.
            dataset_params (str):
                Suffix appended to ``dataset`` on each page request,
                including ``.next(...)`` calls.

        Returns:
            dict: The complete aggregated response dictionary.

        Raises:
            BigDataCorpAPIException:
                Same errors as ``_send_request`` for each page fetch.
        """
        first_page = self._send_request(
            url=url, payload=payload, headers=headers, dataset=dataset,
            query_type=query_type, query_val=query_val)

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
            next_dataset = "{dataset}{dataset_params}.next({next_page_id})"\
                .format(dataset=dataset, dataset_params=dataset_params,
                        next_page_id=next_page_id)
            logger.info(
                "Next dataset: {next_dataset}",
                next_dataset=next_dataset)
            next_payload["Datasets"] = next_dataset

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
                        dataset_params: str = "") -> dict:
        """Fetch a single CPF dataset from BigDataCorp.

        Retries up to five times on transient HTTP errors. Paginated
        responses are merged automatically.

        Args:
            cpf (str): Person CPF document number.
            dataset (str): Dataset name; must be in ``CPF_DATABASES``.
            query_params (str):
                Optional suffix appended to the ``q`` query string.
            dataset_params (str):
                Optional suffix appended to ``dataset`` in ``Datasets``.
                Examples: ``.limit(10)`` or
                ``{NextPageId}.limit(500)``.

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
            "Datasets": dataset + dataset_params,
            "q": "doc{" + cpf + "}" + query_params,
            "Limit": 1}

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "AccessToken": self._bigdata_auth_token}

        return self._paginate(
            url=url, payload=payload, headers=headers, dataset=dataset,
            query_type="cpf", query_val=cpf,
            dataset_params=dataset_params)

    def get_cnpj_dataset(self, cnpj: str, dataset: str,
                         query_params: str = "",
                         dataset_params: str = "") -> dict:
        """Fetch a single CNPJ dataset from BigDataCorp.

        Retries up to five times on transient HTTP errors. Paginated
        responses are merged automatically.

        Args:
            cnpj (str): Company CNPJ document number.
            dataset (str): Dataset name; must be in ``CNPJ_DATABASES``.
            query_params (str):
                Optional suffix appended to the ``q`` query string.
            dataset_params (str):
                Optional suffix appended to ``dataset`` in ``Datasets``.
                Examples: ``.limit(10)`` or
                ``{NextPageId}.limit(500)``.

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
            "Datasets": dataset + dataset_params,
            "q": "doc{" + cnpj + "}" + query_params,
            "Limit": 1}

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "AccessToken": self._bigdata_auth_token}
        return self._paginate(
            url=url, payload=payload, headers=headers, dataset=dataset,
            query_type="cnpj", query_val=cnpj, dataset_params=dataset_params)

    def get_process_dataset(self, process: str, dataset: str,
                            query_params: str = "",
                            dataset_params: str = "") -> dict:
        """Fetch a single process dataset from BigDataCorp.

        Retries up to five times on transient HTTP errors. Paginated
        responses are merged automatically.

        Args:
            process (str): Judicial process number.
            dataset (str): Dataset name; must be in ``PROCESS_DATABASES``.
            query_params (str):
                Optional suffix appended to the ``q`` query string.
            dataset_params (str):
                Optional suffix appended to ``dataset`` in ``Datasets``.
                Examples: ``.limit(10)`` or
                ``{NextPageId}.limit(500)``.

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
            "Datasets": dataset + dataset_params,
            "q": "processnumber{" + process + "}" + query_params,
            "Limit": 1}

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "AccessToken": self._bigdata_auth_token}

        return self._paginate(
            url=url, payload=payload, headers=headers, dataset=dataset,
            query_type="process_number", query_val=process,
            dataset_params=dataset_params)

    def _validate_dataset_mapping(
            self, datasets: list[str], mapping: dict[str, str],
            mapping_name: str) -> None:
        """Validate that mapping keys are dataset names.

        Args:
            datasets (list[str]):
                Dataset names to fetch.
            mapping (dict[str, str]):
                Per-dataset suffix mapping.
            mapping_name (str):
                Parameter name used in the error message.

        Raises:
            ValueError:
                If any mapping key is not in ``datasets``.
        """
        extra_keys = set(mapping.keys()) - set(datasets)
        if extra_keys:
            msg = (
                "The {mapping_name} keys must correspond to the "
                "dataset names: {extra_keys}").format(
                    mapping_name=mapping_name,
                    extra_keys=extra_keys)
            raise ValueError(msg)

    def _fetch_datasets_parallel(
            self, datasets: list[str], fetch_fn: Callable[[str], dict],
            verbosity: bool = False, skip_errors: bool = False,
            max_workers: int | None = None) -> dict[str, dict]:
        """Fetch datasets in parallel using a thread pool.

        Args:
            datasets (list[str]):
                Dataset names to fetch.
            fetch_fn (Callable[[str], dict]):
                Callable that receives a dataset name and returns its
                API response dictionary.
            verbosity (bool):
                When True, logs progress for each dataset.
            skip_errors (bool):
                When True, soft-fail non-critical errors into
                ``__errors__``. Login and invalid-input errors always
                raise.
            max_workers (int | None):
                Maximum number of worker threads. Defaults to
                ``BIGDATACORP__N_PARALLEL`` when None.

        Returns:
            dict[str, dict]:
                Mapping of dataset name to API response dictionary.
                Soft errors are stored under ``__errors__`` when
                ``skip_errors`` is True.

        Raises:
            BigDataCorpAPIInvalidInputException:
                Always re-raised; pending futures are cancelled.
            BigDataCorpAPILoginProblemException:
                Always re-raised; pending futures are cancelled.
            Exception:
                Re-raised when ``skip_errors`` is False.
        """
        if max_workers is None:
            max_workers = BIGDATACORP__N_PARALLEL

        response_dict = {}
        fetch_error = {}
        workers = min(max_workers, max(len(datasets), 1))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_db = {}
            for db in datasets:
                if verbosity:
                    logger.info(
                        "Fetching dataset: {dataset}", dataset=db)
                future = executor.submit(fetch_fn, db)
                future_to_db[future] = db

            for future in as_completed(future_to_db):
                db = future_to_db[future]
                try:
                    response_dict[db] = future.result()
                except (
                        BigDataCorpAPIInvalidInputException,
                        BigDataCorpAPILoginProblemException) as e:
                    for pending in future_to_db:
                        pending.cancel()
                    raise e
                except Exception as e:
                    if skip_errors:
                        fetch_error[db] = str(e)
                    else:
                        for pending in future_to_db:
                            pending.cancel()
                        raise e

        if fetch_error:
            response_dict['__errors__'] = fetch_error
        return response_dict

    def get_cpf_datasets(self, cpf: str, datasets: list[str],
                         verbosity: bool = False,
                         skip_errors: bool = False,
                         query_params: dict[str, str] | None = None,
                         dataset_params: dict[str, str] | None = None
                         ) -> dict[str, dict]:
        """Fetch multiple CPF datasets and return a keyed response dict.

        Datasets are requested in parallel using a thread pool.

        Args:
            cpf (str): Person CPF document number.
            datasets (list[str]): Dataset names to fetch in parallel.
            verbosity (bool):
                When True, logs progress for each dataset.
            skip_errors (bool):
                When True, non-critical errors are stored under
                ``__errors__`` instead of raising. Login and
                invalid-input errors always raise. Defaults to False.
            query_params (dict[str, str] | None):
                Optional mapping of dataset name to ``q`` suffix string.
                Example, ``{"processes": ", returnupdates{false}"}``.
                Each key must be present in ``datasets``.
            dataset_params (dict[str, str] | None):
                Optional mapping of dataset name to ``Datasets`` suffix.
                Example:
                ``{"processes": "{NextPageId}.limit(500)"}``.
                Each key must be present in ``datasets``.

        Returns:
            dict[str, dict]:
                Mapping of dataset name to API response dictionary.
                Soft errors are stored under ``__errors__`` when
                ``skip_errors`` is True.

        Raises:
            ValueError:
                If ``query_params`` or ``dataset_params`` keys are not
                in ``datasets``.
            BigDataCorpAPIInvalidInputException:
                Always re-raised; pending fetches are cancelled.
            BigDataCorpAPILoginProblemException:
                Always re-raised; pending fetches are cancelled.
            BigDataCorpAPIException:
                Other API errors from ``get_cpf_dataset`` when
                ``skip_errors`` is False.
        """
        if query_params is None:
            query_params = {}
        if dataset_params is None:
            dataset_params = {}

        self._validate_dataset_mapping(
            datasets=datasets, mapping=query_params,
            mapping_name="query_params")
        self._validate_dataset_mapping(
            datasets=datasets, mapping=dataset_params,
            mapping_name="dataset_params")

        def fetch_fn(db: str) -> dict:
            return self.get_cpf_dataset(
                cpf=cpf, dataset=db,
                query_params=query_params.get(db, ""),
                dataset_params=dataset_params.get(db, ""))

        return self._fetch_datasets_parallel(
            datasets=datasets, fetch_fn=fetch_fn, verbosity=verbosity,
            skip_errors=skip_errors)

    def get_cnpj_datasets(self, cnpj: str, datasets: list[str],
                          verbosity: bool = False,
                          skip_errors: bool = False,
                          query_params: dict[str, str] | None = None,
                          dataset_params: dict[str, str] | None = None
                          ) -> dict[str, dict]:
        """Fetch multiple CNPJ datasets and return a keyed response dict.

        Strips punctuation from ``cnpj`` before querying. Datasets are
        requested in parallel using a thread pool.

        Args:
            cnpj (str): Company CNPJ document number.
            datasets (list[str]): Dataset names to fetch in parallel.
            verbosity (bool):
                When True, logs progress for each dataset.
            skip_errors (bool):
                When True, non-critical errors are stored under
                ``__errors__`` instead of raising. Login and
                invalid-input errors always raise. Defaults to False.
            query_params (dict[str, str] | None):
                Optional mapping of dataset name to ``q`` suffix string.
                Example, ``{"processes": ", returnupdates{false}"}``.
                Each key must be present in ``datasets``.
            dataset_params (dict[str, str] | None):
                Optional mapping of dataset name to ``Datasets`` suffix.
                Example:
                ``{"processes": "{NextPageId}.limit(500)"}``.
                Each key must be present in ``datasets``.

        Returns:
            dict[str, dict]:
                Mapping of dataset name to API response dictionary.
                Soft errors are stored under ``__errors__`` when
                ``skip_errors`` is True.

        Raises:
            ValueError:
                If ``query_params`` or ``dataset_params`` keys are not
                in ``datasets``.
            BigDataCorpAPIInvalidInputException:
                Always re-raised; pending fetches are cancelled.
            BigDataCorpAPILoginProblemException:
                Always re-raised; pending fetches are cancelled.
            BigDataCorpAPIException:
                Other API errors from ``get_cnpj_dataset`` when
                ``skip_errors`` is False.
        """
        if query_params is None:
            query_params = {}
        if dataset_params is None:
            dataset_params = {}

        self._validate_dataset_mapping(
            datasets=datasets, mapping=query_params,
            mapping_name="query_params")
        self._validate_dataset_mapping(
            datasets=datasets, mapping=dataset_params,
            mapping_name="dataset_params")

        cnpj = cnpj.replace(".", "").replace("/", "").replace("-", "")

        def fetch_fn(db: str) -> dict:
            return self.get_cnpj_dataset(
                cnpj=cnpj, dataset=db,
                query_params=query_params.get(db, ""),
                dataset_params=dataset_params.get(db, ""))

        return self._fetch_datasets_parallel(
            datasets=datasets, fetch_fn=fetch_fn, verbosity=verbosity,
            skip_errors=skip_errors)

    def get_process_datasets(self, process: str, datasets: list[str],
                             verbosity: bool = False,
                             skip_errors: bool = False,
                             query_params: dict[str, str] | None = None,
                             dataset_params: dict[str, str] | None = None
                             ) -> dict[str, dict]:
        """Fetch multiple process datasets and return a keyed response dict.

        Strips punctuation from ``process`` before querying. Datasets are
        requested in parallel using a thread pool.

        Args:
            process (str):
                Judicial process number.
            datasets (list[str]):
                Dataset names to fetch in parallel.
            verbosity (bool):
                When True, logs progress for each dataset.
            skip_errors (bool):
                When True, non-critical errors are stored under
                ``__errors__`` instead of raising. Login and
                invalid-input errors always raise. Defaults to False.
            query_params (dict[str, str] | None):
                Optional mapping of dataset name to ``q`` suffix string.
                Each key must be present in ``datasets``.
            dataset_params (dict[str, str] | None):
                Optional mapping of dataset name to ``Datasets`` suffix.
                Example:
                ``{"basic_data": "{NextPageId}.limit(500)"}``.
                Each key must be present in ``datasets``.

        Returns:
            dict[str, dict]:
                Mapping of dataset name to API response dictionary.
                Soft errors are stored under ``__errors__`` when
                ``skip_errors`` is True.

        Raises:
            ValueError:
                If ``query_params`` or ``dataset_params`` keys are not
                in ``datasets``.
            BigDataCorpAPIInvalidInputException:
                Always re-raised; pending fetches are cancelled.
            BigDataCorpAPILoginProblemException:
                Always re-raised; pending fetches are cancelled.
            BigDataCorpAPIException:
                Other API errors from ``get_process_dataset`` when
                ``skip_errors`` is False.
        """
        if query_params is None:
            query_params = {}
        if dataset_params is None:
            dataset_params = {}

        self._validate_dataset_mapping(
            datasets=datasets, mapping=query_params,
            mapping_name="query_params")
        self._validate_dataset_mapping(
            datasets=datasets, mapping=dataset_params,
            mapping_name="dataset_params")

        process = process.replace(".", "").replace("/", "").replace("-", "")

        def fetch_fn(db: str) -> dict:
            return self.get_process_dataset(
                process=process, dataset=db,
                query_params=query_params.get(db, ""),
                dataset_params=dataset_params.get(db, ""))

        return self._fetch_datasets_parallel(
            datasets=datasets, fetch_fn=fetch_fn, verbosity=verbosity,
            skip_errors=skip_errors)

    def get_usage(
            self, initial_date: str, final_date: str) -> list[dict]:
        """Retrieve usage metrics for a date range across all datasets.

        Queries the usage endpoint once per CPF and CNPJ dataset. Failed
        requests are logged and omitted from the result list; this method
        does not propagate those errors to the caller.

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
                Must contain ``Result`` / ``OnlineCertificates`` /
                ``AdditionalOutputData`` under ``dataset``.

        Returns:
            dict[str, str | bytes]:
                ``file_type`` (str) and ``file_content`` (bytes).

        Raises:
            BigDataCorpAPIException:
                If no file URL is found or the download request fails.
            AttributeError:
                If ``json_data`` does not match the expected certificate
                structure.
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
