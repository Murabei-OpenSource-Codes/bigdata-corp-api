"""Mixin for company related operations."""
import re

# Local imports
from bigdatacorp_api.aux.base import BaseBigDataCorpAPIMixin
from bigdatacorp_api.exceptions import (
    BigDataCorpAPIInvalidDatabaseException,
    BigDataCorpAPIInvalidDocumentException)


CNPJ_DATABASES: list[str] = [
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
    "registration_data"]
"""Supported dataset identifiers for CNPJ queries."""

CNPJ_ONDEMAND_DATABASES: list[str] = [
    "ondemand_pgfn_company",
    "ondemand_cert_debt_absence_by_state_company"]
"""Datasets routed to the on-demand certificate endpoint."""


CNPJ_MARKETPLACE_DATASETS: list[str] = [
    "partner_murabei_credit_score_company"
]
"""CNPJ datasets routed to the marketplace endpoint."""


class CompanyBigDataCorpAPIMixin(BaseBigDataCorpAPIMixin):
    """Mixin for company related operations."""

    def __init__(self, bigdata_auth_token: str) -> None:
        """Initialize the API client.

        Args:
            bigdata_auth_token (str):
                BigDataCorp access token sent as the ``AccessToken`` header.
        """
        super().__init__(bigdata_auth_token=bigdata_auth_token)

    def validate_cnpj(self, cnpj: str) -> str:
        """Return the CNPJ or raise when the number is invalid.

        Accepts the numeric format and the alphanumeric format from
        the Federal Revenue. Strips ``.``, ``/``, ``-``, and spaces,
        then checks both verification digits.

        Args:
            cnpj (str):
                CNPJ number to validate.

        Returns:
            str:
                Fourteen characters, without punctuation.

        Raises:
            BigDataCorpAPIInvalidDocumentException:
                If ``cnpj`` is not a valid CNPJ.
        """
        message = "cnpj [{cnpj}] não é válido"
        cleaned = re.sub(r"[\./\-\s]", "", str(cnpj)).upper()
        if not re.fullmatch(r"[A-Z0-9]{12}\d{2}", cleaned):
            raise BigDataCorpAPIInvalidDocumentException(
                message, payload={"cnpj": cnpj})
        if cleaned == cleaned[0] * 14:
            raise BigDataCorpAPIInvalidDocumentException(
                message, payload={"cnpj": cnpj})

        weights_1 = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
        weights_2 = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
        sum_1 = sum(
            (ord(char) - 48) * weight
            for char, weight in zip(cleaned[:12], weights_1))
        remainder_1 = sum_1 % 11
        check_digit_1 = 0 if remainder_1 < 2 else 11 - remainder_1
        if check_digit_1 != int(cleaned[12]):
            raise BigDataCorpAPIInvalidDocumentException(
                message, payload={"cnpj": cnpj})

        body = cleaned[:12] + str(check_digit_1)
        sum_2 = sum(
            (ord(char) - 48) * weight
            for char, weight in zip(body, weights_2))
        remainder_2 = sum_2 % 11
        check_digit_2 = 0 if remainder_2 < 2 else 11 - remainder_2
        if check_digit_2 != int(cleaned[13]):
            raise BigDataCorpAPIInvalidDocumentException(
                message, payload={"cnpj": cnpj})
        return cleaned

    def list_cnpj_dataset(self) -> list[str]:
        """Return available BigData CNPJ dataset names.

        Returns:
            list[str]:
                Supported dataset identifiers for CNPJ queries.
        """
        return (
            CNPJ_DATABASES + CNPJ_MARKETPLACE_DATASETS +
            CNPJ_ONDEMAND_DATABASES)

    def get_cnpj_dataset(self, cnpj: str, dataset: str,
                         query_params: str = "",
                         dataset_params: str = "") -> dict:
        """Fetch a single CNPJ dataset from BigDataCorp.

        Retries up to five times on transient HTTP errors. Paginated
        responses are merged automatically.

        Args:
            cnpj (str):
                Company CNPJ document number.
            dataset (str):
                Dataset name. Must be in ``CNPJ_DATABASES``,
                ``CNPJ_MARKETPLACE_DATASETS``, or
                ``CNPJ_ONDEMAND_DATABASES``.
            query_params (str):
                Optional suffix appended to the ``q`` query string.
            dataset_params (str):
                Optional suffix appended to ``dataset`` in ``Datasets``.
                Examples: ``.limit(10)`` or
                ``{NextPageId}.limit(500)``.

        Returns:
            dict:
                Raw JSON response from the BigDataCorp API.

        Raises:
            BigDataCorpAPIInvalidDocumentException:
                If ``cnpj`` is not a valid CNPJ. The message is
                ``cnpj [{cnpj}] não é válido``.
            BigDataCorpAPIInvalidDatabaseException:
                If ``dataset`` is not supported for CNPJ queries.
            BigDataCorpAPIRestrictedDocumentException:
                If the API reports a restricted document.
            BigDataCorpAPILoginException:
                If authentication fails or the token expired.
            BigDataCorpAPIQueryParametersException:
                If the API reports an invalid query parameter.
            BigDataCorpAPIProblemAPIException:
                If the API reports an internal service error.
            BigDataCorpAPIOnDemandQueriesException:
                If an on-demand query fails.
            BigDataCorpAPIMonitoringAPIException:
                If the monitoring or async API call fails.
            BigDataCorpAPIUnmappedErrorException:
                If the API returns an unmapped status code.
            requests.RequestException:
                If a transport error remains after five attempts.
        """
        all_databases = (
            CNPJ_DATABASES + CNPJ_MARKETPLACE_DATASETS +
            CNPJ_ONDEMAND_DATABASES)
        if dataset not in all_databases:
            msg = (
                "dataset [{dataset}] not available on bigboost for CNPJ, "
                "available datasets:\n{datasets}")
            raise BigDataCorpAPIInvalidDatabaseException(
                msg, payload={
                    "dataset": dataset,
                    "datasets": all_databases})

        if dataset in CNPJ_MARKETPLACE_DATASETS:
            url = "https://plataforma.bigdatacorp.com.br/marketplace"
        elif dataset in CNPJ_ONDEMAND_DATABASES:
            url = "https://plataforma.bigdatacorp.com.br/ondemand"
        else:
            url = "https://bigboost.bigdatacorp.com.br/companies"

        cnpj = self.validate_cnpj(cnpj)
        payload = {
            "Datasets": dataset + dataset_params,
            "q": "doc{" + cnpj + "}" + query_params,
            "Limit": 1}
        return self.paginate(
            url=url, payload=payload, dataset=dataset, query_type="cnpj",
            query_val=cnpj, dataset_params=dataset_params)

    def get_cnpj_datasets(self, cnpj: str, datasets: list[str],
                          verbosity: bool = False,
                          skip_errors: bool = False,
                          query_params: dict[str, str] | None = None,
                          dataset_params: dict[str, str] | None = None
                          ) -> dict[str, dict]:
        """Fetch multiple CNPJ datasets and return a keyed response dict.

        Validates ``cnpj`` before querying. Datasets are requested in
        parallel using a thread pool.

        Args:
            cnpj (str):
                Company CNPJ document number.
            datasets (list[str]):
                Dataset names to fetch in parallel.
            verbosity (bool):
                When True, logs progress for each dataset.
            skip_errors (bool):
                When True, non-critical errors are stored under
                ``__errors__`` instead of raising. Login and
                query-parameter errors always raise. Defaults to False.
            query_params (dict[str, str] | None):
                Optional mapping of dataset name to ``q`` suffix string.
                Example, ``{"processes": ", returnupdates{false}"}``.
                Each key must be present in ``datasets``.
            dataset_params (dict[str, str] | None):
                Optional mapping of dataset name to ``Datasets`` suffix.
                Ex. ``{"processes": "{NextPageId}.limit(500)"}``.
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
            BigDataCorpAPIInvalidDocumentException:
                If ``cnpj`` is not a valid CNPJ.
            BigDataCorpAPIQueryParametersException:
                Always re-raised. Calls that have not started are
                cancelled.
            BigDataCorpAPILoginException:
                Always re-raised. Calls that have not started are
                cancelled.
            Exception:
                Other errors from ``get_cnpj_dataset`` when
                ``skip_errors`` is False. Calls that have not started
                are cancelled.
        """
        def fetch_fn(db: str) -> dict:
            return self.get_cnpj_dataset(
                cnpj=cnpj, dataset=db,
                query_params=query_params.get(db, ""),
                dataset_params=dataset_params.get(db, ""))

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

        cnpj = self.validate_cnpj(cnpj)
        return self._fetch_datasets_parallel(
            datasets=datasets, fetch_fn=fetch_fn, verbosity=verbosity,
            skip_errors=skip_errors)
