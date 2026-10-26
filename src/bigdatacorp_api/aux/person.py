"""Mixin for person related operations."""
import re


# Local imports
from bigdatacorp_api.aux.base import BaseBigDataCorpAPIMixin
from bigdatacorp_api.exceptions import (
    BigDataCorpAPIInvalidDatabaseException,
    BigDataCorpAPIInvalidDocumentException)


CPF_DATASETS: list[str] = [
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
    "registration_data"]
"""Supported dataset identifiers for CPF queries."""

CPF_ONDEMAND_DATABASES: list[str] = [
    "ondemand_pgfn_person",
    "ondemand_cert_debt_absence_by_state_person"]
"""Datasets routed to the on-demand certificate endpoint."""


class PersonBigDataCorpAPIMixin(BaseBigDataCorpAPIMixin):
    """Mixin for person related operations."""

    def __init__(self, bigdata_auth_token: str) -> None:
        """Initialize the API client.

        Args:
            bigdata_auth_token (str):
                BigDataCorp access token sent as the ``AccessToken`` header.
        """
        super().__init__(bigdata_auth_token=bigdata_auth_token)

    def validate_cpf(self, cpf: str | int) -> str:
        """Return the CPF digits or raise when the number is invalid.

        Strips every non-digit character, then checks both verification
        digits.

        Args:
            cpf (str | int):
                CPF number to validate.

        Returns:
            str:
                Eleven digits.

        Raises:
            BigDataCorpAPIInvalidDocumentException:
                If ``cpf`` is not a valid CPF.
        """
        message = "cpf [{cpf}] não é válido"
        digits = re.sub(r"\D", "", str(cpf))
        if len(digits) != 11 or digits == digits[0] * 11:
            raise BigDataCorpAPIInvalidDocumentException(
                message, payload={"cpf": cpf})

        first_sum = sum(
            int(digit) * weight
            for digit, weight in zip(digits[:9], range(10, 1, -1)))
        first_remainder = (first_sum * 10) % 11
        first_digit = 0 if first_remainder == 10 else first_remainder
        if first_digit != int(digits[9]):
            raise BigDataCorpAPIInvalidDocumentException(
                message, payload={"cpf": cpf})

        second_sum = sum(
            int(digit) * weight
            for digit, weight in zip(digits[:10], range(11, 1, -1)))
        second_remainder = (second_sum * 10) % 11
        second_digit = 0 if second_remainder == 10 else second_remainder
        if second_digit != int(digits[10]):
            raise BigDataCorpAPIInvalidDocumentException(
                message, payload={"cpf": cpf})
        return digits

    def list_cpf_dataset(self) -> list[str]:
        """Return available BigData CPF dataset names.

        Returns:
            list[str]:
                Supported dataset identifiers for CPF queries.
        """
        return CPF_DATASETS + CPF_ONDEMAND_DATABASES

    def get_cpf_dataset(self, cpf: str, dataset: str,
                        query_params: str = "",
                        dataset_params: str = "") -> dict:
        """Fetch a single CPF dataset from BigDataCorp.

        Retries up to five times on transient HTTP errors. Paginated
        responses are merged automatically.

        Args:
            cpf (str):
                Person CPF document number.
            dataset (str):
                Dataset name. Must be in ``CPF_DATASETS`` or
                ``CPF_ONDEMAND_DATABASES``.
            query_params (str):
                Optional suffix appended to the ``q`` query string.
            dataset_params (str):
                Optional suffix appended to ``dataset`` in ``Datasets``.
                Examples: ``.limit(10)`` or
                ``{NextPageId}.limit(500)``.

        Returns:
            dict: Raw JSON response from the BigDataCorp API.

        Raises:
            BigDataCorpAPIInvalidDocumentException:
                If ``cpf`` is not a valid CPF. The message is
                ``cpf [{cpf}] não é válido``.
            BigDataCorpAPIInvalidDatabaseException:
                If ``dataset`` is not supported for CPF queries.
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
            CPF_DATASETS + CPF_ONDEMAND_DATABASES)
        if dataset not in all_databases:
            msg = (
                "dataset [{dataset}] not available on bigboost for CPF, "
                "available datasets:\n{datasets}")
            raise BigDataCorpAPIInvalidDatabaseException(
                msg, payload={
                    "dataset": dataset,
                    "datasets": all_databases})

        if dataset in CPF_ONDEMAND_DATABASES:
            url = "https://plataforma.bigdatacorp.com.br/ondemand"
        else:
            url = "https://bigboost.bigdatacorp.com.br/peoplev2"

        cpf = self.validate_cpf(cpf)
        payload = {
            "Datasets": dataset + dataset_params,
            "q": "doc{" + cpf + "}" + query_params,
            "Limit": 1}
        return self.paginate(
            url=url, payload=payload,dataset=dataset, query_type="cpf",
            query_val=cpf, dataset_params=dataset_params)

    def get_cpf_datasets(self, cpf: str, datasets: list[str],
                         verbosity: bool = False,
                         skip_errors: bool = False,
                         query_params: dict[str, str] | None = None,
                         dataset_params: dict[str, str] | None = None
                         ) -> dict[str, dict]:
        """Fetch multiple CPF datasets and return a keyed response dict.

        Datasets are requested in parallel using a thread pool.

        Args:
            cpf (str):
                Person CPF document number.
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
                If ``cpf`` is not a valid CPF.
            BigDataCorpAPIQueryParametersException:
                Always re-raised. Calls that have not started are
                cancelled.
            BigDataCorpAPILoginException:
                Always re-raised. Calls that have not started are
                cancelled.
            Exception:
                Other errors from ``get_cpf_dataset`` when
                ``skip_errors`` is False. Calls that have not started
                are cancelled.
        """
        if query_params is None:
            query_params = {}
        if dataset_params is None:
            dataset_params = {}

        cpf = self.validate_cpf(cpf)
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