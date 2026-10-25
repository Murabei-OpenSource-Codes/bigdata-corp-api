"""Mixin for process related operations."""
import re

# Local imports
from bigdatacorp_api.aux.base import BaseBigDataCorpAPIMixin
from bigdatacorp_api.exceptions import (
    BigDataCorpAPIInvalidDatabaseException,
    BigDataCorpAPIInvalidDocumentException)


PROCESS_DATABASES: list[str] = [
    'basic_data', 'cade_processes_data']
"""Supported dataset identifiers for process queries."""


class ProcessBigDataCorpAPIMixin(BaseBigDataCorpAPIMixin):
    """Mixin for process related operations."""

    def __init__(self, bigdata_auth_token: str) -> None:
        """Initialize the API client.

        Args:
            bigdata_auth_token (str):
                BigDataCorp access token sent as the ``AccessToken`` header.
        """
        super().__init__(bigdata_auth_token=bigdata_auth_token)

    def list_process_dataset(self) -> list[str]:
        """Return available BigData process dataset names.

        Returns:
            list[str]:
                Supported dataset identifiers for process queries.
        """
        return PROCESS_DATABASES

    def validate_cnj(self, process: str) -> str:
        """Return the CNJ digits or raise when the number is invalid.

        Accepts ``NNNNNNN-DD.AAAA.J.TR.OOOO`` or twenty digits. The
        check digits follow ISO 7064 mod 97-10. The judiciary segment
        must be from 1 to 9.

        Args:
            process (str):
                Judicial process number.

        Returns:
            str:
                Twenty digits, without punctuation.

        Raises:
            BigDataCorpAPIInvalidDocumentException:
                If ``process`` is not a valid CNJ number.
        """
        message = "process_number [{process_number}] não é válido"
        cleaned = str(process).strip()
        # If the input contains formatting characters, check the standard
        # CNJ pattern.
        if "-" in cleaned or "." in cleaned:
            masked = re.fullmatch(
                r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}", cleaned)
            if masked is None:
                raise BigDataCorpAPIInvalidDocumentException(
                    message, payload={"process_number": process})

        # Extract digits only.
        digits = re.sub(r"\D", "", cleaned)
        if len(digits) != 20:
            raise BigDataCorpAPIInvalidDocumentException(
                message, payload={"process_number": process})

        # Decompose into components:
        # Digits: [0:7]=NNNNNNN, [7:9]=DD, [9:13]=AAAA,
        # [13:14]=J, [14:16]=TR, [16:20]=OOOO
        sequential = digits[:7]
        check_digits = digits[7:9]
        tail = digits[9:]

        # Segment of Judiciary (J) must be between 1 and 9.
        judiciary_segment = int(digits[13])
        if judiciary_segment < 1 or judiciary_segment > 9:
            raise BigDataCorpAPIInvalidDocumentException(
                message, payload={"process_number": process})

        # ISO 7064 Modulo 97-10 calculation:
        # Rearrange as: NNNNNNN + AAAA + J + TR + OOOO + 00
        base_number = int(sequential + tail + "00")
        expected_dd = 98 - (base_number % 97)
        if int(check_digits) != expected_dd:
            raise BigDataCorpAPIInvalidDocumentException(
                message, payload={"process_number": process})
        return digits

    def get_process_dataset(self, process: str, dataset: str,
                            query_params: str = "",
                            dataset_params: str = "") -> dict:
        """Fetch a single process dataset from BigDataCorp.

        Retries up to five times on transient HTTP errors. Paginated
        responses are merged automatically.

        Args:
            process (str):
                Judicial process number.
            dataset (str):
                Dataset name; must be in ``PROCESS_DATABASES``.
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
                If ``process`` is not a valid CNJ number. The message
                is ``process_number [{process_number}] não é válido``.
            BigDataCorpAPIInvalidDatabaseException:
                If ``dataset`` is not supported for process queries.
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
        if dataset not in PROCESS_DATABASES:
            msg = (
                "dataset [{dataset}] not available on bigboost for process, "
                "available datasets:\n{datasets}")
            raise BigDataCorpAPIInvalidDatabaseException(
                msg, payload={
                    "dataset": dataset,
                    "datasets": PROCESS_DATABASES})

        process = self.validate_cnj(process=process)
        url = "https://plataforma.bigdatacorp.com.br/processos"
        payload = {
            "Datasets": dataset + dataset_params,
            "q": "processnumber{" + process + "}" + query_params,
            "Limit": 1}
        return self.paginate(
            url=url, payload=payload,dataset=dataset,
            query_type="process_number", query_val=process,
            dataset_params=dataset_params)

    def get_process_datasets(self, process: str, datasets: list[str],
                             verbosity: bool = False,
                             skip_errors: bool = False,
                             query_params: dict[str, str] | None = None,
                             dataset_params: dict[str, str] | None = None
                             ) -> dict[str, dict]:
        """Fetch multiple process datasets and return a keyed response dict.

        Validates ``process`` before querying. Datasets are requested
        in parallel using a thread pool.

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
                query-parameter errors always raise. Defaults to False.
            query_params (dict[str, str] | None):
                Optional mapping of dataset name to ``q`` suffix string.
                Each key must be present in ``datasets``.
            dataset_params (dict[str, str] | None):
                Optional mapping of dataset name to ``Datasets`` suffix.
                Ex. ``{"basic_data": "{NextPageId}.limit(500)"}``.
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
                If ``process`` is not a valid CNJ number.
            BigDataCorpAPIQueryParametersException:
                Always re-raised. Calls that have not started are
                cancelled.
            BigDataCorpAPILoginException:
                Always re-raised. Calls that have not started are
                cancelled.
            Exception:
                Other errors from ``get_process_dataset`` when
                ``skip_errors`` is False. Calls that have not started
                are cancelled.
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

        process = self.validate_cnj(process=process)

        def fetch_fn(db: str) -> dict:
            return self.get_process_dataset(
                process=process, dataset=db,
                query_params=query_params.get(db, ""),
                dataset_params=dataset_params.get(db, ""))

        return self._fetch_datasets_parallel(
            datasets=datasets, fetch_fn=fetch_fn, verbosity=verbosity,
            skip_errors=skip_errors)
