"""System related operations."""

# Local imports
from bigdatacorp_api.aux.base import BaseBigDataCorpAPIMixin
from bigdatacorp_api.aux.person import CPF_DATASETS, CPF_ONDEMAND_DATABASES
from bigdatacorp_api.aux.company import (
    CNPJ_DATABASES, CNPJ_MARKETPLACE_DATASETS, CNPJ_ONDEMAND_DATABASES)


class SystemBigDataCorpAPIMixin(BaseBigDataCorpAPIMixin):
    """Mixin for system related operations."""

    def _usage_entry(
            self, api_type: str, end_point: str,
            body: dict | None = None, *, with_error: bool = False) -> dict:
        """Map one usage response body to a result row.

        Args:
            api_type (str):
                ``people`` or ``companies``.
            end_point (str):
                Dataset name sent as ``Datasets``.
            body (dict | None):
                Parsed JSON returned by ``send_request``. Not read when
                ``with_error`` is True.
            with_error (bool):
                When True, return zero counters and ``__errors__`` True.
                Defaults to False.

        Returns:
            dict:
                Usage counters for one dataset. Includes ``__errors__``
                when the usage query failed for a companies dataset.
        """
        if with_error:
            return {
                "api_type": api_type,
                "end_point": end_point,
                "successful_requests": 0,
                "requests_with_error": 0,
                "queries_charged": 0,
                "queries_not_charged": 0,
                "estimated_price": 0,
                "__errors__": True}
        if body is None:
            raise ValueError("body is required when with_error is False.")
        usage_data = body["UsageData"]
        return {
            "api_type": api_type,
            "end_point": end_point,
            "successful_requests": usage_data["TotalSuccessfulRequests"],
            "requests_with_error": usage_data["TotalRequestsWithError"],
            "queries_charged": usage_data["TotalQueriesCharged"],
            "queries_not_charged": usage_data["TotalQueriesNotCharged"],
            "estimated_price": usage_data["TotalEstimatedPrice"],
            "__errors__": False}

    def get_usage(self, initial_date: str, final_date: str) -> list[dict]:
        """Retrieve usage metrics for a date range across all datasets.

        Queries the usage endpoint once per CPF dataset and once per
        CNPJ dataset, including on-demand and marketplace names.
        A people-dataset failure propagates. A companies-dataset
        failure adds a row with zero counters and ``__errors__`` set
        to True, then continues with the next dataset.

        Args:
            initial_date (str):
                Range start date in ``yyyy-MM-dd`` format.
            final_date (str):
                Range end date in ``yyyy-MM-dd`` format.

        Returns:
            list[dict]:
                One entry per dataset query. Each dict contains
                ``api_type``, ``end_point``, ``successful_requests``,
                ``requests_with_error``, ``queries_charged``,
                ``queries_not_charged``, ``estimated_price``, and
                ``__errors__``.

        Raises:
            BigDataCorpAPIPayloadStatusException:
                If a people-dataset query returns an API status error.
            requests.RequestException:
                If a people-dataset query fails after the retries.
            KeyError:
                If a people response has no ``UsageData``.
        """
        results = []
        url = "https://plataforma.bigdatacorp.com.br/usage"

        payload = {
            "InitialReferenceDate": initial_date,
            "FinalReferenceDate": final_date,
            "DateFormat": "yyyy-MM-dd"
        }

        for api in CPF_DATASETS + CPF_ONDEMAND_DATABASES:
            payload["Api"] = "people"
            payload["Datasets"] = api
            
            response = self.send_request(url=url, payload=payload)
            results.append(self._usage_entry(
                api_type="people", end_point=api, body=response))

        for api in (
                CNPJ_DATABASES + CNPJ_MARKETPLACE_DATASETS +
                CNPJ_ONDEMAND_DATABASES):
            payload["Api"] = "companies"
            payload["Datasets"] = api
            try:
                response = self.send_request(url=url, payload=payload)
                results.append(self._usage_entry(
                    api_type="companies", end_point=api, body=response))
            except Exception:
                results.append(self._usage_entry(
                    api_type="companies", end_point=api, with_error=True))
        return results
