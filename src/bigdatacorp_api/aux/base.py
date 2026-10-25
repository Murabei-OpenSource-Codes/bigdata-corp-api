"""Base module for BigDataCorp API."""
import requests
import tenacity
from loguru import logger
from typing import Callable
from concurrent.futures import (
    FIRST_COMPLETED, ThreadPoolExecutor, wait)

# Configs
from bigdatacorp_api.config import (
    BIGDATACORP__MAX_PAGES, BIGDATACORP__N_PARALLEL,
    BIGDATACORP__REQUEST_TIMEOUT)

# Exceptions
from bigdatacorp_api.exceptions import (
    bigdata_raise_for_status, RETRY_EXCEPTIONS,
    BigDataCorpAPIException, BigDataCorpAPIQueryParametersException,
    BigDataCorpAPILoginException)


def _is_retryable_request_error(exc: BaseException) -> bool:
    """Return True for a transient HTTP transport failure.

    Domain errors in ``RETRY_EXCEPTIONS`` are raised on the first
    attempt. Connection failures, timeouts, HTTP 429, and HTTP 5xx
    are retried.

    Args:
        exc (BaseException):
            Error raised while sending the request.

    Returns:
        bool:
            True when ``send_request`` should try again.
    """
    if isinstance(exc, RETRY_EXCEPTIONS):
        return False
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code == 429 or (
            isinstance(status_code, int) and status_code >= 500):
        return True
    return False


def _warn_incomplete_page(dataset: str, reason: str, next_page_id: object,
                          pages_fetched: int) -> None:
    """Log a warning when pagination stops before a normal end.

    Args:
        dataset (str):
            Requested dataset name.
        reason (str):
            Why pagination stopped.
        next_page_id (object):
            Page id that could not be completed.
        pages_fetched (int):
            Next pages already requested.
    """
    logger.warning(
        "Pagination stopped. dataset={dataset} reason={reason} "
        "next_page_id={next_page_id} pages_fetched={pages_fetched}",
        dataset=dataset, reason=reason, next_page_id=next_page_id,
        pages_fetched=pages_fetched)


def _log_exhausted_retry(retry_state: "tenacity.RetryCallState") -> None:
    """Log the last transport error and raise it again.

    Args:
        retry_state (tenacity.RetryCallState):
            Tenacity state for the failed call.

    Raises:
        BaseException:
            The error from the last attempt.
    """
    outcome = retry_state.outcome
    exc = outcome.exception() if outcome is not None else None
    attempts = retry_state.attempt_number
    if exc is None:
        logger.exception(
            "BigDataCorp request failed after {attempts} attempts.",
            attempts=attempts)
        return
    try:
        raise exc
    except Exception:
        logger.exception(
            "BigDataCorp request failed after {attempts} attempts.",
            attempts=attempts)
        raise


class BaseBigDataCorpAPIMixin:
    """Mixin for BigDataCorp API."""

    def __init__(self, bigdata_auth_token: str) -> None:
        """Initialize the API client.

        Args:
            bigdata_auth_token (str):
                BigDataCorp access token sent as the ``AccessToken`` header.
        """
        self._bigdata_auth_token = bigdata_auth_token
        self._auth_headers = self.build_default_headers()

    def build_default_headers(self) -> dict:
        """Build the default headers for the request.

        Returns:
            dict:
                The default headers.
        """
        return {
            "accept": "application/json",
            "content-type": "application/json",
            "AccessToken": self._bigdata_auth_token}

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=15),
        retry=tenacity.retry_if_exception(_is_retryable_request_error),
        retry_error_callback=_log_exhausted_retry)
    def send_request(self, url: str, payload: dict) -> dict:
        """Send a POST request to BigDataCorp with retries.

        Connection failures, timeouts, HTTP 429, and HTTP 5xx are
        retried up to five times. The last transport error is logged
        and raised again. Domain status errors are raised on the first
        attempt. BigData returns a JSON body with status codes even
        when the query is not successful.

        Args:
            url (str):
                Target API URL.
            payload (dict):
                Request payload.

        Returns:
            dict:
                The response JSON dictionary.

        Raises:
            BigDataCorpAPIRestrictedDocumentException:
                If the API reports a restricted document.
            BigDataCorpAPILoginException:
                If login fails or expires.
            BigDataCorpAPIQueryParametersException:
                If the API reports an invalid query parameter.
            BigDataCorpAPIProblemAPIException:
                If there is an internal API problem.
            BigDataCorpAPIOnDemandQueriesException:
                If on-demand query fails.
            BigDataCorpAPIMonitoringAPIException:
                If monitoring API fails.
            BigDataCorpAPIUnmappedErrorException:
                For any other API error.
            requests.RequestException:
                If a transport error remains after five attempts.
                The error is logged before it is raised again.
        """
        response = requests.post(
            url, json=payload, headers=self._auth_headers,
            timeout=BIGDATACORP__REQUEST_TIMEOUT)
        response.raise_for_status()
        response_json = response.json()

        # BigdataCorp status code handling
        bigdata_raise_for_status(bigdata_response=response_json)
        return response_json

    def paginate(self, url: str, payload: dict, dataset: str, query_type: str,
                 query_val: str, dataset_params: str = "") -> dict:
        """Iterate paginated responses and merge list fields.

        Args:
            url (str):
                Target API URL.
            payload (dict):
                Request payload.
            dataset (str):
                Requested dataset name.
            query_type (str):
                Query kind passed by the caller. The page loop does
                not read it.
            query_val (str):
                Query value passed by the caller. The page loop does
                not read it.
            dataset_params (str):
                Suffix appended to ``dataset`` on each page request,
                including ``.next(...)`` calls.

        Returns:
            dict:
                The aggregated response. A repeated ``NextPageId``, the
                page cap, an empty ``Result``, or a missing dataset
                object stops the loop. The abnormal stops are logged
                as warnings.

        Raises:
            BigDataCorpAPIPayloadStatusException:
                The same status errors as ``send_request``.
            requests.RequestException:
                The same transport errors as ``send_request``.
        """
        first_page = self.send_request(url=url, payload=payload)
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

        seen_page_ids = set()
        pages_fetched = 0
        while True:
            next_page_id = dataset_dict.get("NextPageId")

            if not next_page_id:
                break
            if next_page_id in seen_page_ids:
                _warn_incomplete_page(
                    dataset=dataset,
                    reason="NextPageId was repeated.",
                    next_page_id=next_page_id,
                    pages_fetched=pages_fetched)
                break
            if BIGDATACORP__MAX_PAGES <= pages_fetched:
                reason = "Page cap of {max_pages} was reached.".format(
                    max_pages=BIGDATACORP__MAX_PAGES)
                _warn_incomplete_page(
                    dataset=dataset, reason=reason,
                    next_page_id=next_page_id,
                    pages_fetched=pages_fetched)
                break
            seen_page_ids.add(next_page_id)
            pages_fetched = pages_fetched + 1

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
            next_page = self.send_request(url=url, payload=next_payload)

            # Get next dataset dict
            next_result_list = next_page.get("Result", [])
            if not next_result_list:
                break

            next_result_obj = next_result_list[0]
            next_dataset_dict = next_result_obj.get(dataset_key)
            if (not next_dataset_dict
                    or not isinstance(next_dataset_dict, dict)):
                _warn_incomplete_page(
                    dataset=dataset,
                    reason="The next page dataset object was missing.",
                    next_page_id=next_page_id,
                    pages_fetched=pages_fetched)
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
                ``__errors__``. Login and query-parameter errors always
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
            BigDataCorpAPIQueryParametersException:
                Always re-raised. Calls that have not started are
                cancelled.
            BigDataCorpAPILoginException:
                Always re-raised. Calls that have not started are
                cancelled.
            Exception:
                Re-raised when ``skip_errors`` is False. Calls that
                have not started are cancelled.
        """
        if max_workers is None:
            max_workers = BIGDATACORP__N_PARALLEL

        response_dict = {}
        fetch_error = {}
        workers = min(max_workers, max(len(datasets), 1))
        # Login and invalid query parameters are never skipped.
        deal_break_exception = (
            BigDataCorpAPIQueryParametersException,
            BigDataCorpAPILoginException)

        pending = list(datasets)
        inflight = {}

        def submit_next() -> None:
            while pending and len(inflight) < workers:
                db = pending.pop(0)
                if verbosity:
                    logger.info(
                        "Fetching dataset: {dataset}", dataset=db)
                inflight[executor.submit(fetch_fn, db)] = db

        with ThreadPoolExecutor(max_workers=workers) as executor:
            submit_next()
            while inflight:
                done, _ = wait(
                    inflight, return_when=FIRST_COMPLETED)
                failed = None
                for future in done:
                    db = inflight.pop(future)
                    try:
                        response_dict[db] = future.result()
                    except Exception as e:
                        # Dataset, permission, and quota errors stay
                        # in __errors__ when skip_errors is set. Any
                        # other error cancels the next call.
                        if skip_errors and not isinstance(
                                e, deal_break_exception):
                            fetch_error[db] = str(e)
                        else:
                            failed = e
                            break
                if failed is not None:
                    pending.clear()
                    for pending_future in inflight:
                        pending_future.cancel()
                    raise failed
                submit_next()

        if fetch_error:
            response_dict['__errors__'] = fetch_error
        return response_dict

    def get_result_file(self, dataset: str, json_data: dict
                        ) -> dict[str, str | bytes]:
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
                If no file URL is found, the certificate structure is
                missing, or the download request fails.
        """
        try:
            certificate_data = json_data.get(dataset)\
                .get('Result')[0]\
                .get('OnlineCertificates')[0]\
                .get('AdditionalOutputData')
            file_url = certificate_data.get("RawResultFile")
            file_type = certificate_data\
                .get("RawResultFileType", "").lower()
        except (AttributeError, IndexError, TypeError):
            raise BigDataCorpAPIException(
                message="No file URL found for provided data.",
                payload={
                    "dataset": dataset,
                    "json_data": json_data})

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
