"""Custom exceptions for BigDataCorp API client errors."""


class BigDataCorpAPIException(Exception):
    """Base exception for BigDataCorp API client errors.

    Attributes:
        message (str): Human-readable error description.
        payload (dict): Optional context returned by the API or client.
    """

    def __repr__(self) -> str:
        """Return the exception representation.

        Returns:
            str: Class name and message formatted for display.
        """
        template = "{class_name}: {message}"
        return template.format(
            class_name=self.__class__.__name__,
            message=self.message)

    def __str__(self) -> str:
        """Return the string representation of the exception.

        Returns:
            str: Same value as ``repr(self)``.
        """
        return self.__repr__()

    def __init__(self, message: str, payload: dict = {}) -> None:
        """Initialize the exception.

        Args:
            message (str): Human-readable error description.
            payload (dict): Optional context for debugging or logging.
        """
        self.message = message
        self.payload = payload

    def to_dict(self) -> dict:
        """Serialize the exception to a dictionary.

        Returns:
            dict: Keys ``payload``, ``type``, and ``message``.
        """
        rv = {
            "payload": self.payload,
            "type": self.__class__.__name__,
            "message": self.message}
        return rv


class BigDataCorpAPIInvalidDocumentException(BigDataCorpAPIException):
    """Raised when a CPF or CNPJ document is invalid."""


class BigDataCorpAPIInvalidDatabaseException(BigDataCorpAPIException):
    """Raised when the requested dataset is not supported."""


class BigDataCorpAPIMinorDocumentException(BigDataCorpAPIException):
    """Raised when a CPF belongs to a minor."""


class BigDataCorpAPIMaxRetryException(BigDataCorpAPIException):
    """Raised when all HTTP retry attempts are exhausted."""


class BigDataCorpAPILoginProblemException(BigDataCorpAPIException):
    """Raised when authentication fails or the token has expired."""


class BigDataCorpAPIProblemAPIException(BigDataCorpAPIException):
    """Raised when BigDataCorp reports an internal API or service error."""


class BigDataCorpAPIOnDemandQueriesException(BigDataCorpAPIException):
    """Raised when an on-demand query fails."""


class BigDataCorpAPIMonitoringAPIException(BigDataCorpAPIException):
    """Raised when the monitoring or asynchronous API call fails."""


class BigDataCorpAPIUnmappedErrorException(BigDataCorpAPIException):
    """Raised when the API returns an unmapped status code."""


class BigDataCorpAPIInvalidInputException(BigDataCorpAPIException):
    """Raised when request input data is invalid."""


class BigDataCorpAPIEmptyEnrichedProcessException(BigDataCorpAPIException):
    """Raised when a process query returns no enriched data."""
