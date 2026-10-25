"""Timeout, parallelism, and pagination limits for the client.

Values are read from ``BIGDATACORP__REQUEST_TIMEOUT``,
``BIGDATACORP__N_PARALLEL``, and ``BIGDATACORP__MAX_PAGES`` when those
environment variables are set. A blank value keeps the default. Any
other value must be a positive integer.
"""
import os


def _positive_int(name: str, default: int) -> int:
    """Read a positive integer from the environment.

    A missing or blank value returns ``default``.

    Args:
        name (str):
            Environment variable name.
        default (int):
            Value used when the variable is missing or blank.

    Returns:
        int:
            Parsed positive integer.

    Raises:
        ValueError:
            If the value is not a positive integer.
    """
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(
            "{name} must be a positive integer. Got {raw}.".format(
                name=name, raw=raw)) from None
    if value < 1:
        raise ValueError(
            "{name} must be a positive integer. Got {raw}.".format(
                name=name, raw=raw))
    return value


BIGDATACORP__REQUEST_TIMEOUT: int = _positive_int(
    "BIGDATACORP__REQUEST_TIMEOUT", 60)
"""Seconds to wait for a single BigDataCorp HTTP request."""

BIGDATACORP__N_PARALLEL: int = _positive_int(
    "BIGDATACORP__N_PARALLEL", 8)
"""Max threads used when fetching multiple datasets in parallel."""

BIGDATACORP__MAX_PAGES: int = _positive_int(
    "BIGDATACORP__MAX_PAGES", 200)
"""Max extra pages fetched after the first response."""
