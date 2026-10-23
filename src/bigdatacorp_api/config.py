"""Timeout and parallelism settings for the BigDataCorp API client.

Values are read from ``BIGDATACORP__REQUEST_TIMEOUT`` and
``BIGDATACORP__N_PARALLEL`` when those environment variables are set.
"""
import os


BIGDATACORP__REQUEST_TIMEOUT: int = int(
    os.getenv("BIGDATACORP__REQUEST_TIMEOUT", 60))
"""Seconds to wait for a single BigDataCorp HTTP request."""

BIGDATACORP__N_PARALLEL: int = int(
    os.getenv("BIGDATACORP__N_PARALLEL", 8))
"""Max threads used when fetching multiple datasets in parallel."""