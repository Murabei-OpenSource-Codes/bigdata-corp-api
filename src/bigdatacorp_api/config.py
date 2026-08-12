"""Module for configuring the BigDataCorp API."""
import os


BIGDATACORP__REQUEST_TIMEOUT = int(
    os.getenv("BIGDATACORP__REQUEST_TIMEOUT", 60))
"""The timeout for requests to the BigDataCorp API."""