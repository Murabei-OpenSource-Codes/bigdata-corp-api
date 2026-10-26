"""BigDataCorp REST API client.

Exposes ``BigDataCorpAPI`` for fetching CPF, CNPJ, and process datasets,
aggregating paginated responses, reporting usage, and downloading result
files from on-demand certificate endpoints.
"""
# Local imports
from bigdatacorp_api.aux import (
    PersonBigDataCorpAPIMixin, CompanyBigDataCorpAPIMixin,
    ProcessBigDataCorpAPIMixin, SystemBigDataCorpAPIMixin)


class BigDataCorpAPI(PersonBigDataCorpAPIMixin, CompanyBigDataCorpAPIMixin,
                     ProcessBigDataCorpAPIMixin, SystemBigDataCorpAPIMixin):
    """Client for BigDataCorp REST API endpoints.

    Wraps authentication, dataset discovery, paginated fetches, usage
    reporting, and certificate file downloads behind typed methods with
    domain-specific exceptions.
    """

    def __init__(self, bigdata_auth_token: str) -> None:
        """Initialize the API client.

        Args:
            bigdata_auth_token (str):
                BigDataCorp access token sent as the ``AccessToken`` header.
        """
        super().__init__(bigdata_auth_token=bigdata_auth_token)
