"""BigDataCorp API client package.

Provides ``BigDataCorpAPI`` for querying CPF, CNPJ, and process datasets,
plus domain-specific exceptions for API error handling.

Example:
    Import the client from the ``data`` submodule::

        from bigdatacorp_api.data import BigDataCorpAPI

        api = BigDataCorpAPI(bigdata_auth_token="YOUR_TOKEN")
        result = api.get_cpf_dataset(cpf="12345678901", dataset="basic_data")
"""
