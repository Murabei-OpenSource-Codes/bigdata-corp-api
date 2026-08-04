# bigdata-corp-api

Python client library for BigDataCorp API endpoints used in Murabei data
enrichment and due-diligence workflows.

## Objective and motivation

This package wraps BigDataCorp REST endpoints behind a typed Python API with
domain-specific exceptions and dataset discovery helpers.

### Why this exists

Murabei services query BigDataCorp for CPF, CNPJ, and process data during
enrichment pipelines. A shared client avoids duplicating authentication,
retry logic, and error mapping across workers and applications.

### How it is used

Install the package in workers or apps that call BigDataCorp during batch or
on-demand enrichment. Instantiate `BigDataCorpAPI` with an auth token, then
call dataset list or fetch methods.

### Scope

Owns HTTP communication, dataset constants, and exception types. Credential
storage, orchestration, and downstream persistence live in consuming
services.

## Install

From PyPI (when published):

```bash
pip install bigdata-corp-api
```

From a built wheel or sdist:

```bash
pip install dist/bigdata_corp_api-*.whl
```

From source (requires Poetry or `pip` with `poetry-core`):

```bash
poetry build
pip install dist/bigdata_corp_api-*.whl
```

## Quick start

```python
from bigdatacorp_api.data import BigDataCorpAPI

api = BigDataCorpAPI(bigdata_auth_token="YOUR_TOKEN")

# List available CPF datasets
datasets = api.list_cpf_dataset()

# Fetch one dataset
result = api.get_cpf_dataset(
    cpf="12345678901",
    dataset="basic_data",
)

# Fetch multiple datasets
batch = api.get_cpf_datasets(
    cpf="12345678901",
    datasets=["basic_data", "occupation_data"],
)
```

CNPJ and process queries follow the same pattern via `list_cnpj_dataset`,
`get_cnpj_dataset`, `list_process_dataset`, and `get_process_dataset`.

Usage reporting for a date range:

```python
usage = api.get_usage(
    initial_date="2025-01-01",
    final_date="2025-01-31",
)
```

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `BIGDATA_AUTH_TOKEN` | Yes (for tests) | BigDataCorp API token passed to `BigDataCorpAPI` |
| `TEST_CPF` | Tests only | Valid CPF used by integration tests |

The token is supplied at construction time; this package does not read
environment variables during normal API calls.

## Development

Build a release (bumps patch in `VERSION`, renders `pyproject.toml`, tags):

```bash
./build.sh
```

Run tests (integration tests call the live API when env vars are set):

```bash
export BIGDATA_AUTH_TOKEN="your-token"
export TEST_CPF="valid-cpf"
python -m unittest discover -s src/bigdatacorp_api/tests
```

Lint with Ruff (configuration in `pyproject.toml`):

```bash
ruff check src/
```

## License

BSD-3-Clause — see [LICENSE](LICENSE).
