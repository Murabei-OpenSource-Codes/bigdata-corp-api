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

Owns HTTP communication, dataset constants, exception types, retry and
pagination, parallel batch fetches, usage reporting, and certificate
file downloads from on-demand responses. Credential storage,
orchestration, and downstream persistence live in consuming services.

## Install

From PyPI (when published):

```bash
pip install bigdata-corp-api
```

From a built wheel or sdist:

```bash
pip install dist/bigdata_corp_api-*.whl
```

From source (requires Poetry):

```bash
pip install poetry
export VERSION=$(grep -E '^VERSION=' VERSION | cut -d'=' -f2-)
sed -e 's#{VERSION}#'"${VERSION}"'#g' pyproject_template.toml > pyproject.toml
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

# Append dataset_params to the Datasets field
result = api.get_cnpj_dataset(
    cnpj="00000000000191",
    dataset="processes",
    dataset_params="{NextPageId}.limit(500)",
)

# Fetch multiple datasets in parallel
batch = api.get_cpf_datasets(
    cpf="12345678901",
    datasets=["basic_data", "occupation_data"],
)

# Soft-fail non-critical errors; per-dataset q and Datasets suffixes
batch = api.get_cpf_datasets(
    cpf="12345678901",
    datasets=["basic_data", "processes"],
    skip_errors=True,
    query_params={"processes": ", returnupdates{false}"},
    dataset_params={"processes": "{NextPageId}.limit(500)"},
)
# Soft failures are under batch["__errors__"]
```

CNPJ and process queries follow the same pattern via `list_cnpj_dataset`,
`get_cnpj_dataset`, `list_process_dataset`, and `get_process_dataset`.
Batch helpers accept `skip_errors`, a `query_params` mapping (suffix for
`q`), and a `dataset_params` mapping (suffix for `Datasets`).

Usage reporting for a date range:

```python
usage = api.get_usage(
    initial_date="2025-01-01",
    final_date="2025-01-31",
)
```

Download a certificate file from an on-demand dataset response:

```python
cert_response = api.get_cpf_dataset(
    cpf="12345678901",
    dataset="ondemand_cert_debt_absence_by_state_person",
)
file_data = api.get_result_file(
    dataset="ondemand_cert_debt_absence_by_state_person",
    json_data=cert_response,
)
# file_data["file_type"] and file_data["file_content"]
```

## Configuration

The auth token is passed to `BigDataCorpAPI` at construction time. Timeout
and batch parallelism are read from the environment when set.

| Variable | Required | Description |
|----------|----------|-------------|
| `BIGDATACORP__REQUEST_TIMEOUT` | No | HTTP timeout in seconds (default `60`) |
| `BIGDATACORP__N_PARALLEL` | No | Max threads for batch fetches (default `8`) |
| `BIGDATA_AUTH_TOKEN` | Tests | Token for integration tests |
| `TEST_CPF` | Tests | Valid CPF for integration tests |
| `TEST_CNPJ` | Tests | Valid CNPJ for integration tests |

## Development

Build a release (bumps patch in `VERSION`, renders `pyproject.toml`, tags,
and pushes):

```bash
./build.sh
```

Run tests (integration tests call the live API when env vars are set):

```bash
export BIGDATA_AUTH_TOKEN="your-token"
export TEST_CPF="valid-cpf"
export TEST_CNPJ="valid-cnpj"
python -m unittest discover -s src/bigdatacorp_api/tests
```

Lint with Ruff (configuration in `pyproject.toml`):

```bash
ruff check src/
```

CI on `main` builds from `VERSION` and `pyproject_template.toml`, then
publishes to PyPI when `PYPI_API_TOKEN` is configured.

## License

BSD-3-Clause — see [LICENSE](LICENSE).
