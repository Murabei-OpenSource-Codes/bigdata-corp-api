# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- `pyproject.toml` — corrected distribution name to `bigdata-corp-api` and
  explicit `bigdatacorp_api` package layout under `src/`
- `.gitignore` — ignore `build/` and `*.egg-info/` artifacts

### Removed

- `requirements.txt` — dependency declared in `pyproject.toml`
- Committed build artifacts under `build/` and `dist/`

## [0.18] - 2026-06-05

### Changed

- `BigDataCorpAPI` — extracted `_send_request` helper with shared retry and
  error handling for CPF, CNPJ, and process dataset calls

## [0.17] - 2026-06-03

### Added

- `BigDataCorpAPI` — on-demand datasets (`ondemand_pgfn_*`,
  `ondemand_cert_debt_absence_*`)
- `ONDEMAND_DATABASES` — list of on-demand dataset identifiers

### Changed

- Packaging migrated to Poetry (`pyproject.toml`, `build.sh`); setuptools
  `setup.py` removed
- Docstrings and import ordering in `data.py`

## [0.16.1] - 2025-04-06

### Changed

- `BigDataCorpAPI` — docstrings, typing annotations, and internal refactor
  across dataset fetch methods
- `BigDataCorpAPI.get_usage` — return type annotation

## [0.16] - 2025-02-03

### Added

- `BigDataCorpAPIEmptyEnrichedProcessException`

### Changed

- Exception payloads for CPF queries include `dataset` key

### Fixed

- `BigDataCorpAPI.get_process_dataset` — raise when enriched process result
  is empty

## [0.15] - 2024-10-23

### Added

- `registration_data` dataset for CPF and CNPJ queries

## [0.14] - 2024-09-04

### Added

- `PROCESS_DATABASES`, `list_process_dataset`, `get_process_dataset`, and
  `get_process_datasets` for legal-process queries
- Process datasets: `basic_data`, `cade_processes_data`

## [0.13] - 2024-08-20

### Added

- `MARKETPLACE_DATABASES` and marketplace URL routing for
  `partner_murabei_credit_score_company`

## [0.12] - 2024-08-13

### Added

- `partner_murabei_credit_score_company` CNPJ dataset

## [0.11] - 2024-07-15

### Added

- `government_debtors` dataset for CPF and CNPJ queries

## [0.10] - 2024-03-04

### Added

- Expired BigBoost user detection (`BigDataCorpAPILoginProblemException`
  when login code is `-101`)

## [0.8] - 2023-11-10

### Fixed

- `BigDataCorpAPI` — use HTTP status code instead of JSON status code for
  transport-level error checks in `get_usage`

## [0.7] - 2023-11-10

### Changed

- Version bump only (no source changes from 0.6)

## [0.6] - 2023-08-02

### Added

- `bigdatacorp_api.__init__.py` package marker

## [0.5] - 2023-08-01

### Fixed

- Additional HTTP 500 and API error handling paths

### Changed

- PEP 8 and docstring line wrapping in `get_usage`

## [0.4] - 2023-07-21

### Added

- Domain exceptions mapped from BigData status codes:
  `BigDataCorpAPIInvalidInputException`, `BigDataCorpAPILoginProblemException`,
  `BigDataCorpAPIProblemAPIException`, `BigDataCorpAPIOnDemandQueriesException`,
  `BigDataCorpAPIMonitoringAPIException`, `BigDataCorpAPIUnmappedErrorException`

## [0.3] - 2023-05-31

### Added

- `BigDataCorpAPI.get_usage` — usage reporting for a date range
- `BigDataCorpAPIInvalidDatabaseException`

## [0.2] - 2023-04-05

### Added

- CNPJ support: `CNPJ_DATABASES`, `list_cnpj_dataset`, `get_cnpj_dataset`,
  and extended company dataset catalogue

## [0.1] - 2023-03-31

### Changed

- Tests import exceptions from `bigdatacorp_api.exceptions`

## [0.0] - 2023-03-31

### Added

- Initial release with `BigDataCorpAPI` for CPF dataset discovery and fetch
- Core exceptions in `bigdatacorp_api.exceptions`
- Batch helper `get_cpf_datasets`
