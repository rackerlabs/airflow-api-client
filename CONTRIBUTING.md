# Contributing to Airflow API Client

Thank you for your interest in contributing. This document covers how to set up a development
environment, the coding standards we follow, and the process for submitting changes.

> **Note:** This file has been reviewed and approved by Rackspace Legal. Do not modify the
> DCO section or the contribution process without seeking Legal review again.

## Developer Certificate of Origin (DCO)

This project uses the [Developer Certificate of Origin (DCO)](https://developercertificate.org/)
rather than a Contributor Licence Agreement (CLA). By submitting a pull request you certify that
you have the right to contribute the code and that you agree to the DCO terms.

Every commit must include a `Signed-off-by` trailer with your real name and email address:

```
Signed-off-by: Jane Smith <jane.smith@example.com>
```

Add it automatically with:

```bash
git commit -s -m "your commit message"
```

If you forget on a previous commit:

```bash
git commit --amend -s --no-edit
```

Pull requests with unsigned commits will not be merged.

## Development setup

Requires Python 3.10+ and [uv](https://github.com/astral-sh/uv).

```bash
git clone https://github.com/rackerlabs/airflow-api-client.git
cd airflow-api-client
uv sync --extra dev
```

Install pre-commit hooks (runs Black, isort, and Pylint before each commit):

```bash
uv run pre-commit install
```

## Code style

| Tool | Purpose |
|---|---|
| [Black](https://black.readthedocs.io/) | Formatting (line length 100) |
| [isort](https://pycqa.github.io/isort/) | Import ordering |
| [Pylint](https://pylint.readthedocs.io/) | Linting |
| [Flake8](https://flake8.pycqa.org/) | Additional style checks |

Run all checks manually:

```bash
uv run black airflow_api_client/ tests/
uv run isort airflow_api_client/ tests/
uv run pylint --rcfile=pyproject.toml airflow_api_client/
uv run flake8 airflow_api_client/ tests/
```

The pre-commit hooks run Black, isort, and Pylint automatically on `git commit`.

## Running tests

```bash
uv run pytest tests/
```

## Submitting a pull request

1. Fork the repository and create a branch from `main`.
2. Make your changes with signed-off commits (see DCO above).
3. Ensure all tests pass and linters report no errors.
4. Open a pull request against `main`. CI must pass before the PR can be merged.
5. A maintainer will review your PR and may request changes.

Please keep pull requests focused — one logical change per PR makes review faster.

The current maintainers are listed in [MAINTAINERS](MAINTAINERS).

## Reporting bugs

Open a GitHub issue with steps to reproduce, the command you ran, and the full error output.
