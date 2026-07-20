# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-20

Initial release.

### Added

- Command-line interface (`airflow-api-client`) for interacting with Apache
  Airflow 3.x via its REST API.
- `list` commands for DAGs, DAG runs, and task instances, with JMESPath-style
  `--filter` support and result limiting.
- `get` commands for DAG details, DAG run details, task logs, and XCom results.
- `run` command to trigger DAGs, including passing a run configuration via
  `--conf` and payload templates.
- Pause / unpause commands for DAGs.
- Raw API request command for arbitrary endpoints.
- Profile-based configuration via `~/.config/airflow-api-client/config.ini`,
  with support for multiple named profiles selectable with `-P`, and a custom
  config file location via `-C`/`--config-file`.
- `profile create` command for storing connection details, with passwords
  stored securely in the system keyring when the `keyring` extra is installed.
- Multiple credential sources: command line, `AIRFLOW_API_CLIENT_PASSWORD`
  environment variable, and piped stdin.
- Pluggable authentication via the `airflow_api_client.auth` entry point.
- `airflow3` optional dependency extra to install the matching
  `apache-airflow-client` for Airflow 3.x servers.

[Unreleased]: https://github.com/rackerlabs/airflow-api-client/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rackerlabs/airflow-api-client/releases/tag/v0.1.0
