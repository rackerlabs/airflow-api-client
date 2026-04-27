# Airflow API Client

A command-line tool for interacting with Apache Airflow 3.x via its REST API.

## Installation

```bash
pip install airflow-api-client
```

Or install from source:

```bash
git clone https://github.com/rackerlabs/airflow-api-client.git
cd airflow-api-client
uv sync
```

## Configuration

All required inputs can be supplied on the command line.
However, for convenience, you can also create profiles in a config file to avoid having to specify
common parameters repeatedly.

Create a config file at `~/.config/airflow-api-client/config.ini`:

```ini
[default]
endpoint = https://airflow.example.com
username = your_username
password = your_password
verbose = 0

[production]
endpoint = https://airflow.prod.example.com
username = prod_user
```

Use profiles with `-P`:

```bash
airflow-api-client -P production list dags
```

You may also specify a custom config file location with `-C` or `--config-file`:

```bash
airflow-api-client -C /path/to/config.ini list dags
```

### Creating Profiles

Profiles can be created using the `profile create` command:

```bash
# Create a profile (password stored securely in system keyring if available)
airflow-api-client profile create -e https://airflow.prod.example.com -u my_user

# Create a named profile
airflow-api-client profile create --profile-name production -e https://airflow.prod.example.com -u my_user

# Force plaintext password storage (not recommended)
airflow-api-client profile create -e https://airflow.prod.example.com -u my_user --no-keyring-password

# Pipe password from another source
echo "my_password" | airflow-api-client profile create -e https://airflow.prod.example.com -u my_user

# Use AIRFLOW_API_CLIENT_PASSWORD environment variable
AIRFLOW_API_CLIENT_PASSWORD=secret airflow-api-client profile create -e https://airflow.prod.example.com -u my_user
```

When run interactively, the command will prompt for any missing fields (endpoint, username,
password) with hidden input for the password.

### Keyring Support

Passwords can be stored securely in the system keyring (e.g. GNOME Keyring, macOS Keychain)
instead of plaintext in the config file. Install with:

```bash
pip install airflow-api-client[keyring]
```

When keyring is installed, `profile create` will automatically store passwords in the system
keyring. Use `--no-keyring-password` to force plaintext storage in the config file instead.

When authenticating, if no password is provided via `--password`, environment variable, or config
file, the client will automatically check the system keyring as a fallback.

## Quick Start

If no configuration file is used, the default required inputs are:
`--endpoint`, `--username`, and `--password`. (You can also set these via environment variables as
described below.)

```bash
# List all DAGs
airflow-api-client list dags

# List DAG runs for a specific DAG
airflow-api-client list dag-runs my_dag

# Get details about a specific DAG
airflow-api-client get dag my_dag

# Get logs for a DAG run
airflow-api-client get log my_dag/run_id

# Run a DAG
airflow-api-client run my_dag --conf '{"key": "value"}'

# Pause / unpause a DAG
airflow-api-client pause my_dag
airflow-api-client unpause my_dag

# Get XCom results
airflow-api-client xcom my_dag/run_id/task_id/return_value

# Create a payload template
airflow-api-client create-payload my_dag

# Make a raw API request
airflow-api-client raw GET /api/v2/dags
```

## Authentication

The default authentication provider uses username/password to obtain JWT tokens through
the standard /auth/token endpoint.

See below for custom authentication providers if you need to support other auth mechanisms.

### Environment Variables

Certain environment variables are supported for fallback configuration.

```bash
export AIRFLOW_API_CLIENT_ENDPOINT=https://airflow.example.com
export AIRFLOW_API_CLIENT_PASSWORD=your_password
export AIRFLOW_API_CLIENT_USERNAME=your_username
export AIRFLOW_API_CLIENT_TOKEN=your_jwt_token
```

The order of precedence for configuration is:
1. Command-line arguments
2. Environment variables
3. Profile from config file

Additionally, the output format for the human readable output can be configured using
`AIRFLOW_API_CLIENT_TABLE_FORMAT` which must be one of the formats supported by the `tabulate`
library (e.g. `github`, `grid`, `fancy_grid`, etc.). If not set, it defaults to `grid`.

### Command Line

```bash
airflow-api-client -e https://airflow.example.com -u username -p password list dags
```

## Commands

### list

List Airflow resources. Supports subcommands for different resource types.

```bash
# List all DAGs
airflow-api-client list dags

# List DAG runs for a specific DAG (most recent first, default limit: 10)
airflow-api-client list dag-runs my_dag
airflow-api-client list dag-runs my_dag --limit 20

# List task instances for a DAG run
airflow-api-client list task-instances my_dag/run_id
```

All list subcommands support filtering with `--filter`:

```bash
airflow-api-client list dags --filter "is_paused == true"
airflow-api-client list dag-runs my_dag --filter "state == success"
```

### get

Get detailed information about a specific resource.

```bash
# Get DAG details
airflow-api-client get dag my_dag

# Get DAG run details
airflow-api-client get dag-run my_dag/run_id

# Get task instance details
airflow-api-client get task-instance my_dag/run_id/task_id

# Get logs for all tasks in a DAG run
airflow-api-client get log my_dag/run_id

# Get log for a specific task
airflow-api-client get log my_dag/run_id/task_id

# Get log for a specific try number
airflow-api-client get log my_dag/run_id/task_id --try-number 2

# Custom timestamp format for human output
airflow-api-client get log my_dag/run_id --time-format "%H:%M:%S"
```

### run

Trigger a DAG run:

```bash
# Interactive mode (prompts for parameters, if any are required)
airflow-api-client run my_dag

# With configuration
airflow-api-client run my_dag --conf '{"param": "value"}'

# From file
airflow-api-client run my_dag --conf @config.json

# With a custom run ID
airflow-api-client run my_dag --run-id my_custom_run_id

# Trigger without waiting for completion
airflow-api-client run my_dag --no-wait
```

### pause / unpause

Pause or unpause a DAG:

```bash
airflow-api-client pause my_dag
airflow-api-client unpause my_dag
```

### xcom

Retrieve XCom entries:

```bash
# All XComs for a DAG run
airflow-api-client xcom my_dag/run_id

# All XComs for a specific task
airflow-api-client xcom my_dag/run_id/my_task

# Specific XCom key
airflow-api-client xcom my_dag/run_id/my_task/return_value
```

The positional argument for the `xcom` command is in the format `dag_id/run_id/task_id/key`, where
`task_id` and `key` are optional. If only `dag_id` and `run_id` are provided, it will list all
XComs for that DAG run. If `dag_id`, `run_id`, and `task_id` are provided, it will list all XComs
for that task. If all four components are provided, it will retrieve the specific XCom value.

### create-payload

Generate a payload template for a DAG:

```bash
airflow-api-client create-payload my_dag > payload.json
```

The `run` sub-command accepts payloads in files containing JSON. This command can generate a payload
based on the DAG's parameters, which can then be supplied to the `run` command by prefixing the
`--conf` argument with `@`:

```bash
airflow-api-client create-payload my_dag --output-file payload.json
airflow-api-client run my_dag --conf @payload.json
```

### raw

Make raw API requests directly to Airflow. This is a power-user command that gives full control
over the API interaction. The URL must include the full API path (e.g. `/api/v2/dags`).

```bash
# GET request
airflow-api-client raw GET /api/v2/dags

# POST with payload
airflow-api-client raw POST /api/v2/dags/my_dag/dagRuns --payload '{"dag_run_id": "test", "conf": {}}'

# With custom headers
airflow-api-client raw GET /api/v2/dags --header "X-Custom=value"

# PATCH request
airflow-api-client raw PATCH /api/v2/dags/my_dag --payload '{"is_paused": true}'
```

Supported methods: `GET`, `POST`, `PUT`, `PATCH`, `DELETE`. Output defaults to JSON.

## Output Formats

Control output format with `-f`:

```bash
# Human-readable (default)
airflow-api-client list dags

# JSON
airflow-api-client -f json list dags

# YAML (available if PyYAML is installed)
airflow-api-client -f yaml list dags
```

The human-readable table format can be customised by setting the `AIRFLOW_API_CLIENT_TABLE_FORMAT`
environment variable to any format supported by the
[tabulate](https://github.com/astanin/python-tabulate) library (e.g. `simple`, `github`,
`fancy_grid`, `psql`, `orgtbl`, etc.). Defaults to `grid`.

## Verbosity

Control logging verbosity:

```bash
# Default (info and above)
airflow-api-client list dags

# Debug level
airflow-api-client -v list dags

# Debug with HTTP traffic
airflow-api-client -vv list dags
```

Do note that all logging takes place on stderr by default, so that the output can be easily piped
to other tools without mixing with logs.

## Custom Authentication Providers

Create a custom auth provider by subclassing `AirflowAPIClientAuthProvider`:

If, for example, you have an external system to obtain tokens from, you can implement a custom
auth provider like this:

```python
# my_auth.py
from airflow_api_client.auth import AirflowAPIClientAuthProvider

class MyAuthProvider(AirflowAPIClientAuthProvider):
    @property
    def client_token(self):
        # Your custom token logic
        return self.get_token_from_somewhere()
```

Or if you want to implement a provider that obtains the password from a secret manager:

```python
# my_auth.py
from airflow_api_client.auth import AirflowAPIClientAuthProvider

class MyAuthProvider(AirflowAPIClientAuthProvider):
    @property
    def password(self):
        # Your custom password retrieval logic
        return self.get_password_from_secret_manager()
```

Any arguments your custom auth provider needs can be added to the parser by overriding the
`add_arguments` class method:

```python
# my_auth.py
from airflow_api_client.auth import AirflowAPIClientAuthProvider

class MyAuthProvider(AirflowAPIClientAuthProvider):
    @classmethod
    def add_arguments(cls, profile: str, parser: "ArgumentParser", config: "RawConfigParser"):
        # Add custom arguments to the parser
        parser.add_argument("--my-arg", help="An argument for my auth provider")

    @property
    def client_token(self):
        # Your custom token logic that can use self.args.my_arg
        return self.get_token_using_arg(self.args.my_arg)
```

Register it via entry points in `pyproject.toml`:

```toml
[project.entry-points."airflow_api_client.auth"]
myauth = "my_package.my_auth:MyAuthProvider"
```

Use it:

```bash
airflow-api-client --auth-provider myauth list dags
```

## Custom Commands

Add custom commands by creating a plugin:

```python
# my_commands.py
from airflow_api_client.commands import Command

class MyCommand(Command):
    name = "mycommand"
    help = "My custom command"

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument("--option", help="An option")

    def execute(self, app):
        print("Executing my command")
```

Here `parser` is a command-specific argument parser that you can add arguments to, and `app` is the
main application instance which has a reference to the API client `client` and the parsed CLI `args`.
See typedefs.py for details.

Register via entry points:

```toml
[project.entry-points."airflow_api_client.commands"]
mycommand = "my_package.my_commands:MyCommand"
```

## Development

```bash
# Clone and install
git clone https://github.com/rackerlabs/airflow-api-client.git
cd airflow-api-client
uv sync --extra dev

# Run linters
uv run black airflow_api_client/
uv run isort airflow_api_client/
uv run pylint airflow_api_client/
uv run flake8 airflow_api_client/

# Run tests
uv run pytest

# Run the CLI
uv run airflow-api-client --help
```

## How is this different from airflowctl?

The Apache Airflow project maintains a separate CLI tool,
[airflowctl](https://github.com/apache/airflow/tree/main/airflow-ctl) (`pip install
apache-airflow-ctl`), which is auto-generated from the Airflow OpenAPI spec. It provides
comprehensive coverage of the REST API but requires familiarity with the API endpoints and their
parameters.

This tool takes a different approach — it's a workflow-oriented tool designed for day-to-day
operations without needing intimate knowledge of the Airflow API:

- `run my_dag` interactively prompts for parameters based on the DAG's schema
- `create-payload` introspects DAG parameters and generates a payload template
- `xcom my_dag/run_id/task_id` uses natural path-style identifiers instead of API query parameters
- `run` polls for completion and displays logs and XCom results automatically
- Config file profiles let you switch between environments with `-P production`
- Pluggable auth providers and custom commands via entry points
- Human-readable output with filtering (`--filter "state == success"`)

In short: `airflowctl` is a CLI skin over the REST API, this tool is a workflow tool that
use the REST API.

## License

Apache-2.0

## Requirements

- Python 3.10+
- Apache Airflow 3.1+
