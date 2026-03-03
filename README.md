# Airflow API Client

A command-line tool for interacting with Apache Airflow 3.x via its REST API.

## Installation

```bash
pip install airflow-api-client
```

Or install from source:

```bash
git clone https://github.com/yourusername/airflow-api-client.git
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
airflow-api-client -P production list
```

You may also specify a custom config file location with `-C` or `--config-file`:

```bash

## Quick Start

If no configuration file is used, the default required inputs are:
--endpoint, --username, and --password. (You can also set these via environment variables as
described below.)

```bash
# List all DAGs
airflow-api-client list

# Run a DAG
airflow-api-client run my_dag --conf '{"key": "value"}'

# Get XCom results
airflow-api-client xcom my_dag:run_id:task_id:return_value

# Create a payload template
airflow-api-client create-payload my_dag
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
library (e.g. `github`, `grid`, `fancy_grid`, etc.). If not set, it defaults to `simple`.

### Command Line

```bash
airflow-api-client -e https://airflow.example.com -u username -p password list
```

## Commands

### list

List all available DAGs:

```bash
airflow-api-client list
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
```

### xcom

Retrieve XCom entries:

```bash
# All XComs for a DAG run
airflow-api-client xcom my_dag:manual__123456

# All XComs for a specific task
airflow-api-client xcom my_dag:manual__123456:my_task

# Specific XCom key
airflow-api-client xcom my_dag:manual__123456:my_task:return_value
```

The positional argument for the `xcom` command is in the format `dag_id:run_id:task_id:key`, where
`task_id`, and `key` are optional. If only `dag_id` and `run_id` are provided, it will list all
XComs for that DAG run. If `dag_id`, `run_id`, and `task_id` are provided, it will list all XComs
for that task. If all four components are provided, it will retrieve the specific XCom value.

### create-payload

Generate a payload template for a DAG:

```bash
airflow-api-client create-payload my_dag > payload.json
```

The `run` sub-comand accepts payloads in files containing JSON. This command can generate a payload
based on the DAG's parameters, which can then be supplied to the `run` command by prefixing the
`--conf` argument with `@`:

```bash
airflow-api-client create-payload my_dag --output-file payload.json
airflow-api-client run my_dag --conf @payload.json
```

## Output Formats

Control output format with `-f`:

```bash
# Human-readable (default)
airflow-api-client xcom my_dag:run_id:task_id

# JSON
airflow-api-client -f json xcom my_dag:run_id:task_id

# YAML (available if PyYAML is installed)
airflow-api-client -f yaml xcom my_dag:run_id:task_id
```

## Verbosity

Control logging verbosity:

```bash
# Normal (warnings and errors)
airflow-api-client list

# Info level
airflow-api-client -v list

# Debug level
airflow-api-client -vv list

# Debug with HTTP traffic
airflow-api-client -vvv list
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

```pythonpython
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

```python# my_auth.py
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
airflow-api-client --auth-provider myauth list
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
git clone https://github.com/yourusername/airflow-api-client.git
cd airflow-api-client
uv sync --extra dev

# Run linters
uv run black airflow_api_client/
uv run isort airflow_api_client/
uv run pylint airflow_api_client/
uv run flake8 airflow_api_client/

# Run the CLI
uv run airflow-api-client --help
```

## License

Apache-2.0

## Requirements

- Python 3.10+
- Apache Airflow 3.1+
