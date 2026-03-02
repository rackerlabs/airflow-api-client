"""Utility functions for outputting data in various formats (JSON, YAML, human-readable tables)."""

import json
import os

from tabulate import tabulate

try:
    import yaml
except ImportError:
    yaml = None


def output(data, args, headers=None):
    """Outputs data in the specified format (JSON, YAML, or human-readable table)."""
    if args.output_format == "json":
        print(json.dumps(data, indent=4, sort_keys=True, default=str))
    elif args.output_format == "yaml":

        print(yaml.dump(data, sort_keys=False))
    elif headers:  # human output, with tabular data
        print(
            tabulate(
                data,
                headers=headers,
                tablefmt=os.getenv("AIRFLOW_API_CLIENT_TABLE_FORMAT", "simple"),
            )
        )
    else:  # human output, raw/unformatted data
        print(data)
