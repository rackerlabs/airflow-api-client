# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0
"""Base class for list commands"""

from typing import Any, List

from airflow_api_client.commands import Command
from airflow_api_client.filters import get_obj_value
from airflow_api_client.typedefs import App


class ListCommandBase(Command):
    """Base class for list commands with common functionality"""

    @classmethod
    def add_common_arguments(cls, parser) -> None:
        """Add common arguments for all list commands"""
        parser.add_argument(
            "--filter",
            "-f",
            action="append",
            help="""
Filter results based on an object's value on the form: 'attr operator value', ie
'is_paused == true' or 'owners !^ "airflow"'. Supported operators:
" ==, !=, >, <, ^ (contains), !^ (not contains).  Repeat for multiple filters.
Note that the value will be serialised using JSON, so use types accordingly. (eg. set `value` to
`null` in order to match attributes with a value of `None` and enclose strings in
quotation marks etc. Supports jmespath-notation to traverse objects.
Note that this functionality is generic and can be used to filter on any
attribute of any underlying object's JSON representation (see --output-format json) for obtaining
the full JSON representation of each object to determine which attributes to filter on.
See the airflow_client Python library docs for more info.
            """,
        )
        parser.add_argument(
            "--extra-attrs",
            "-e",
            type=str,
            action="append",
            help="""
Extra attribute to include in the output.
This will be included in the output alongside the default attributes for each resource. Output
may be modified to better suit tabular representation. Repeat for multiple extra attributes.
Related to notices in --filter. Richer objects such as lists and dictionaries are converted to
comma-separated strings for better readability in table output, for more complex or verbatim
output consider using the --output-format option with json or yaml and parsing the
output with jq/yq or similar. Supports jmespath-notation to select attributes
Example: list -e last_parse_duration -e owners -e default_args.has_on_failure_callback dags
Or see slow DAG Runs: list -f "duration > 10" dag-runs my_dag_id
                  """,
        )

    def _add_extra_attrs(self, app: App, obj: Any, headers: List, attrs: List) -> None:
        """Helper method to add extra attributes to the output based on user input. Modifies headers
        and attrs lists in place.
        """
        for extra_attr in app.args.extra_attrs or []:
            extra_attr_value = get_obj_value(obj, extra_attr)
            # Make lists and dicts more readable in output by converting to comma-separated strings
            if hasattr(extra_attr_value, "__iter__") and not isinstance(extra_attr_value, str):
                extra_attr_value = ", ".join(str(v) for v in extra_attr_value)

            headers.append(extra_attr.replace("_", " ").replace(".", " ").title())
            attrs.append(extra_attr_value)
