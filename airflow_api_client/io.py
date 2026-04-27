# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0
"""Utility functions for outputting data in various formats (JSON, YAML, human-readable tables)."""

import json
import os
import shutil
import sys
from typing import Any, Dict

from tabulate import tabulate

from airflow_api_client.log import logger

try:
    import yaml
except ImportError:
    yaml = None


def output(data, args, headers=None, maxcolwidths=None, tablefmt=None):
    """Outputs data in the specified format (JSON, YAML, or human-readable table)."""
    if args.output_format == "json":
        print(json.dumps(data, indent=4, sort_keys=False, default=str))
    elif args.output_format == "yaml":

        print(yaml.dump(data, sort_keys=False))
    elif headers is not None:  # human output, with tabular data
        if not data:
            logger.debug("No data to display")
            return
        try:
            padding = len(data[0][0]) + 24
        except (IndexError, KeyError):
            padding = 24
        maxcolwidths = maxcolwidths or shutil.get_terminal_size((80, 20)).columns - padding

        # missingval is applied AFTER maxcolwidths is calculated in tabulate, and it raises
        # a TypeError if it encounters None values, so we need to replace them with empty strings
        # here before passing the data to tabulate
        if all(isinstance(row, dict) for row in data):
            data = [{k: "" if v is None else v for k, v in d.items()} for d in data]
        else:
            data = [[c if c is not None else "" for c in row] for row in data]

        print(
            tabulate(
                data,
                headers=headers,
                tablefmt=tablefmt or os.getenv("AIRFLOW_API_CLIENT_TABLE_FORMAT", "grid"),
                maxcolwidths=maxcolwidths,
            )
        )
    else:  # human output, raw/unformatted data
        print(data)


class PayloadConstructor:
    """Helper class to construct payloads based on OpenAPI schema definitions."""

    def __init__(self, params: Dict[str, Any]) -> None:
        self.params = params

    def construct_payload(self) -> Dict[str, Any]:
        """Constructs a payload by prompting the user for input based on the schema."""
        payload = {}
        for key, param_def in self.params.items():
            schema = param_def.get("schema", {})

            # Show parameter info
            sys.stdout.write(f"\nParameter: {key}\n")
            if "description" in param_def:
                sys.stdout.write(f"Description: {param_def['description']}\n")
            if "default" in param_def:
                sys.stdout.write(f"Default: {json.dumps(param_def['default'])}\n")

            payload[key] = self.prompt_value(key, schema)

        return payload

    def prompt_object(
        self, name: str, schema: Dict[str, Any], prefix: str, indent: int
    ) -> Dict[str, Any]:
        """Prompt for an object value, handling nested properties if defined."""
        while True:
            properties = schema.get("properties", {})
            if properties:
                sys.stderr.write(f"{prefix}Enter values for object '{name}':\n")
                obj = {}
                for prop_name, prop_schema in properties.items():
                    obj[prop_name] = self.prompt_value(prop_name, prop_schema, indent + 1)
                return obj
            # No properties defined, ask for JSON
            sys.stderr.write(f"{prefix}Enter JSON object for '{name}': ")
            try:
                return json.loads(input().strip())
            except json.JSONDecodeError as ex:
                logger.error(f"{prefix}Error: Invalid JSON - {ex}\n")
                continue

    def prompt_array(self, name: str, schema: Dict[str, Any], prefix: str, indent: int) -> list:
        """Prompt for an array value, handling item types and nested objects if defined."""
        items_schema = schema.get("items", {"type": "string"})
        item_type = items_schema.get("type", "string")

        # Special handling for arrays of objects with properties
        if item_type == "object" and "properties" in items_schema:
            sys.stderr.write(f"{prefix}Enter values for array '{name}' (empty line to finish):\n")
            arr = []
            idx = 1
            while True:
                sys.stderr.write(f"{prefix}  Add item {idx}? (y/n): ")
                if input().strip().lower() not in ("y", "yes"):
                    break
                arr.append(self.prompt_value(f"item {idx}", items_schema, indent + 1))
                idx += 1
            return arr

        # Standard array handling
        sys.stderr.write(f"{prefix}Enter values for array '{name}' (empty line to finish):\n")
        arr = []
        idx = 1
        while True:
            sys.stderr.write(f"{prefix}  [{idx}]: ")
            val = input().strip()
            if not val:
                break
            # Parse based on item type
            if item_type == "integer":
                arr.append(int(val))
            elif item_type == "number":
                arr.append(float(val))
            elif item_type == "boolean":
                arr.append(val.lower() in ("true", "1", "yes"))
            elif item_type == "object":
                arr.append(json.loads(val))
            else:
                arr.append(val)
            idx += 1
        return arr

    def prompt_value(self, name: str, schema: Dict[str, Any], indent: int = 0) -> Any:
        """Recursively prompt for a value based on schema type."""

        # pylint: disable=too-many-return-statements,too-many-branches
        prefix = "  " * indent
        value_type = schema.get("type", "string")
        types = value_type if isinstance(value_type, list) else [value_type]

        # Handle object type
        if "object" in types:
            return self.prompt_object(name, schema, prefix, indent)

        # Handle array type
        if "array" in types:
            return self.prompt_array(name, schema, prefix, indent)

        # Handle simple types - loop until valid input
        while True:
            description = schema.get("description", "")
            if description:
                sys.stderr.write(f"{prefix}{description}\n")
            sys.stderr.write(f"{prefix}Enter value for '{name}' ({', '.join(types)}): ")
            user_input = input().strip()

            if not user_input:
                if "null" in types:
                    return None
                if "default" in schema:
                    return schema["default"]
                logger.error(f"{prefix}Error: Parameter '{name}' requires a value\n")
                continue

            # Try type conversion
            for t in types:
                try:
                    if t == "null" and user_input.lower() in ("null", "none"):
                        return None
                    if t == "boolean":
                        if user_input.lower() in ("true", "1", "yes"):
                            return True
                        if user_input.lower() in ("false", "0", "no"):
                            return False
                        continue
                    if t == "integer":
                        return int(user_input)
                    if t == "number":
                        return float(user_input)
                    if t == "string":
                        return user_input
                except ValueError:
                    continue

            # If we get here, no type matched
            logger.error(
                f"{prefix}Error: Invalid value '{user_input}'."
                f" Expected one of: {', '.join(types)}\n"
            )
