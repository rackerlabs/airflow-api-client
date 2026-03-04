"""Code related to filtering objects based on user-provided criteria"""

import json
import operator
import re

import jmespath

from airflow_api_client.log import logger

SUPPORTED_OPERATORS = {
    "!=": operator.ne,
    "==": operator.eq,
    ">": operator.gt,
    "<": operator.lt,
    "!^": lambda a, b: not operator.contains(a, b),
    "^": operator.contains,
}

# Ensure that we get the correct operator when splitting filter
# strings (ie make sure !^ is matched before ^)
OPERATORS = [re.escape(op) for op in sorted(SUPPORTED_OPERATORS.keys(), key=len, reverse=True)]


def get_obj_value(obj, attr):
    """Get the value of an attribute from an object, supporting nested attributes using dot
    notation. Supports calling to_dict/json methods and accessing dict keys.

    Examples:
        tags.name -> access name on each tag in list
        tags.to_dict.name -> call to_dict() on each tag, then get name from dict
        config.to_dict.nested.value -> call to_dict(), then access nested.value
    """

    data = json.loads(obj.json()) if hasattr(obj, "json") else obj.to_dict()
    ret = jmespath.search(attr.strip(), data)
    logger.debug(f"Getting attribute {attr} from object {obj} (data: {data}) = {ret}")
    return ret


def validate_filter(filter_str) -> tuple[str, str, str]:
    """Validate that filters are in the correct format and contain supported operators"""

    match = re.search("(" + "|".join(OPERATORS) + ")", filter_str)
    if not match:
        raise ValueError(f"No operator found in {filter_str}")

    op = match.group(1)
    attr, value = filter_str.split(op, 1)
    return attr.strip(), op, value.strip()


def filter_obj(obj, filters) -> bool:
    """Take a list of strings indicating a filter, ie "attribute!='value'" and return True if the
    object matches all filters"""
    if not filters:
        return obj

    for filter_str in filters:
        attr, op_str, value = validate_filter(filter_str)
        attr, value = filter_str.split(op_str, 1)
        attr = attr.strip()
        value = value.strip()
        try:
            value = json.loads(value) if value is not None else value
        except json.JSONDecodeError:
            logger.warning(f"Could not parse value {value} as JSON, treating as string")

        value = None if value == "None" else value
        obj_value = get_obj_value(obj, attr)
        if obj_value is None:
            logger.warning(f"Object {obj} does not have attribute {attr}")
            return False
        op_func = SUPPORTED_OPERATORS[op_str]
        logger.debug(
            f"Comparing {attr}: {obj_value} ({type(obj_value)}) {op_str}"
            f" {value} = {op_func(obj_value, value)}"
        )
        if not op_func(obj_value, value):
            return False

        logger.debug(f"Object {obj} matches filter {filter_str}")
    return True
