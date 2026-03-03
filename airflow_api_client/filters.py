"""Code related to filtering objects based on user-provided criteria"""

import json
import operator

from airflow_api_client.log import logger

SUPPORTED_OPERATORS = {
    "!=": operator.ne,
    "==": operator.eq,
    ">": operator.gt,
    "<": operator.lt,
    "^": operator.contains,
}


def get_obj_value(obj, attr):
    """Get the value of an attribute from an object, supporting nested attributes using dot
    notation. Prefer to use upstream .json() methods to benefit from the typing/serialisation logic
    already implemented there, but fall back to getattr if the attribute is not present on the
    .json() output"""

    if "." in attr:
        first_attr, rest = attr.split(".", 1)
        first_value = get_obj_value(obj, first_attr)
        if first_value is None:
            logger.warning(f"Object {obj} does not have attribute {first_attr}")
            return None
        return get_obj_value(first_value, rest)

    ret = None
    # Most, if not all, airflow_client objects have a .json() method that returns a dict
    # representation of the object with all the relevant attributes, so we should prefer to use that
    # if available to get the value for filtering, rather than using getattr on the object which may
    # not have the attribute directly accessible or may have it in a different format (eg. an enum
    # that we want to compare using its value)
    if hasattr(obj, "json") and callable(getattr(obj, "json")):
        obj_dict = json.loads(obj.json())
        if attr in obj_dict:
            ret = obj_dict[attr]
    else:
        ret = getattr(obj, attr, None)

    if not callable(ret) or isinstance(ret, str):
        return ret

    try:
        return ret()
    except TypeError as ex:
        raise ValueError(
            f"Attribute {attr} on object {obj} is a callable that requires arguments, cannot"
            " call without arguments to get value for filtering"
        ) from ex


def validate_filters(filters):
    """Validate that filters are in the correct format and contain supported operators"""
    for filter_str in filters:
        if not any(op in filter_str for op in SUPPORTED_OPERATORS):
            raise ValueError(
                f"Filter {filter_str} does not contain a supported operator. Supported operators"
                f" are: {', '.join(list(SUPPORTED_OPERATORS.keys()))}"
            )


def filter_obj(obj, filters):
    """Take a list of strings indicating a filter, ie "attribute!='value'" and return True if the
    object matches all filters"""
    if not filters:
        return obj

    for filter_str in filters:
        logger.debug(f"Processing filter: {filter_str}")
        for op_str, op_func in SUPPORTED_OPERATORS.items():
            if op_str in filter_str:
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
                logger.debug(
                    f"Comparing: {obj_value} {op_str} {value} = {op_func(obj_value, value)}"
                )
                if not op_func(obj_value, value):
                    logger.debug(
                        f"Object {obj} does not match filter {filter_str} obj_value {obj_value}"
                        f" (attribute value: {obj_value})"
                    )
                    return False
                break

        logger.debug(f"Object {obj} matches filter {filter_str}")
    return True
