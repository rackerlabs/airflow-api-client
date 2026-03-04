"""Tests for filtering functionality"""

from unittest.mock import MagicMock

import pytest

from airflow_api_client.filters import filter_obj, get_obj_value

# pylint: disable=protected-access,duplicate-code


def test_filter_single_equals():
    """Test filtering with == operator"""
    obj = MagicMock()
    obj.json.return_value = '{"state": "success", "try_number": 1}'

    assert filter_obj(obj, ['state == "success"']) is True
    assert filter_obj(obj, ['state == "failed"']) is False


def test_filter_single_not_equals():
    """Test filtering with != operator"""
    obj = MagicMock()
    obj.json.return_value = '{"state": "success"}'

    assert filter_obj(obj, ['state != "failed"']) is True
    assert filter_obj(obj, ['state != "success"']) is False


def test_filter_greater_than():
    """Test filtering with > operator"""
    obj = MagicMock()
    obj.json.return_value = '{"try_number": 3, "duration": 10.5}'

    assert filter_obj(obj, ["try_number > 2"]) is True
    assert filter_obj(obj, ["try_number > 3"]) is False
    assert filter_obj(obj, ["duration > 10"]) is True


def test_filter_less_than():
    """Test filtering with < operator"""
    obj = MagicMock()
    obj.json.return_value = '{"try_number": 2, "duration": 5.5}'

    assert filter_obj(obj, ["try_number < 3"]) is True
    assert filter_obj(obj, ["try_number < 2"]) is False
    assert filter_obj(obj, ["duration < 10"]) is True


def test_filter_contains():
    """Test filtering with ^ (contains) operator"""
    obj = MagicMock()
    obj.json.return_value = '{"task_id": "my_task_name"}'

    assert filter_obj(obj, ['task_id ^ "task"']) is True
    assert filter_obj(obj, ['task_id ^ "other"']) is False


def test_filter_not_contains():
    """Test filtering with !^ (not contains) operator"""
    obj = MagicMock()
    obj.json.return_value = '{"task_id": "my_task_name"}'

    assert filter_obj(obj, ['task_id !^ "other"']) is True
    assert filter_obj(obj, ['task_id !^ "task"']) is False


def test_filter_multiple_filters_all_match():
    """Test multiple filters where all match"""
    obj = MagicMock()
    obj.json.return_value = '{"state": "success", "try_number": 3, "duration": 5.5}'

    filters = ['state == "success"', "try_number > 2", "duration < 10"]
    assert filter_obj(obj, filters) is True


def test_filter_multiple_filters_one_fails():
    """Test multiple filters where one fails"""
    obj = MagicMock()
    obj.json.return_value = '{"state": "success", "try_number": 3, "duration": 15.5}'

    filters = ['state == "success"', "try_number > 2", "duration < 10"]
    assert filter_obj(obj, filters) is False


def test_filter_nested_attribute():
    """Test filtering on nested attributes"""
    obj = MagicMock()
    obj.json.return_value = '{"config": {"param": "value"}}'

    assert filter_obj(obj, ['config.param == "value"']) is True
    assert filter_obj(obj, ['config.param == "other"']) is False


def test_filter_list_attribute():
    """Test filtering on list attributes with JMESPath"""
    obj = MagicMock()
    obj.json.return_value = '{"tags": [{"name": "prod"}, {"name": "critical"}]}'

    # JMESPath uses [*] not .* for wildcards
    assert filter_obj(obj, ['tags[*].name ^ "prod"']) is True
    assert filter_obj(obj, ['tags[*].name ^ "dev"']) is False


def test_get_obj_value_simple():
    """Test getting simple attribute value"""
    obj = MagicMock()
    obj.json.return_value = '{"state": "success"}'

    assert get_obj_value(obj, "state") == "success"


def test_get_obj_value_nested():
    """Test getting nested attribute value"""
    obj = MagicMock()
    obj.json.return_value = '{"config": {"param": "value"}}'

    assert get_obj_value(obj, "config.param") == "value"


def test_get_obj_value_list():
    """Test getting values from list with JMESPath"""
    obj = MagicMock()
    obj.json.return_value = '{"tags": [{"name": "prod"}, {"name": "dev"}]}'

    result = get_obj_value(obj, "tags[*].name")
    assert result == ["prod", "dev"]


def test_filter_invalid_operator():
    """Test that invalid operator raises ValueError"""
    obj = MagicMock()
    obj.json.return_value = '{"state": "success"}'

    with pytest.raises(ValueError, match="No operator found"):
        filter_obj(obj, ["state = 'success'"])  # Single = is invalid


def test_filter_missing_attribute():
    """Test filtering on missing attribute returns False"""
    obj = MagicMock()
    obj.json.return_value = '{"state": "success"}'

    assert filter_obj(obj, ["nonexistent == 'value'"]) is False


def test_filter_empty_filters():
    """Test that empty filter list returns the object"""
    obj = MagicMock()

    assert filter_obj(obj, []) == obj
    assert filter_obj(obj, None) == obj
