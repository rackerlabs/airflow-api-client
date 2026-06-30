# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0

"""Compatibility patches for airflow_client model validation.

airflow-client is generated from OpenAPI specs, and the generated models use Pydantic for
validation. Given that we need to support a wide range of Airflow server versions, we need to be
lenient about validation. Not all fields are guaranteed to be present in the server response, and
some fields may be null. We might not need any of these fields, so we don't want to crash the tool
if they are missing or null. We just want to ignore the validation error and construct the model
without validation, and deal with any data issues later if we actually need to use the missing
fields.

These patches make every model fall back to
model_construct (no validation) so the tool keeps working rather than crashing.
"""

import importlib
import inspect
import pkgutil

from pydantic import ValidationError

from airflow_api_client.log import logger

_COMPAT_MODE_LOGGED = False


def _patch_model(cls):
    original_func = cls.model_validate.__func__

    @classmethod
    def lenient_model_validate(klass, obj, *args, **kwargs):
        global _COMPAT_MODE_LOGGED  # pylint: disable=global-statement
        try:
            return original_func(klass, obj, *args, **kwargs)
        except ValidationError as e:
            if not _COMPAT_MODE_LOGGED:
                logger.debug(
                    "Mismatch between server version and apache-airflow-client library."
                    " Running in compatibility mode, ignoring validation errors.",
                    model=klass.__name__,
                    error=str(e),
                )
                _COMPAT_MODE_LOGGED = True
            if not isinstance(obj, dict):
                raise
            # pylint: disable=no-member
            known = set(klass.model_fields.keys())
            return klass.model_construct(**{k: v for k, v in obj.items() if k in known})

    cls.model_validate = lenient_model_validate


def patch_airflow_client_models():
    """Patch all airflow_client models to ignore validation errors."""
    import airflow_client.client.models as models_pkg  # pylint: disable=import-outside-toplevel

    for _importer, modname, _ispkg in pkgutil.iter_modules(models_pkg.__path__):
        module = importlib.import_module(f"airflow_client.client.models.{modname}")
        for _name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                hasattr(obj, "from_dict")
                and hasattr(obj, "model_fields")
                and obj.__module__ == module.__name__
            ):
                _patch_model(obj)
