import json
import sys
from typing import Optional, Union

from airflow_api_client.log import logger
from airflow_api_client.models import DAG


def get_payload_for_dag(app, dag_id, dag: Optional[Union[DAG, None]] = None):
    """Returns the payload expected for a given DAG. Prompts for values for each parameter
    defined in the DAG's parameters and constructs a payload dictionary.
    """
    dag = dag or DAG(app, dag_id)
    parameters = dag.params or {}
    payload = {"conf": {}}
    logger.debug("Constructing payload for DAG run", dag_id=dag_id, parameters=parameters)

    for key, value in parameters.items():
        value_types = value["schema"]["type"]
        prompt = f"Enter value for parameter: '{key}'\n"
        prompt += f'Description: {value["description"]}'
        prompt += f'\nType(s): {", ".join(value_types)}'
        prompt += "\n> "
        sys.stdout.write(prompt)
        value = input()
        for value_type in value_types:
            if value_type == "null" and value == "":
                value = None
                break
            if value_type == "integer":
                value = int(value)
            elif value_type == "number":
                value = float(value)
            elif value_type == "boolean":
                value = value.lower() in ("true", "1", "yes")
        try:
            payload["conf"][key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            payload["conf"][key] = value
    return payload
