"""Code related to the xcom sub-command"""

from typing import Any, Dict, List, Union

import airflow_client.client

from airflow_api_client.log import logger
from airflow_api_client.models import DAGRun, TaskInstance, XComEntry
from airflow_api_client.output import output
from airflow_api_client.typedefs import App


def _get_xcom(
    app: App,
    dag_id: str,
    dag_run_id: str,
    task_id: Union[str, None] = None,
    key: Union[str, None] = None,
) -> List[airflow_client.client.models.response_get_xcom_entry.ResponseGetXcomEntry]:
    # Retrieves the value of an XCom entry for a given DAG run and task. The xcom_spec should be
    logger.debug(
        "Retrieving XCom entry", dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id, key=key
    )

    if task_id:  # Covers dag_id:dag_run_id:task_id and dag_id:dag_run_id:task_id:key
        ti = TaskInstance(app, dag_id, dag_run_id, task_id)
        return ti.get_xcom_entries(key=key)

    # dag_id:dag_run_id
    entries = []
    dag_run = DAGRun(app, dag_id, dag_run_id)
    for ti in dag_run.task_instances:
        entries.extend(ti.get_xcom_entries(key=key))
    return entries


def get_xcom(app: App, xcom_spec: str) -> Union[None, Dict[str, Any]]:
    """Retrieves XCom entries based on the provided specification. The xcom_spec should be in
    the format DAG_ID:DAG_RUN_ID[:TASK_ID[:KEY]]. Depending on the level of specificity in the
    xcom_spec, it retrieves the corresponding XCom entries and formats the output
    """
    parts = xcom_spec.split(":")
    if len(parts) < 2:
        logger.error(
            "Invalid xcom specification format",
            expected_format="DAG_ID:DAG_RUN_ID[:TASK_ID[:KEY]]",
        )
        raise ValueError(
            "Invalid xcom specification format. Expected DAG_ID:DAG_RUN_ID[:TASK_ID[:KEY]]"
        )
    dag_id, dag_run_id = parts[0], parts[1]
    task_id = parts[2] if len(parts) > 2 else None
    key = parts[3] if len(parts) > 3 else None

    logger.debug(
        f"Getting XCom entry with specification {xcom_spec}",
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        key=key,
    )

    entries: List[XComEntry] = []

    if task_id:  # Covers dag_id:dag_run_id:task_id and dag_id:dag_run_id:task_id:key
        ti = TaskInstance(app, dag_id, dag_run_id, task_id)
        entries.extend(ti.get_xcom_entries(key=key))
    else:  # dag_id:dag_run_id
        entries = []
        dag_run = DAGRun(app, dag_id, dag_run_id)
        for ti in dag_run.task_instances:
            entries.extend(ti.get_xcom_entries(key=key))

    if not entries:
        logger.warning("No XCom entries found for the specified specification", xcom_spec=xcom_spec)
        return None

    entries.sort(key=lambda e: e.timestamp)
    if app.args.output_format in ("json", "yaml"):
        dagrun_output = {"dag_id": dag_id, "dag_run_id": dag_run_id}
        for entry in entries:
            dagrun_output.setdefault("tasks", {}).setdefault(entry.task_id, {})[entry.key] = {
                "value": entry.value,
                "timestamp": entry.timestamp,
            }
        output(dagrun_output, app.args)
        return dagrun_output

    # Standard human-readable output, sorted by timestamp
    for entry in entries:
        headers = ["Task ID", "Key", "Timestamp", "Value"]
        data = [
            [
                entry.task_id,
                entry.key,
                entry.timestamp,
                entry.value,
            ]
        ]
        output(data, app.args, headers=headers)
    return None
