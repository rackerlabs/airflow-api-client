"""Code related to the `run` sub-command."""

import json
import time
from pathlib import Path
from typing import Dict, Optional, Union

from airflow_api_client.commands import create_payload, xcom
from airflow_api_client.log import logger
from airflow_api_client.models import DAG, DAGRun


def run_dag(app, dag_id, conf: Optional[Union[None, str, Dict]] = None) -> DAGRun:
    """Triggers a DAG with an optional payload. The response is printed to the console in the
    specified output format (JSON, YAML, or human-readable). If a payload is provided, it is used as
    the `conf` for the DAG run. If no payload is provided, the user is prompted to enter values for
    each parameter defined in the DAG's parameters. After triggering the DAG, the function polls for
    completion and retrieves any XCom entries associated with the DAG run.
    """

    conf = app.args.conf

    dag = DAG(app, dag_id)

    if conf is None:
        conf = create_payload.get_payload_for_dag(app, dag_id, dag=dag)
    else:
        if conf.startswith("@"):
            conf_path = Path(conf[1:]).absolute()
            if not conf_path.is_file():
                logger.error("Conf file not found", path=str(conf_path))
                raise FileNotFoundError(f"Conf file not found: {conf_path}")
            with open(conf_path, "r", encoding="utf-8") as f:
                conf = f.read()
        conf = {"conf": json.loads(conf)}

    try:
        conf = json.loads(conf) if isinstance(conf, str) else conf
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON provided for --conf", error=str(e))
        raise

    dag_run: DAGRun = dag.run(conf=conf)

    # Poll for completion. Prefer this over calling airflow_client's wait_dag_run_until_finished
    # until that's no longer marked as experimental.
    while (current_state := dag_run.state) not in ("success", "failed"):
        logger.debug("Polling for DAG run completion", dag_id=dag_id, state=current_state)
        time.sleep(2)

    logger.info("DAG run completed", dag_id=dag_id, final_state=dag_run.state)

    xcom.get_xcom(app, f"{dag_id}:{dag_run.run_id}")

    return dag_run

    # self.get_xcom(f"{dag_id}:{dag_run_id}")
