"""Code related to the `run` sub-command."""

import json
import time
from pathlib import Path

from airflow_api_client.commands import Command
from airflow_api_client.log import logger
from airflow_api_client.models import DAG, DAGRun
from airflow_api_client.typedefs import App

DESCRIPTION = """
Command to trigger a DAG run with an optional payload. The response is printed to the console
in the specified output format (JSON, YAML, or human-readable). If a payload is provided, it is
used as the `conf` for the DAG run. If no payload is provided, the user is prompted to enter
values for each parameter defined in the DAG's parameters. After triggering the DAG, the command
polls for completion and retrieves any XCom entries associated with the DAG run.
"""


class RunDagCommand(Command):
    """Command to trigger a DAG run with an optional payload."""

    name = "run"
    help = DESCRIPTION

    @classmethod
    def add_arguments(cls, parser) -> None:
        parser.add_argument("dag_id", help="DAG ID to run")
        parser.add_argument("--conf", "-c", type=str, help="JSON conf or @file")

    def execute(self, app: App, **kwargs) -> None:
        """Triggers a DAG with an optional payload. The response is printed to the console in the
        specified output format (JSON, YAML, or human-readable). If a payload is provided, it is
        used as the `conf` for the DAG run. If no payload is provided, the user is prompted to
        enter values for each parameter defined in the DAG's parameters. After triggering the DAG,
        the function polls forcompletion and retrieves any XCom entries associated with the DAG run.
        """
        logger.debug("Executing RunDagCommand")

        conf = app.args.conf

        dag = DAG(app, app.args.dag_id)

        if conf is None:
            conf = dag.create_payload()
        else:
            if conf.startswith("@"):
                conf_path = Path(conf[1:]).absolute()
                if not conf_path.is_file():
                    logger.error("Conf file not found", path=str(conf_path))
                    raise FileNotFoundError(f"Conf file not found: {conf_path}")
                with conf_path.open("r", encoding="utf-8") as f:
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
            logger.debug("Polling for DAG run completion", dag=dag, state=current_state)
            time.sleep(2)

        logger.info("DAG run completed", dag=dag, final_state=dag_run.state)

        dag_run.xcoms.output()

        return dag_run
