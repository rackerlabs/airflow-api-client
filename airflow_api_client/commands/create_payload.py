"""Command to construct a payload for a DAG run by prompting for values for each parameter defined
in the DAG's parameters.
"""

import json

from airflow_api_client.commands import Command
from airflow_api_client.io import output
from airflow_api_client.log import logger
from airflow_api_client.models import DAG


class CreatePayload(Command):
    """Command to construct a payload for a DAG run by prompting for values for each parameter
    defined in the DAG's parameters."""

    name = "create-payload"
    help = (
        "Construct a payload for a DAG run by prompting for values for each parameter defined"
        " in the DAG's parameters."
    )

    @classmethod
    def add_arguments(cls, parser) -> None:
        """Adds arguments for the command."""
        parser.add_argument(
            "dag_id",
            metavar="DAG_ID",
            help=(
                "The ID of the DAG for which to create a payload. The DAG's parameters will be used"
                " to prompt for values and construct the payload based on the DAGs `params`."
            ),
        )
        parser.add_argument(
            "--output-file",
            "-o",
            metavar="OUTPUT_FILE",
            help=(
                "Optional file path to save the constructed payload. If not provided, the payload"
                " will be printed to the console."
            ),
        )

    def execute(self, **kwargs) -> None:
        """Returns the payload expected for a given DAG. Prompts for values for each parameter
        defined in the DAG's parameters and constructs a payload dictionary.
        """
        logger.debug("Executing CreatePayload with arguments", dag_id=self.app.args.dag_id)
        dag = DAG(self.app, self.app.args.dag_id)
        parameters = dag.params or {}
        payload = {}
        logger.debug(
            "Constructing payload for DAG run", dag_id=self.app.args.dag_id, parameters=parameters
        )
        payload = dag.create_payload()

        if self.app.args.output_file:
            with open(self.app.args.output_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=4, sort_keys=True, default=str)
            logger.info(f"Payload saved to {self.app.args.output_file}")
        else:
            output(json.dumps(payload, indent=4, sort_keys=True, default=str), self.app.args)
        return payload
