# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0
"""List DAGs command"""

from airflow_api_client.filters import filter_obj
from airflow_api_client.io import output
from airflow_api_client.models import DAGBag
from airflow_api_client.typedefs import App


class ListDagsCommand:
    """Helper class for listing DAGs"""

    @classmethod
    def add_subparser(cls, subparsers):
        """Add subparser for dags"""
        _ = subparsers.add_parser("dags", help="List all DAGs")

    @staticmethod
    def list_dags(app: App, add_extra_attrs_fn) -> None:
        """List all DAGs in Airflow"""

        # Use the lighter API call to get the list of DAGs unless extra details are needed
        details = any(
            [app.args.extra_attrs, app.args.filter, app.args.output_format in ["yaml", "json"]]
        )

        all_dags = [
            dag
            for dag in DAGBag(app).get_all_dags(details=details)
            if filter_obj(dag, app.args.filter)
        ]

        if app.args.output_format in ("json", "yaml"):
            output({"dags": [d.to_dict() for d in all_dags]}, app.args)
            return

        headers = ["DAG ID", "Display Name", "Is Paused", "Owners", "Tags", "Next Dag Run"]
        data = []
        for dag in all_dags:
            attrs = [
                dag.dag_id,
                dag.dag_display_name,
                "Yes" if dag.is_paused else "No",
                ", ".join(dag.owners),
                ", ".join([t.name for t in dag.tags]),
                dag.next_dagrun_logical_date or "N/A",
            ]

            add_extra_attrs_fn(app, dag, headers, attrs)

            data.append(attrs)

        output(data, app.args, headers=headers)
