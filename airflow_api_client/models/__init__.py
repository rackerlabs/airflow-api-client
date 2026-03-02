"""A module for interacting with Airflow DAGs, DAG runs, task instances, XCom and any other
Airflow entities via the Airflow API.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from airflow_client.client.models import (
    DAGDetailsResponse,
    DagRunState,
    ResponseGetXcomEntry,
    TriggerDAGRunPostBody,
)

from airflow_api_client.log import logger

if TYPE_CHECKING:
    from airflow_api_client import App

TERMINAL_DAGRUN_STATES = {DagRunState.SUCCESS, DagRunState.FAILED}

# pylint: disable=protected-access

# pylint: disable=too-many-arguments,too-many-positional-arguments


class XComEntry:
    """A class representing an XCom entry in Airflow, providing lazy loading of the XCom data and
    delegation of attribute access to the underlying XCom data object.
    """

    def __init__(
        self, app: "App", dag_id: str, task_id: str, run_id: str, key: str, xcom_data=None
    ) -> None:
        self.app = app
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.key = key
        self._xcom_data = xcom_data

    def __repr__(self):
        """Return a string representation of the XComEntry instance, including the key, DAG ID, task
        ID, and run ID.
        """
        return (
            f"xcom_entry(key='{self.key}', dag_id='{self.dag_id}',"
            f" task_id='{self.task_id}', run_id='{self.run_id}')"
        )

    @property
    def xcom_data(self) -> ResponseGetXcomEntry:
        """Lazily load the XCom data from the Airflow API if it hasn't been loaded yet, and return
        the underlying XCom data object.
        """
        if self._xcom_data is None:
            self._xcom_data = self.app.client._xCom_api.get_xcom_entry(
                dag_id=self.dag_id, task_id=self.task_id, dag_run_id=self.run_id, xcom_key=self.key
            ).actual_instance
        return self._xcom_data

    def __getattr__(self, item) -> Any:
        """Delegate attribute access to the underlying xcom data object"""
        return getattr(self.xcom_data, item)

    @classmethod
    def from_key(cls, app: "App", dag_id: str, task_id: str, run_id: str, key: str) -> "XComEntry":
        """Fetch a single XCom entry for the given key from the Airflow API."""
        logger.debug(
            f"Fetching XCom entry for task '{task_id}' in DAG run '{run_id}' with key '{key}'"
        )
        xcom_entry_data = app.client._xCom_api.get_xcom_entry(
            dag_id=dag_id, task_id=task_id, dag_run_id=run_id, xcom_key=key
        ).actual_instance
        return cls(
            app=app,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            key=key,
            xcom_data=xcom_entry_data,
        )


# pylint: enable=too-many-arguments,too-many-positional-arguments


class TaskInstance:
    """A class representing a task instance in Airflow, providing methods to retrieve XCom entries
    for the task instance and to represent the task instance as a string.
    """

    def __init__(self, app: "App", dag_id: str, run_id: str, task_id: str) -> None:
        self.app = app
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id

    def get_xcom_entries(self, key: Optional[Union[None, str]] = None) -> list[XComEntry]:
        """Fetch XCom entries for this task instance from the Airflow API. If a key is provided,
        fetch only the XCom entry for that key; otherwise, fetch all XCom entries
        for this task instance.
        """
        if key is not None:
            logger.debug(
                f"Fetching XCom entries for task '{self.task_id}' in DAG run"
                f" '{self.run_id}' with key '{key}'"
            )
            return [
                XComEntry.from_key(
                    app=self.app,
                    dag_id=self.dag_id,
                    task_id=self.task_id,
                    run_id=self.run_id,
                    key=key,
                )
            ]

        xcom_entries_data = self.app.client._xCom_api.get_xcom_entries(
            dag_id=self.dag_id, task_id=self.task_id, dag_run_id=self.run_id
        ).xcom_entries

        return [
            XComEntry.from_key(
                app=self.app,
                dag_id=self.dag_id,
                task_id=self.task_id,
                run_id=self.run_id,
                key=entry.key,
            )
            for entry in xcom_entries_data
        ]

    def __repr__(self):
        return (
            f"TaskInstance(dag_id='{self.dag_id}', task_id='{self.task_id}',"
            f" run_id='{self.run_id}')"
        )


class DAGRun:
    """A class representing a DAG run in Airflow, providing methods to retrieve the state of the DAG
    run, fetch task instances for the DAG run, and represent the DAG run as
    a string.
    """

    def __init__(self, app: "App", dag_id, run_id) -> None:
        self.app = app
        self.dag_id = dag_id
        self.run_id = run_id
        self._has_finished = False  # To keep track of whether we need to re-fetch the attributes
        self._attributes = None

    def __repr__(self):
        """Return a string representation of the DAGRun instance, including the DAG ID and run
        ID."""
        return f"DAGRun(dag_id='{self.dag_id}', run_id='{self.run_id}'"

    @property
    def attributes(self):
        """Lazily load the DAG run attributes from the Airflow API if they haven't been loaded yet,
        and return the attributes. If the DAG run has finished, we can return the cached attributes
        without re-fetching from the API, since the state won't change anymore.
        """
        if self._attributes is not None and self._has_finished:
            return self._attributes
        logger.debug(f"Fetching attributes for DAG run '{self.run_id}' from Airflow API")
        self._attributes = self.app.client._DAG_run_api.get_dag_run(
            dag_id=self.dag_id, dag_run_id=self.run_id
        )

        return self._attributes

    @property
    def state(self) -> str:
        """Return the state of the DAG run. If the DAG run has finished, we can return the cached
        state without re-fetching from the API, since the state won't change anymore. If the DAG run
        is still running, we need to fetch the latest state from the API to ensure we have the most
        up-to-date information.
        """
        if self._has_finished:
            return self.attributes.state
        if (state := self.attributes.state) in TERMINAL_DAGRUN_STATES:
            self._has_finished = True
        return state

    @property
    def task_instances(self) -> list[TaskInstance]:
        """Retrieve task instances for this DAG run from the Airflow API."""
        logger.debug(f"Fetching task instances for DAG run '{self.run_id}' from Airflow API")
        task_instances_data = self.app.client._task_instance_api.get_task_instances(
            dag_id=self.dag_id, dag_run_id=self.run_id
        ).task_instances
        return [
            TaskInstance(
                app=self.app, dag_id=self.dag_id, run_id=self.run_id, task_id=instance.task_id
            )
            for instance in task_instances_data
        ]


class DAG:
    """A class representing a DAG in Airflow, providing methods to trigger a DAG run, retrieve DAG
    details, and delegate attribute access to the underlying DAG details object.
    """

    def __init__(
        self, app: "App", dag_id: str, dag: Optional[Union[None, DAGDetailsResponse]] = None
    ) -> None:
        self.app = app
        self.id = dag_id
        self._dag = dag

    def __repr__(self):
        """Return a string representation of the DAG instance, including the DAG ID."""
        return f"DAG(dag_id='{self.dag_id}')"

    def run(
        self, run_id: Optional[Union[str, None]] = None, conf: Optional[Union[Dict, None]] = None
    ) -> DAGRun:
        """Trigger a DAG run for this DAG. If a run ID is provided, use that as the run ID for the
        DAG run; otherwise, generate a unique run ID based on the current timestamp. If a
        configuration dictionary is provided, include that in the payload for triggering
        the DAG run.
        """
        now_utc = datetime.now(timezone.utc)
        run_id = f"manual__{int(now_utc.timestamp())}"
        payload = {
            "conf": conf["conf"],
            "dag_run_id": run_id,
            "logical_date": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        logger.debug(f"Triggering DAG '{self.id}' with payload", payload=payload)
        run_payload = TriggerDAGRunPostBody(**payload)
        result = self.app.client._DAG_run_api.trigger_dag_run(
            dag_id=self.id, trigger_dag_run_post_body=run_payload
        )
        logger.info("DAG run triggered successfully", dag_id=self.id, response=result)
        return DAGRun(self.app, dag_id=self.dag_id, run_id=run_id)

    @property
    def dag(self) -> DAGDetailsResponse:
        """Lazily load the DAG details from the Airflow API if they haven't been loaded yet, and
        return the underlying DAG details object. If the DAG details have already been loaded,
        return the cached DAG details without re-fetching from the API, since the DAG details won't
        change after the DAG has been created.
        """
        if self._dag is not None:
            return self._dag
        self._dag = self.app.client._DAG_api.get_dag_details(dag_id=self.id)
        return self._dag

    def __getattr__(self, item):
        """Delegate attribute access to the underlying DAG details object. If the DAG details
        haven't been loaded yet, we need to fetch them from the Airflow API before trying to access
        the attribute. If the attribute still isn't found after fetching the DAG details, we raise
        an AttributeError to indicate that the attribute doesn't exist on the DAG object.
        """
        try:
            return getattr(self.dag, item)
        except AttributeError:
            # If instantiated from DAGBag, the DAG details may not be available at this point,
            # so we need to fetch them before trying to access the attribute again.
            if not isinstance(self._dag, DAGDetailsResponse):
                self._dag = self.app.client._DAG_api.get_dag_details(dag_id=self.id)
                try:
                    return getattr(self._dag, item)
                except AttributeError:
                    pass
            raise AttributeError(f"'DAG' object has no attribute '{item}'") from None


class DAGBag:  # pylint: disable=too-few-public-methods
    """A class representing a collection of DAGs in Airflow, providing methods to retrieve all DAGs
    and delegate attribute access to the underlying Airflow API client for any attributes that
    aren't explicitly defined on the DAGBag class.
    """

    def __init__(self, app: "App") -> None:
        self.app = app

    def get_all_dags(self, details: Optional[bool] = True, **kwargs) -> list[DAG]:
        """Fetch all DAGs from the Airflow API. If the `details` parameter is set to `True`, fetch
        the full details for each DAG; otherwise, fetch only the basic information for each DAG. The
        `kwargs` parameter can be used to pass additional query parameters to the API request, such
        as `limit` or `offset`.
        """
        dags = self.app.client._DAG_api.get_dags(**kwargs).dags
        if not details:
            return [DAG(app=self.app, dag_id=dag.dag_id, dag=dag) for dag in dags]
        ret = []
        for dag in dags:
            ret.append(
                DAG(
                    app=self.app,
                    dag_id=dag.dag_id,
                    dag=self.app.client._DAG_api.get_dag_details(dag_id=dag.dag_id),
                )
            )
        return ret
