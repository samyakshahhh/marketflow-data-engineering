"""DAG integrity.

These tests need Airflow, so they run inside the container:

    docker compose run --rm airflow-cli bash -c "cd /opt/airflow/project && pytest -q"

and are skipped automatically when Airflow is not installed (the normal case
for a local `pytest` run, which is deliberately Airflow free). The static
checks that do not need Airflow live in ``test_dag_static.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
EXPECTED_DAG_IDS = {
    "market_api_to_bronze",
    "bronze_to_silver",
    "silver_to_gold",
    "company_profile_ingestion",
}

pytest.importorskip("airflow", reason="Airflow is only installed in the Docker image")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS_DIR))


def test_every_dag_file_imports_without_errors(dagbag) -> None:
    assert dagbag.import_errors == {}


def test_all_expected_dags_are_present(dagbag) -> None:
    assert EXPECTED_DAG_IDS.issubset(set(dagbag.dag_ids))


def test_dags_retry_and_have_an_owner(dagbag) -> None:
    for dag_id in EXPECTED_DAG_IDS:
        dag = dagbag.get_dag(dag_id)
        assert dag is not None
        assert dag.tasks, f"{dag_id} has no tasks"
        for task in dag.tasks:
            assert task.retries >= 2, f"{dag_id}.{task.task_id} should retry"
            assert task.owner


def test_downstream_dags_are_scheduled_on_assets(dagbag) -> None:
    """Silver and Gold run when upstream data arrives, not on a fixed clock."""
    for dag_id in ("bronze_to_silver", "silver_to_gold"):
        # Airflow 3 SDK timetables carry no summary/description, so the class
        # name is the reliable signal (AssetTriggeredTimetable).
        timetable_type = type(dagbag.get_dag(dag_id).timetable).__name__
        assert "Asset" in timetable_type, f"{dag_id} is scheduled by {timetable_type}"


def test_the_ingestion_dag_runs_on_a_schedule(dagbag) -> None:
    dag = dagbag.get_dag("market_api_to_bronze")

    assert type(dag.timetable).__name__ == "CronTriggerTimetable"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
