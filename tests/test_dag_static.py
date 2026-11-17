"""Static checks on the DAG files that do not need Airflow installed."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_FILES = sorted(DAGS_DIR.glob("*.py"))

EXPECTED_DAG_IDS = {
    "market_api_to_bronze.py": "market_api_to_bronze",
    "bronze_to_silver.py": "bronze_to_silver",
    "silver_to_gold.py": "silver_to_gold",
    "company_profile_ingestion.py": "company_profile_ingestion",
}


def test_the_expected_dag_files_exist() -> None:
    assert {path.name for path in DAG_FILES} == set(EXPECTED_DAG_IDS)


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda path: path.name)
def test_dag_files_parse(path: Path) -> None:
    ast.parse(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda path: path.name)
def test_dag_files_declare_their_dag_id(path: Path) -> None:
    source = path.read_text(encoding="utf-8")

    assert f'DAG_ID = "{EXPECTED_DAG_IDS[path.name]}"' in source
    assert "doc_md=__doc__" in source


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda path: path.name)
def test_dag_files_contain_no_credentials(path: Path) -> None:
    source = path.read_text(encoding="utf-8").lower()

    for forbidden in ("password=", "apikey=", "secret=", "snowflakecomputing.com"):
        assert forbidden not in source, f"{forbidden} found in {path.name}"


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda path: path.name)
def test_dag_files_contain_no_sql(path: Path) -> None:
    """SQL belongs in sql/, so DAG files stay readable and SQL stays reviewable."""
    source = path.read_text(encoding="utf-8").upper()

    for statement in ("SELECT ", "INSERT INTO", "MERGE INTO", "UPDATE "):
        assert statement not in source, f"{statement.strip()} found in {path.name}"
