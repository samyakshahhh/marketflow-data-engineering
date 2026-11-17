"""### company_profile_ingestion (secondary pipeline)

Loads company metadata (name, sector, industry, exchange, market cap) into
`BRONZE.COMPANY_PROFILE_RAW` and merges it into `SILVER.COMPANY_PROFILE`, where
`GOLD.VW_STOCK_PERFORMANCE` joins it for BI consumption.

This is reference data that changes rarely, so it runs weekly and is kept
completely separate from the price pipeline: if the profile endpoint is not
available on the account's FMP plan, this DAG fails on its own and Bronze ->
Silver -> Gold for prices keeps working.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, get_current_context, task

from src.api.fmp_client import FMPClient
from src.orchestration.common import (
    TAGS,
    default_args,
    new_batch_id,
    run_id_from_context,
    task_id_from_context,
)
from src.transformations import api_to_bronze
from src.transformations import bronze_to_silver as transform
from src.utils.config import get_config
from src.utils.logging_utils import get_logger
from src.warehouse import audit
from src.warehouse.connection import snowflake_connection

logger = get_logger(__name__)

DAG_ID = "company_profile_ingestion"
LAYER = audit.LAYER_BRONZE


@dag(
    dag_id=DAG_ID,
    description="Secondary pipeline: company metadata from FMP into Bronze and Silver.",
    schedule="0 7 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=[*TAGS, "reference-data"],
    doc_md=__doc__,
)
def company_profile_ingestion() -> None:
    @task
    def prepare_run() -> dict[str, str]:
        context = get_current_context()
        identity = {"run_id": run_id_from_context(context), "batch_id": new_batch_id()}
        logger.info("starting company profile run_id=%s", identity["run_id"])
        return identity

    @task
    def ingest_profiles(identity: dict[str, str]) -> dict[str, int]:
        """Fetch one profile per configured symbol and append them to Bronze."""
        context = get_current_context()
        run_id, batch_id = identity["run_id"], identity["batch_id"]
        task_id = task_id_from_context(context)
        symbols = get_config().symbols
        loaded = 0

        with snowflake_connection() as connection:
            audit.start_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                batch_id=batch_id,
                layer=LAYER,
            )
            with FMPClient() as client:
                for symbol in symbols:
                    result = api_to_bronze.ingest_company_profile(
                        connection,
                        client,
                        symbol,
                        run_id=run_id,
                        batch_id=batch_id,
                    )
                    loaded += result.loaded_records
            audit.finish_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                status=audit.STATUS_SUCCESS,
                source_record_count=len(symbols),
                target_record_count=loaded,
            )
        return {"symbols": len(symbols), "loaded": loaded}

    @task
    def merge_profiles_to_silver(identity: dict[str, str]) -> dict[str, int]:
        """Merge the latest profile per symbol into Silver."""
        context = get_current_context()
        run_id, batch_id = identity["run_id"], identity["batch_id"]
        task_id = task_id_from_context(context)

        with snowflake_connection() as connection:
            window = transform.plan_window(
                connection,
                table_name=transform.SILVER_COMPANY_PROFILE,
                pending_sql="16_pending_bronze_company_profile.sql",
            )
            audit.start_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                batch_id=batch_id,
                layer=audit.LAYER_SILVER,
            )
            result = transform.transform_company_profile(
                connection,
                window=window,
                run_id=run_id,
                batch_id=batch_id,
            )
            transform.advance_watermark(
                connection,
                table_name=transform.SILVER_COMPANY_PROFILE,
                window=window,
                run_id=run_id,
                batch_id=batch_id,
            )
            audit.finish_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                status=audit.STATUS_SUCCESS,
                source_record_count=result.source_records,
                target_record_count=result.merged_records,
            )
        return {"merged": result.merged_records}

    identity = prepare_run()
    ingest_profiles(identity) >> merge_profiles_to_silver(identity)


company_profile_ingestion()
