"""Run the Snowflake setup scripts (sql/01..07) in order.

Optional convenience: the same files can be pasted into a Snowsight worksheet.

    # inside the Airflow container (no local Python needed)
    docker compose run --rm airflow-cli python /opt/airflow/project/scripts/setup_snowflake.py

    # or locally, if snowflake-connector-python is installed
    python scripts/setup_snowflake.py --dry-run

Credentials come from the SNOWFLAKE_* environment variables.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.config import sql_dir  # noqa: E402
from src.warehouse.connection import snowflake_connection  # noqa: E402
from src.warehouse.loader import run_script  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("marketflow.setup")


def setup_scripts() -> list[Path]:
    """The numbered DDL scripts, in execution order."""
    return sorted(path for path in sql_dir().glob("0*.sql") if path.is_file())


def main() -> int:
    parser = argparse.ArgumentParser(description="Create the MarketFlow Snowflake objects.")
    parser.add_argument("--dry-run", action="store_true", help="list the scripts without running them")
    args = parser.parse_args()

    scripts = setup_scripts()
    if not scripts:
        logger.error("no setup scripts found in %s", sql_dir())
        return 1

    if args.dry_run:
        for script in scripts:
            logger.info("would run %s", script.name)
        return 0

    with snowflake_connection() as connection:
        for script in scripts:
            logger.info("running %s", script.name)
            run_script(connection, script.read_text(encoding="utf-8"))

    logger.info("Snowflake objects are ready (%d scripts executed)", len(scripts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
