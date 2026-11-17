-- Clear data quality results written by an earlier attempt of the same run.
DELETE FROM AUDIT.DATA_QUALITY_RESULTS
WHERE RUN_ID = %(run_id)s
  AND TABLE_NAME = %(table_name)s;
