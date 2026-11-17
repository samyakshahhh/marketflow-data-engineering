-- Clear quarantine rows written by an earlier attempt of the same run, so a
-- task retry does not duplicate them.
DELETE FROM AUDIT.DATA_QUALITY_ERRORS
WHERE RUN_ID = %(run_id)s
  AND TABLE_NAME = %(table_name)s;
