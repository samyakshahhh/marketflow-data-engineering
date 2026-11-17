-- Close the audit row for one pipeline step with its outcome and counts.
UPDATE AUDIT.ETL_RUN_LOG
SET END_TIME              = CURRENT_TIMESTAMP(),
    DURATION_SECONDS      = DATEDIFF('millisecond', START_TIME, CURRENT_TIMESTAMP()) / 1000.0,
    STATUS                = %(status)s,
    SOURCE_RECORD_COUNT   = %(source_record_count)s,
    TARGET_RECORD_COUNT   = %(target_record_count)s,
    REJECTED_RECORD_COUNT = %(rejected_record_count)s,
    ERROR_MESSAGE         = %(error_message)s,
    UPDATED_AT            = CURRENT_TIMESTAMP()
WHERE RUN_ID = %(run_id)s
  AND DAG_ID = %(dag_id)s
  AND TASK_ID = %(task_id)s;
