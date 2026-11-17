-- Open (or reopen, on an Airflow retry) the audit row for one pipeline step.
MERGE INTO AUDIT.ETL_RUN_LOG t
USING (
    SELECT
        %(run_id)s::VARCHAR  AS RUN_ID,
        %(dag_id)s::VARCHAR  AS DAG_ID,
        %(task_id)s::VARCHAR AS TASK_ID
) s
   ON t.RUN_ID = s.RUN_ID
  AND t.DAG_ID = s.DAG_ID
  AND t.TASK_ID = s.TASK_ID
WHEN MATCHED THEN UPDATE SET
    t.BATCH_ID              = %(batch_id)s,
    t.LAYER                 = %(layer)s,
    t.START_TIME            = CURRENT_TIMESTAMP(),
    t.END_TIME              = NULL,
    t.DURATION_SECONDS      = NULL,
    t.STATUS                = 'RUNNING',
    t.SOURCE_RECORD_COUNT   = NULL,
    t.TARGET_RECORD_COUNT   = NULL,
    t.REJECTED_RECORD_COUNT = NULL,
    t.ERROR_MESSAGE         = NULL,
    t.UPDATED_AT            = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    RUN_ID, DAG_ID, TASK_ID, BATCH_ID, LAYER, START_TIME, STATUS, UPDATED_AT
) VALUES (
    s.RUN_ID, s.DAG_ID, s.TASK_ID, %(batch_id)s, %(layer)s,
    CURRENT_TIMESTAMP(), 'RUNNING', CURRENT_TIMESTAMP()
);
