-- Record a task failure in the audit log, creating the row if the task failed
-- before it could open one. START_TIME is preserved when it already exists.
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
    t.STATUS           = 'FAILED',
    t.END_TIME         = CURRENT_TIMESTAMP(),
    t.DURATION_SECONDS = DATEDIFF('millisecond', t.START_TIME, CURRENT_TIMESTAMP()) / 1000.0,
    t.ERROR_MESSAGE    = %(error_message)s,
    t.UPDATED_AT       = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    RUN_ID, DAG_ID, TASK_ID, BATCH_ID, LAYER, START_TIME, END_TIME, STATUS, ERROR_MESSAGE, UPDATED_AT
) VALUES (
    s.RUN_ID, s.DAG_ID, s.TASK_ID, %(batch_id)s, %(layer)s,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'FAILED', %(error_message)s, CURRENT_TIMESTAMP()
);
