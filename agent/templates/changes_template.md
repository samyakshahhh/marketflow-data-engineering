# <JIRA-ID> — Change audit

> Append-only. One entry per change set. Never delete historical entries
> unless explicitly instructed.

---

## Change <n> — YYYY-MM-DD

| Field | Value |
| --- | --- |
| Jira ID | <JIRA-ID> |
| Jira title | <title> |
| Development date | YYYY-MM-DD |
| Developer | <git user name> |
| Status after this change | IN PROGRESS / READY FOR REVIEW / DONE / BLOCKED |

### Summary

<what changed and why, 2–5 sentences>

### Files

| Action | Path |
| --- | --- |
| created | |
| modified | |
| deleted | (none) |

### Airflow DAGs affected

- <dag_id> — <change> (or "none")

### Snowflake objects affected

- <SCHEMA.OBJECT> — <created / altered / data changed> (or "none")

### Important implementation decisions

- See decisions.md D<n> — <one-line summary>

### Validation performed

- See validation.md, change <n>: <one-line summary with status words>

### Known limitations

- <limitation>

### Git

- Branch: <branch>; committed: yes (<sha>) / no
