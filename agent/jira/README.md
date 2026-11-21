# agent/jira — per-ticket context and audit trail

Every repository change belongs to a Jira ticket. This directory keeps, per
ticket, everything another developer (or agent session) needs to continue the
work later.

## The gate

Before any change to a tracked project file the agent must have a Jira ID.
Without one it replies:

> Please provide the Jira ID before development can begin.

and does not modify the repository. Details: `agent/rules/development.md` §1–2.

## Folder layout

```
agent/jira/<JIRA-ID>/
├── context.md        # living summary: title, status, requirements digest, dependencies,
│                     # files / Snowflake objects / DAGs touched, outstanding items, notes
├── requirements.md   # business + technical requirements and acceptance criteria, as given
├── plan.md           # development plan(s), one dated section per iteration
├── decisions.md      # decisions, assumptions, approvals (append-only)
├── changes.md        # audit log of what was changed, one dated entry per change set (append-only)
└── validation.md     # validation results per change set (append-only)
```

| File | Template |
| --- | --- |
| `context.md` | `agent/templates/jira_context_template.md` |
| `requirements.md` | `agent/templates/requirements_template.md` |
| `plan.md` | `agent/templates/development_plan_template.md` |
| `decisions.md` | `agent/templates/decision_template.md` |
| `changes.md` | `agent/templates/changes_template.md` |
| `validation.md` | `agent/templates/validation_template.md` |

Create files as they become relevant: `context.md` and `requirements.md` at
the start; `plan.md` before implementation; `decisions.md` at the first
decision; `changes.md` and `validation.md` after the first change.

## Rules

1. **Existing folder → read everything first** and continue from it. Never
   recreate or overwrite files from templates.
2. **Append, don't rewrite.** `decisions.md`, `changes.md` and `validation.md`
   are append-only logs with dated entries. `context.md` and `plan.md` may be
   updated, but previous content that is no longer valid is marked
   *superseded (date)* rather than deleted.
3. **No secrets** in any Jira file.
4. **Facts only.** Requirements come from the developer or the ticket; if
   something is missing, record it as an open question, not an assumption
   presented as fact. Assumptions go in `decisions.md`, labelled as assumptions.
5. **Validation status words** are PASSED / FAILED / NOT RUN / NOT AVAILABLE.
6. Historical information is deleted only on explicit developer instruction.

## Future integration

Today the developer types the Jira ID and requirements. A future Jira
integration (see `docs/architecture/future_integrations.md`) would fill
`requirements.md` from the ticket and could post the `changes.md` summary back
as a comment; the folder structure stays the same.
