# agent/memory — persistent decisions and preferences

Two levels:

| File | Holds | Example |
| --- | --- | --- |
| [`global.md`](global.md) | Project-wide decisions and preferences that apply to every developer | "The Jira project key is MF." |
| `users/<developer>.md` | One developer's personal preferences | "Prefers one test file per new module." |

## What gets saved

Only information that is:

1. **explicitly** identified by a developer as something to remember
   ("remember that…", "from now on…", "always…"), or
2. **clearly confirmed** by the developer after the agent asks:
   > Should I save this as a persistent development preference?

Do **not** save:

- ordinary conversational statements or one-off instructions for the current task
- anything already recorded in code, rules, docs or git history
- ticket-specific information — that belongs in `agent/jira/<JIRA-ID>/`
- secrets, credentials, account identifiers, hostnames with credentials,
  email addresses or other personal contact data

## Developer identity

The user file name is the developer's `git config user.name`, lower-cased,
spaces replaced by `-` (e.g. `jane-doe.md`). If git has no user name, ask the
developer what name to use. Create the file from
`agent/templates/memory_template.md` the first time a preference is confirmed —
not before.

## Entry format

Each entry (see `agent/templates/memory_template.md`):

```markdown
### <short title>
- Date: YYYY-MM-DD
- Scope: global | user
- Area: airflow | snowflake | python | sql-style | testing | docs | workflow | other
- Preference / decision: <one or two sentences>
- Reason: <why, as stated by the developer>
- Source: <"explicit request" or "confirmed when asked"> (Jira ID if any)
- Status: active | superseded by <title> on YYYY-MM-DD
```

Never delete entries; mark them `superseded` instead.

## How memory is used

During development the agent combines: global rules + business context + user
preferences + Jira context + existing code. Memory is the **lowest-priority**
project input (`CLAUDE.md` §7):

- Existing code and the explicit current requirement beat old preferences.
- Current Jira requirements beat outdated memory.
- Security rules cannot be overridden by any preference.

When a memory entry conflicts with the code or the current request, follow the
higher-priority source and tell the developer the memory entry may be outdated.
