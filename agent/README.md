# agent/ — repository-native development agent framework

This directory holds everything the development agent needs beyond the code
itself. It is plain Markdown, versioned with the project, so any developer (or
any agent session) can pick up where the last one stopped.

| Directory | Purpose | Changes how often |
| --- | --- | --- |
| [`rules/`](rules/) | Permanent development rules and guardrails, derived from the existing code | Rarely; only with a Jira ID and approval |
| [`memory/`](memory/) | Confirmed project-wide decisions and per-developer preferences | When a developer confirms something should be remembered |
| [`jira/`](jira/) | One folder per Jira ticket: context, requirements, plan, decisions, changes, validation | During every ticket |
| [`templates/`](templates/) | Skeletons that keep Jira folders and memory entries consistent | Rarely |

Entry point: the root [`CLAUDE.md`](../CLAUDE.md).

How the pieces combine during development:

```
CLAUDE.md (operating contract)
   │
   ├── agent/rules/*            what is always true
   ├── docs/business/*          what the business means
   ├── agent/memory/global.md   what the team decided
   ├── agent/memory/users/*     how this developer likes to work
   ├── agent/jira/<ID>/*        what this ticket needs and what was done
   └── the existing code        how it is actually done today
```

Precedence when these disagree is defined in `CLAUDE.md` §7.
