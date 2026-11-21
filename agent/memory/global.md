# Global project memory

Project-wide decisions and preferences confirmed by the team. Format and
saving rules: [`README.md`](README.md). Newest entries at the bottom; never
delete — mark superseded.

---

### Repository-native agent framework adopted
- Date: 2026-11-21
- Scope: global
- Area: workflow
- Preference / decision: Development assistance is governed by the root
  `CLAUDE.md` and the Markdown files under `agent/` and `docs/`. No custom LLM
  API, MCP server, vector store, web UI or external database is used for agent
  memory at this stage.
- Reason: keep the first version simple and versioned with the code.
- Source: explicit request (framework bootstrap; created before any Jira ID was
  issued for it)
- Status: active

---

## Open items (not yet decided — ask, do not assume)

- **Jira project key** — not recorded anywhere in the repository. Examples use
  `MF-<n>` for illustration only. Save the real key here once a developer confirms it.
- **Least-privilege Snowflake role** — objects are currently created by an
  administrative role; a dedicated pipeline role and grants script has been
  discussed by the project owner but not built.
- **Failure notifications** — no channel (email/Slack/other) is defined.
