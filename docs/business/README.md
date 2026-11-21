# Business knowledge

Business documentation is **authoritative project context** unless a newer
Jira requirement explicitly supersedes it. The agent must not invent business
rules: if required business information is not here, in `README.md` or in
`docs/DATA_FLOW.md`, it says *"not found in the repository"* and asks.

## Current contents

| File | Contents | Basis |
| --- | --- | --- |
| [`data_definitions.md`](data_definitions.md) | Source system, entities, field meanings, calculation definitions, validation rules, stated assumptions | Extracted from the existing SQL, Python and README (each item cites its source) |

## Where new business knowledge goes

Add files here as knowledge is confirmed. Suggested names:

| Topic | File |
| --- | --- |
| Business terminology / glossary | `glossary.md` |
| Source-system descriptions (per provider) | `source_systems.md` (FMP currently lives in `data_definitions.md`) |
| Business rules and exceptions | `business_rules.md` |
| Financial concepts | `financial_concepts.md` |
| Source-to-target mappings | `mappings/<entity>.md` |
| Data lineage | `lineage.md` (current lineage: README *Auditability and lineage*) |
| Calculation definitions | `data_definitions.md` |

Each entry should state: the rule/definition, its **source** (Jira ID,
stakeholder, document, or code path), the date, and whether it supersedes an
earlier entry. Adding or changing business documentation is development work
and needs a Jira ID.

## Not found in the repository (ask before assuming)

- Business owners / stakeholders and SLAs for data freshness
- Trading calendar / exchange holiday handling beyond "rows the API returns"
- Corporate actions (splits, dividends) adjustment policy
- Currency conversion rules (prices are stored as returned by the provider)
- Data retention requirements
