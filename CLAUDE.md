# podcast-network

A graph of who appears on which clean-energy podcasts. FastAPI + PostgreSQL 18
on Render, Create React App + Tailwind frontend, Python scrapers driven by
GitHub Actions.

```
backend/    FastAPI app (main.py, ~2.6k lines) — API + admin endpoints
frontend/   CRA + Tailwind. PodcastHostNetwork.js is the graph
scraper/    scrapers, the name scanner, and .sql migrations
```

## Working agreements

**Branches.** Develop on `claude/gracious-darwin-vbul7z`, push there **and**
mirror to `main`. GitHub Actions cron only fires from the default branch, so
scheduled work that only exists on the dev branch never runs.

**Bulk data changes always get quantified and approved first.** Say how many
rows, show a sample, wait. This has repeatedly mattered: a title-drift bug
would have inserted 831 duplicate episodes, and a "stale credit" cleanup was
abandoned after four rounds of sampling kept turning up real appearances the
premise had assumed were wrong. "The current scanner wouldn't produce this
credit" is not the same claim as "this credit is wrong."

**Migrations need `SET lock_timeout = '10s'`.** An `ALTER TABLE` once queued
behind a running scan's read lock on `podcasts`; the pending exclusive lock
then blocked every reader, and clearing it needed `pg_cancel_backend` —
killing the local psql client was not enough.

**Never deploy code that reads a column before its migration has run.** A
`scan_descriptions` reference shipped ahead of its migration and 500'd the
diagnostics endpoint.

**Backend and frontend deploy independently, backend first.** Do not change an
API's response shape and its consumer in one commit — the old frontend will
briefly call the new backend. Put the reader in first, or gate it behind a
flag (see `GRAPH_PAYLOAD_NORMALISED`).

**Render sets `CI=true`, which makes CRA treat lint warnings as errors.**
Frontend builds failed silently for some time while a stale bundle kept
serving. Always verify with `CI=true npm run build`, not `npm start`.

## Data model

`podcasts`, `episodes`, `hosts`, `episode_host` (PK episode_id+host_id, with
`is_guest`, `role`, `data_source`), `host_podcast` (show-level hosts),
`host_aliases`, `not_duplicate_pairs`, `suggestions`, `rejected_names`,
`image_suggestions`, `host_roles`, `host_social_links`, `episode_tag`.

**`data_source` precedence:** `apple_verified` > `manual` > everything else
(`parsed_desc`, `parsed_title`, `approved_suggestion`, `itunes_artist`).
Reconciliation must exempt both `apple_verified` and `manual` — those are
human or authoritative decisions and automated passes must not overwrite them.

**Aliases are stored, not inferred.** A nickname goes in `host_aliases`
alongside the canonical name so future scans match either spelling. The
scanner's `get_hosts()` unions `hosts` and `host_aliases`.

**Deleting a credit does not stick on its own — suppress it.** Removing a row
from `episode_host` is undone by the next scrape, which re-reads the same
description, derives the same name and re-inserts it. This cost a full morning
of per-person curation: Bill Gates went back to 48 credits from 3, Joe Manchin
to 41 from 2, on one scheduled run. `credit_suppressions (episode_id,
host_id)` records the removal, and a `BEFORE INSERT` trigger on `episode_host`
silently skips any suppressed pair. It is enforced by the trigger, not at the
call sites, because ten places insert into that table. To re-add a credit
deliberately, delete the suppression first — the admin add-credit endpoint
already does.

**Scanner-level fixes are durable; row deletions are not.** The curation that
survived that scrape survived because it changed the scanner — a strip pattern,
`scan_descriptions` off for a show, an exclusion. Anything done purely by
DELETE came back. Prefer fixing the extraction; use suppression for the
genuinely per-episode cases.

**`not_duplicate_pairs` records deliberate non-merges.** Albert Gore III was
wrongly merged into Al Gore and had to be separated; the pair is recorded so
it is not re-proposed.

## Scanner notes (`scraper/episode_name_scanner.py`, ~1k lines)

- **Name matching must be word-boundary anchored.** A bare substring match
  credited "Dan Yates" from inside "Jordan Yates". `\b` alone is insufficient
  because of hyphenated surnames; the pattern is
  `(?<![\w-])NAME(?![\w-])`.
- **`DESC_SCAN_MAX_CHARS = 2500`**, truncated on a word boundary.
- Hosts are indexed by surname (`build_surname_index` / `candidate_hosts`) —
  this was a 311x speedup over scanning every name against every episode.
- `strip_html()` turns tags into spaces, not nothing. 2,327 episodes had raw
  HTML, and deleting tags outright glues words together.
- Show-level hosts are credited when named by first name only.
- Organisations are filtered via `_ORG_WORDS` / `looks_like_organisation()`.
  **When a company keeps producing false positives, add it to the exclusion
  list rather than inventing a general rule.**
- `rejected_names` is the persistent exclusion list, read from the DB.
- `podcasts.scan_descriptions` turns description scanning off per show.
  Politico Energy stays off: it discusses politicians constantly but almost
  never has guests, so the false positives are unusable.
- Panellists count as guests.
- **The ~40 strip patterns need auditing** — several show-specific fixes
  overreach.
- **`run()` re-inserted every match on every full-archive scan, not just new
  ones** — since it scans the whole archive by design (see above), ~99% of
  matches on a typical run were already in `episode_host`, but each still
  cost one `INSERT ... ON CONFLICT DO NOTHING` round-trip. At ~60ms/round-trip
  from a GitHub Actions runner to Render, ~24,000 redundant round-trips ate
  the scheduled `scrape.yml` job's 45-minute budget and killed the "Scan
  episodes for known names" step on 60% of runs — which meant the two steps
  after it (queueing suggestions, recording the successful-run timestamp)
  silently stopped running too. Fixed by filtering matches against
  `get_existing_credits()` (a plain `(episode_id, host_id)` set from
  `episode_host`) before inserting, then batching the genuinely-new remainder
  with `execute_values`. `suggest()` doesn't have this problem — it already
  filters against known/rejected/pending/credited before its per-row insert
  loop, so its insert volume was always small.

## Guest affiliations (`scraper/extract_affiliations.py`)

Job title + organisation per **guest appearance**, not per person — people
change jobs, and every observation is kept for auditing; a person's current
role is their most recent row. Tables in `migrate_add_host_affiliations.sql`:
`affiliation_extractions` (one row per processed `(episode, host)`, whatever
the outcome, with the exact snippet sent) and `host_affiliations` (the
facts; `data_source` `llm_extracted` or `manual`, and `manual` is never
overwritten). Both FK to `episode_host` with `ON UPDATE/DELETE CASCADE`, so
merges and credit deletions carry through without touching that code.

- **Status (Sep 24 2026):** `migrate_add_host_affiliations.sql` has run on
  production. Stage 1 done: 200 random guest appearances (Sonnet 5, $0.15),
  194 processed, 210 roles stored. Stage 2 (~15.8k remaining, ~$12) awaits
  approval. `migrate_add_host_role_pins.sql` has **not** run — the role
  endpoints read that table, so it must run before the backend deploys.
  Not in `scrape.yml` yet; needs `ANTHROPIC_API_KEY` as a GitHub secret.
- **Pilot (Sep 2026, 88 snippets from the Aug export):** Haiku 4.5 credited
  another guest's role to the named person on 1-2 snippets per run —
  "Joe Batir speaks with Jigar Shah, Director at the DOE Loan Programs
  Office" came back as Joe Batir's role on three runs of three, despite a
  prompt rule with that exact shape. Sonnet 5 (`--model claude-sonnet-5`,
  thinking disabled) made no such errors and found more possessive company
  mentions ("Tigercomm's Mike Casey"), at ~3x the cost.
- Too varied for regex, so a model reads it. Kept cheap by sending only
  ~120 chars before / ~280 after each name mention (`build_snippet`), sending
  an identical (person, snippet) once, 40 items per request, via the Batch
  API (half price). `estimate` against the Aug data export: 6,022 guest
  appearances → 5,250 unique items → ~$0.77 on Haiku, ~$2.34 on Sonnet 5
  (chars/4 approximation, Sonnet scaled by the pilot's measured 1.5x).
- **Every stored value must occur verbatim in its snippet**
  (`verified_affiliations`); anything else is dropped and counted. This is
  the guard against the model supplying a company from outside knowledge.
- Order of operations: `estimate` (read-only) → `pilot --limit 100` (API,
  CSV only, no DB writes) → review → migration → `submit` with approval per
  the quantify-first rule → `collect`. `run` = collect then submit, for cron;
  batches can take up to 24h, so each run collects the previous run's batch.
  `--max-cost` (default $2) refuses to submit above the estimate.
- `no_mention` rows (name not in title or description, e.g. Apple-only
  credits) are terminal; a later description refresh does not re-open them.
- **Hosts are out of scope** (a separate process, per Evan). Two filters: a
  person in `host_podcast` for that show is never selected (1,216 of 6,022
  guest credits in the Aug export — Joe Batir alone had 195 "guest" credits
  on his own show), and the model flags anyone the text presents as this
  podcast's host/producer (`is_podcast_host`) for hosts `host_podcast`
  doesn't know about (Energy Central's Jason Price and Matt Chester);
  those get status `host` and nothing stored.
- Snippets also take one later first-name-only mention ("Sergey is a senior
  fellow at ..."), unless that first name is attached to another surname in
  the same text.
- **This is raw data.** Different wording across appearances ("CEO" vs
  "Co-Founder and CEO", "Fervo" vs "Fervo Energy") is stored as-is; a later
  process derives each person's displayed current role. Company
  normalisation is also later: `organizations` + `organization_aliases`
  (same pattern as `hosts`/`host_aliases`), an `org_id` filled in on
  `host_affiliations`, optional `parent_org_id` for sub-units (whether
  "Microsoft" queries include "Microsoft Research" is undecided), reviewed
  through a Company Admin page like name suggestions. Not seeded from
  Colorado Current's `companies` table (too small to matter against
  thousands of extracted orgs); website domain is the join key if that link
  is ever wanted.
- Per appearance the model also records `appears_on_episode` (false = only
  talked about: politicians discussed, production credits, links, books —
  a curation signal for false guest credits, never acted on automatically;
  defaults to true when unsure) and `from_other_episode` (past-episode
  lists, reruns). Per role: `is_former` (former roles are kept, not
  dropped) and `title_kind` (`position` vs `description`, e.g. "ecologist
  and conservationist").
- **Model: Sonnet 5.** On the Sep 2026 production sample Haiku gave one
  guest another person's role and missed all six possessive company
  mentions ("Heatmap's Katie Brigham"); Sonnet did neither. With the extra
  fields, measured ~100 output tokens per item; `MAX_TOKENS` raised to
  16,000 after a 40-item request overran 4,096. Full production backfill
  estimate: 15,554 unique snippets, ~$11.91 at batch price.

### Displayed current role

`backend/role_selection.py` `pick_current_role()`: a pin in
`host_role_pins` (`migrate_add_host_role_pins.sql`) wins; otherwise the
newest appearance's current roles (not `is_former`, not
`from_other_episode`), ranked position+org > position > description >
bare org. Public `GET /api/people/{id}/current-role` (fetched per card by
`HostProfileCard`, so the graph payload is unchanged); admin
`GET /api/admin/people/{id}/roles`, `PUT`/`DELETE .../role-pin` (the
`RoleEditor` section of Edit Person). No per-row history editing —
deliberately out of scope for now. `merge_people()` moves the dropped
person's pin only if the survivor has none.

## Tests

**`scraper/tests/` now has a real pytest suite** (109 cases) covering
`host_extractor.py`, `episode_name_scanner.py`, and the title/date/dedupe
logic in `scraper.py`. Run it with:

```
cd scraper
python3.11 -m venv venv && source venv/bin/activate   # needs 3.10+ for `X | None` syntax
pip install -r requirements-dev.txt
pytest
```

Most cases are pure-function tests over fixture strings (no DB). A handful —
alias resolution, honorific-based host merging, first-name host attribution —
need real tables and run against a disposable database:

```
scraper/tests/setup_test_db.sh     # creates podcast_scanner_test once
pytest tests/test_db_integration.py
```

These skip automatically (not fail) if that database doesn't exist, so
`pytest` is always safe to run. Never point `SCANNER_TEST_DATABASE_URL` at
`podcast_db` — the fixture truncates its tables between tests.

Two small refactors made `scraper.py`'s previously-inline logic testable
without changing behavior: `compute_duration_seconds`, `compute_published_date`,
and `compute_match_gate` are now standalone functions the class methods call
into, rather than logic embedded directly in `insert_episode` /
`_backfill_from_rss`.

`frontend/src/App.test.js` is still the untouched create-react-app scaffold
(asserts on a "learn react" link this app has never had) and remains
unfixed — no frontend test infra exists yet.

The regressions pinned down as cases, each one a real incident:

| case | what it must keep doing | covered in |
|---|---|---|
| `Jordan Yates` | must **not** credit `Dan Yates` — word-boundary matching | `test_episode_name_scanner.py::TestWordBoundaryMatching` |
| hyphenated surnames | `\b` is not enough; the `[\w-]` guards matter | same |
| raw HTML descriptions | tags become spaces, so words do not glue together | `TestStripHtml` |
| `U+202F` and friends | unicode spaces in titles must not create duplicate rows | `test_scraper.py::TestNormalizeEpisodeTitle` |
| SunCast title drift | numbered feed titles must not insert duplicates — the 80% match-rate guard | `TestComputeMatchGate` |
| published_date | `(A or B) if C else None` precedence bug once nulled every RSS date | `TestComputePublishedDate` |
| organisation names | `Norton Rose Fulbright` and similar are not people | `test_host_extractor.py::TestLooksLikePersonChannel`, `test_episode_name_scanner.py::TestOrganisationFiltering` |
| labelled credits | `Host:` / `Guest:` / `Moderator:`, same-line and block form | `TestLabelledCredits` |
| first-name host credits | show-level hosts named by first name only | `test_db_integration.py::TestShowHostFirstNames` |
| alias matching | a stored nickname matches as well as the canonical name | `test_db_integration.py::TestAliasMatching` |
| `apple_verified` / `manual` | reconciliation must never overwrite these | **not covered** — that logic lives in `backend/main.py` SQL, not the scraper; still a gap |

**Known gap found while writing these tests, not yet fixed:** the
`host_extractor.py` docstring's own example — `"Hosted by Amy Westervelt"`
with no role word in between — matches none of `HOST_PATTERNS`. Every
pattern that starts with "Hosted by" either requires a `Co-` prefix or a
lowercase role word before the capitalized name (`"hosted by partner Todd
Alexander"` works; the bare form doesn't). Tests were written against actual
behavior rather than papering over this.

## Environment

Local backend expects `backend/.env` (`DB_HOST` etc.) and currently points at
a **local** Postgres whose data is stale relative to production. To run the
frontend against live data:

```
REACT_APP_API_URL=https://podcast-network-backend.onrender.com npm start
```

Production allows `localhost:3000` as a CORS origin. Note `homepage` in
`package.json` puts built assets under `/podcast-network/`, so serving a
production build at a domain root needs `PUBLIC_URL=/`.

**Scheduled jobs:** `scrape.yml` at `17 */6 * * *`, `backup.yml` at
`41 4 * * *`. Backups are verified restorable.

### Reaching the production database

The scrapers read `DATABASE_URL` through `os.getenv` and **do not load a .env
file themselves**, so putting one in place is not enough — it has to be
exported:

```
set -a; source .env; set +a          # repo-root .env, gitignored, mode 600
psql "$DATABASE_URL" -c '\dt'
```

`manager.py` also takes the connection string directly, which is what the
workflow does:

```
python3 manager.py status --db "$DATABASE_URL"
python3 manager.py scrape --new-only --db "$DATABASE_URL"
```

Commands: `scrape`, `add`, `status`, `reset`, `backfill`, `backfill-rss`,
`refresh-descriptions`. Useful flags: `--new-only`, `--podcast <exact title>`,
`--max`, `--dry-run`, `--limit`, `--since`, `--force`.

This is the **live production database**. There is no staging copy. The
read-only checks are free; anything that writes falls under the
quantify-and-approve rule above, and the `lock_timeout` rule applies to every
migration.

### Triggering the remote jobs

`gh` is authenticated as FrazRoc and its token carries the `workflow` scope,
and both workflows declare `workflow_dispatch`, so they can be started by
hand:

```
gh workflow run scrape.yml                                   # full sweep
gh workflow run scrape.yml -f podcast_title="Catalyst with Shayle Kann"
gh workflow run backup.yml

gh run list --workflow scrape.yml --limit 5
gh run watch <run-id>
gh run view <run-id> --log-failed
```

Runs fire against **`main`**, not whatever branch is checked out locally, so
scraper changes have to be pushed and mirrored before a dispatched run picks
them up. A full sweep takes 20-45 minutes.

### Two sessions, one database

Work happens in two git worktrees against one production database and one
repo. Git keeps the branches apart; nothing keeps the database apart.

- Only one session runs migrations or bulk writes at a time. The `ALTER TABLE`
  lock cascade above is exactly what concurrent schema work reproduces, and it
  is much harder to diagnose with two actors.
- Don't start a scrape while the other session is mid-migration, or vice
  versa.
- After the other session pushes, `git pull --ff-only` before committing, or
  the branches diverge and need a merge that neither of you intended.

## Open threads

- Free Postgres expires around 2026-10-11 — migration decision needed
- ~3,300 suggestions pending review
- 3 shows skipped in the RSS backfill (~560 episodes); The Hydrogen Podcast
  feed 404s; 7 shows capped at 50 episodes were never re-measured
- Node `channel` / `genre` are podcast properties being shown on people, which
  makes those two graph filters somewhat arbitrary
- `react-scripts` 5.0.1 is the latest release and is unmaintained; a Vite
  migration is the real fix
