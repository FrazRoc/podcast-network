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
- Insert loops are per-row and slow; `execute_values` batching is the fix.

## Tests — the gap worth closing first

**There is no working test suite.** `frontend/src/App.test.js` is the
untouched create-react-app scaffold, asserting on a "learn react" link this
app has never had, and `setupTests.js` imports `@testing-library/jest-dom`,
which has never been a declared dependency. There are no Python tests at all.

Every scanner fix so far has been verified by running against production data
and eyeballing samples, which means **nothing stops a future change from
silently reintroducing a bug that was already fixed.** The regressions worth
pinning down as cases, each one a real incident:

| case | what it must keep doing |
|---|---|
| `Jordan Yates` | must **not** credit `Dan Yates` — word-boundary matching |
| hyphenated surnames | `\b` is not enough; the `[\w-]` guards matter |
| raw HTML descriptions | tags become spaces, so words do not glue together |
| `U+202F` and friends | unicode spaces in titles must not create duplicate rows |
| SunCast title drift | numbered feed titles must not insert duplicates — the 80% match-rate guard |
| published_date | `(A or B) if C else None` precedence bug once nulled every RSS date |
| organisation names | `Norton Rose Fulbright` and similar are not people |
| labelled credits | `Host:` / `Guest:` / `Moderator:`, same-line and block form |
| first-name host credits | show-level hosts named by first name only |
| alias matching | a stored nickname matches as well as the canonical name |
| `apple_verified` / `manual` | reconciliation must never overwrite these |

These are pure-function cases over text — they need fixture strings, not a
database — so a plain `pytest` file next to the scanner would cover most of
it without any fixtures or network.

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

## Open threads

- Free Postgres expires around 2026-10-11 — migration decision needed
- ~3,300 suggestions pending review
- 3 shows skipped in the RSS backfill (~560 episodes); The Hydrogen Podcast
  feed 404s; 7 shows capped at 50 episodes were never re-measured
- Node `channel` / `genre` are podcast properties being shown on people, which
  makes those two graph filters somewhat arbitrary
- `react-scripts` 5.0.1 is the latest release and is unmaintained; a Vite
  migration is the real fix
