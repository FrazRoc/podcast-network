# podcast-network

A graph of who appears on which clean-energy podcasts. FastAPI + PostgreSQL 18
on Render, Create React App + Tailwind frontend, Python scrapers driven by
GitHub Actions.

```
backend/    FastAPI app (main.py, ~3.5k lines) — API + admin endpoints
frontend/   CRA + Tailwind. PodcastHostNetwork.js is the graph
scraper/    scrapers, the name scanner, and .sql migrations
```

## Working agreements

**Branches and deploys.** Everything that runs in production runs from
**`main`**: both Render services (backend and frontend) and the GitHub
Actions cron. Each session develops on its own branch and merges to `main`
to ship — `claude/gracious-darwin-vbul7z` (scanner / curation) and
`claude/podcast-guest-metadata-extraction-xvby8l` (guest affiliations).

Until Sep 24 2026 both Render services deployed from
`claude/gracious-darwin-vbul7z`, not `main` — so a merge to `main` from
another branch did not deploy, and whichever session last pushed that branch
decided what was live. Evan switched both services to `main` that day.
Changing a service's branch in Render does **not** redeploy by itself; it
needed a manual deploy. GitHub's deployment records may still show the old
branch name as the environment label — check the deployed commit SHA, not
the label.

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
`image_suggestions`, `host_roles`, `host_social_links`, `episode_tag`,
`credit_suppressions`, and (Sep 2026) `affiliation_extractions`,
`host_affiliations`, `host_role_pins` — see Guest affiliations below.

`hosts.bluesky_handle` exists in production and is read by the backend, but
no migration or `podcast-schema.sql` creates it; a database built from the
repo's SQL alone lacks it (`tests/setup_test_db.sh` adds it).

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
role is picked from their rows (see Displayed current role). Tables in
`migrate_add_host_affiliations.sql`:
`affiliation_extractions` (one row per processed `(episode, host)`, whatever
the outcome, with the exact snippet sent) and `host_affiliations` (the
facts; `data_source` `llm_extracted` or `manual`, and `manual` is never
overwritten). Both FK to `episode_host` with `ON UPDATE/DELETE CASCADE`, so
merges and credit deletions carry through without touching that code.

- **Status (Sep 24 2026):** all three migrations (`host_affiliations`,
  `host_role_pins`, `organizations`) have run on production and the code is
  on `main`. Random samples of 200, 200 and 1,000 were reviewed, then
  **stage 2 — the whole backlog, 14,699 appearances (~$11.35) — was
  submitted** as `msgbatch_01Nnc1zGabXa8tDzKB2FW4ue`; `collect` records it.
- **Scheduled:** `scrape.yml` step "Extract guest roles and companies" runs
  `extract_affiliations.py run --model claude-sonnet-5 --limit 1000
  --max-cost 2` every 6 hours: collect earlier batches, then submit
  appearances not yet processed. Skipped until the `ANTHROPIC_API_KEY` GitHub
  Actions secret exists, skipped on single-show dispatches, and
  `continue-on-error` so it can never stop the run being recorded. Over the
  cost cap, `run` logs and waits for the next run (a manual `submit` exits
  with an error). `collect` runs `organizations.py sync` afterwards, so new
  company spellings reach Company Admin without a separate step. The default
  model is now Sonnet 5. Locally the key lives in the affiliations
  worktree's `.env` (gitignored) — load it alongside `DATABASE_URL`.
- Production has 16,074 guest credits (7,148 guests); only 24 are someone on
  their own show — the 1,216 in the Aug export had been cleaned up since.
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
  the guard against the model supplying a company from outside knowledge. The match is on whole words (a bare substring test accepted "Director"
  from "Directorate"), and a title may be the singular of a plural in the
  text — "cofounders and managing directors of remove, Marian Krüger and
  Hans Westerhof" yields "cofounder and managing director" for each (+s,
  +es, y→ies only). Before that, shared titles were silently dropped and
  only the company kept. Companies still need an exact whole-word match.
- **Organisations inside titles** (Sep 25 2026, after a hand review found
  ~130 company-less rows like "Grist reporter", "BBC Science Correspondent",
  "Aurora's Head of Consulting"): the prompt now shows that pattern, names
  the company when it's in a nearby sentence ("a company called Sakuu … CTO
  Karl Littau"), and each snippet starts `[Show: <podcast title>]` so "our
  Head of Italy" on "Energy Unplugged by Aurora" can name Aurora verbatim.
  `SNIPPET_BEFORE` went 120 → 200 for the nearby-sentence case. After the
  verbatim check, `with_title_orgs()` (`backend/title_orgs.py`) catches what
  the model still leaves in a title, by looking pieces of the title up among
  known organisation spellings; it refuses ordinary words ("Science fiction
  writer", "Earth scientist", "Australian energy analyst"), title acronyms
  (EVP), events (COP26), honours and books, and press titles at non-media
  companies ("Tesla reporter"). On the reviewed data it made no wrong matches
  (121 of 134 found). Existing rows were fixed by a one-off pass, not by
  re-extraction.
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
- `submit`/`run` take `--random` to pick `--limit` appearances at random
  rather than in episode order (the oldest episodes of a few shows).
- Known rough edges from stage 1, left for the normalisation step rather
  than fixed in extraction: plural titles when the model copies them as
  written ("Senators", "historians of technology"); editorial wording copied into titles ("the controversial pick to
  be the president"); loose descriptions ("an experienced solar
  professional with a broad knowledge of the industry"); a single old
  appearance shows as current (Etosha Cave, 2019). Leading "a"/"an"/"the"
  is stripped from titles and a trailing possessive from companies at
  extraction; the first 200 were cleaned by a one-off UPDATE (9 titles, 1
  company). Some organisations style their names lowercase ("remove", a CDR
  accelerator) — that is the name, not an extraction error.
- Second random 200 run Sep 24 2026 ($0.15): ~400 appearances processed in
  total, 417 roles, 35 mentioned-only, 8 flagged hosts.
- Stage 1 produced 168 distinct company strings from 174 appearances, so
  the full backfill will mean thousands of organisations to normalise.

### Displayed current role

`backend/role_selection.py` `pick_current_role()`: a pin in
`host_role_pins` (`migrate_add_host_role_pins.sql`) wins. Otherwise
(rewritten Sep 25 2026 — "newest appearance wins" kept showing a bare
company or a bare title when an earlier appearance had the full role, and
pins don't scale): current roles only (not `is_former`; `from_other_episode`
only as a last resort); a title with no company borrows the company from
the person's latest row on the same show (Aurora/BNEF staff on their own
podcasts); the employer is the newest row naming any organisation, counting
its parent/sub-orgs as one (`top_org_id`), and the shown role is the newest
position there, skipping bare words like "researcher"/"author" when a fuller
title exists; if the newest appearance is 3+ years past that, the old
newest-appearance ranking applies (position+org > position > description >
bare org). A borrowed company is marked `company_inferred`. Titles are also
singularised ("Reporters" → "Reporter") in `tidy_title`. Public `GET /api/people/{id}/current-role` (fetched per card by
`HostProfileCard`, so the graph payload is unchanged); admin
`GET /api/admin/people/{id}/roles`, `PUT`/`DELETE .../role-pin` (the
`RoleEditor` section of Edit Person). No per-row history editing —
deliberately out of scope for now. `merge_people()` moves the dropped
person's pin only if the survivor has none.

The shown title is tidied at display time only (`display_title()` /
`format_for_display()` in `role_selection.py`): a leading "a"/"an"/"the" is
dropped, positions get title case ("Co-Founder and CEO"; small words stay
lower, words that already have a capital — CEO, DOE's — are untouched), and
descriptions get a capital first letter only ("Senior investigative data
reporter"). Stored rows stay verbatim for the snippet check and the
Edit Person history; pins are shown exactly as typed.

Pins are deliberate and never expire: a pinned person keeps the pin even
after a newer appearance with a different role (Evan's choice; the panel
shows what the rule alone would pick, next to the pin).

The admin People list (`GET /api/admin/people`) filters on it ("Has role &
company", "Has role only", "Has company only"), sorts by company
(`company_asc`), and shows "title · company" per row. `_all_current_roles()`
computes everyone's current role in one pass with the same
`pick_current_role()` and joins it into the list query as unnested arrays,
so the rule lives in one place; a test pins list and panel to the same
answer. ~1–2 s with stage 1's 210 rows — if it slows after stage 2, store
the current role instead of recomputing it per request.

### Organisations and Company Admin

`migrate_add_organizations.sql`: `organizations` (name, `org_type` —
company / nonprofit / government / academic / research / media / investor /
association / other — `parent_org_id`, `website_domain`, `not_an_org`),
`organization_aliases` (every spelling, `normalized_name` unique),
`not_same_org_pairs`, and `host_affiliations.company_key`. Also enables
`pg_trgm`.

- **Roles link through the alias, not a stored org id:**
  `host_affiliations.company_key = organization_aliases.normalized_name`,
  with the key from `backend/org_names.py normalize_org_name()` (case,
  punctuation, leading "the", trailing possessive, legal suffixes like
  Inc/LLC/Ltd — not "Company" or "Group", which are parts of names). A merge
  or new alias re-links every past and future role at once.
- **New spellings become organisations automatically** —
  `scraper/organizations.py sync` (stamps missing keys, creates one org +
  `auto` alias per unaliased key, named by the most common spelling, ties to
  the bare form). Evan chose auto-create + review merges over approving each
  company: the first ~1,300 roles gave ~1,075 organisations, nearly all seen
  once.
- **Company Admin** (`/admin/companies`, `AdminCompanies.js`): list (search
  names and spellings; views All / No type yet / Not an organisation; type
  filter; sort by people), edit panel (name, type, website, parent, not an
  organisation, merge a duplicate in, spellings, sub-orgs, people with a
  "current" badge, optionally including sub-orgs), and a **Merge
  suggestions** tab. Suggestions are computed per request, ranked by people
  affected: `acronym` (initials of a multi-word name, ≥3 letters — "BNEF" /
  "Bloomberg New Energy Finance"), `similar` (pg_trgm similarity ≥ 0.5 —
  "Bloomberg NEF" / "BloombergNEF"), `contains` (one name plus more words —
  "Bloomberg" / "Bloomberg Green", often a parent). Actions: same (keep
  either), is part of (sets parent), different (recorded in
  `not_same_org_pairs`), skip.
- Endpoints: `GET /api/admin/companies`, `GET/PUT /api/admin/companies/{id}`,
  `POST .../{keep}/merge/{drop}`, `POST /api/admin/companies/not-same`,
  `GET /api/admin/companies-suggestions`.
- **The suggestion queue is stored**, not computed per request
  (`company_merge_suggestions`, `migrate_add_company_merge_suggestions.sql`;
  logic in `backend/org_suggestions.py`). The full pass took ~95 s per page
  load at 7,500 organisations; reading the stored queue takes <1 s. Rebuilt
  by `organizations.py sync` (so after every scheduled extraction),
  `organizations.py suggestions`, and the tab's Recompute button (a
  background task, ~1 min). "Not the same" deletes its row, a merge cascades
  the dropped company's rows away, and parent/child pairs are filtered at
  read time; pairs a merge newly creates appear at the next rebuild.
  Sep 24 2026 after stage 2: 7,516 companies, 6,543 suggestions (5,013
  similar, 1,382 contains, 148 acronym); 6,308 companies have one person.
- **Cleanup, Sep 24 2026:** 290 companies merged into 248 (formatting
  variants, one-word misspellings, 88 acronyms checked against episode
  text) and 2,062 pairs marked different (no distinctive word in common,
  wrong acronym expansions); logs of both are kept outside the repo. Then
  combined companies ("Columbia University and NASA" — one title at two
  organisations) were split: 58 roles became 114, 52 combined companies
  deleted; 51 books/descriptions marked not an organisation. The prompt
  now asks for exactly one organisation per `company` (a shared title
  becomes one entry per organisation) — written while the API account had
  no credit, so not yet checked against a live model run. Queue after all
  this: 3,484.
- **Places and fragments are never companies** (Evan, Sep 2026):
  `org_names.is_place()` (a state, country, region or their abbreviation —
  "California", "UK", "AZ", "North America"; cities are left alone, they
  stand for a city government) and `is_fragment()` (cut off mid-name —
  "the University of", "Dun &", "…" — or a bare placeholder like "the
  Centre", "Solar", "the company"). The extraction drops them as companies,
  the sync creates any that slip through as `not_an_org`, and the displayed
  current role blanks a company marked not an organisation. 105 existing
  ones were marked (70 places, 35 fragments); the fragments alone sat in
  400+ merge suggestions. `extract_affiliations.py reextract` re-reads the
  affected appearances (244) with Opus 5 and a wider window
  (`WIDE_WINDOW`) to find the real organisation — built but never needed:
  with the API account out of credit, Claude Code read all 244 itself and
  rewrote them (75 gained a real organisation, the rest kept their title and
  lost the fake company). Watch for real names that trip the
  fragment rule (e.g. "Compostable LA" was caught by a French "la" and
  unmarked; "Planet A", "Instant ON" are deliberately allowed).
- The merge-suggestions tab hides every card naming a company that was just
  merged away and reloads the queue; skipped cards stay hidden until
  Refresh. (First version left those cards in place, and acting on them
  failed with "Company not found".)
- **Ambiguous spellings are checked before a merge** (Sep 25 2026, after
  "aurora" had been merged into Aurora Solar and pulled 21 Aurora Energy
  Research people with it; Ceres and EDF were the same shape). Every merge
  in Company Admin first calls `GET .../{keep}/merge/{drop}/preview`; any
  spelling `org_names.ambiguous_spelling()` flags — one non-acronym word
  that is only the first word of a longer name — is listed in a dialog,
  unticked by default. Unticked spellings are sent as `split_alias_ids` and
  become their own companies instead of moving. It flags Harvard/Fervo-style
  names too; tick those. A spelling already merged wrongly can be split off
  with the ✕ on its chip (`POST .../{org_id}/aliases/{alias_id}/split`).
- **Role lines show the organisation's name** (`organizations.name`), not
  the episode's wording, on the public card, People list and Company Admin —
  so a merge or rename reaches every role line at once. People Admin's role
  history keeps each show's own wording (`company_as_written`). Company
  detail's "current" flag matches on `org_id` (a pin, being free text, on
  spelling), so a rename that adds no alias doesn't unmark anyone.
- Sep 2026 name cleanup: 386 renames (every lowercase leading "the", glued-on
  descriptions like "the research firm Wood Mackenzie"), 47 merges (incl.
  Aurora's "our Berlin office"-style phrasings into Aurora Energy Research),
  112 marked not an organisation (book titles, politicians/administrations as
  employer, unnamed descriptions, reports, state names). Undo:
  `names_undo.json` in that session's scratchpad.
- Sep 2026 types: 3,018 companies typed — the 150 with the most people by
  hand, the rest by name rules (≈85% right on a sample; think tanks are
  `research`). 3,451 one-off names with no clue stay untyped.
- Company Admin rows show the type (one colour per type) and the number of
  open merge suggestions; the Edit Company panel lists them with Merge this
  into it / Merge it into this / Different, and its Merge section defaults to
  merging the viewed company into another. "Open" = `_LIVE_SUGGESTIONS` in
  main.py, shared with the queue.
- "Similar" suggestions skip pairs whose only overlap is generic words
  (`org_suggestions.GENERIC_WORDS`: "University of Bern" / "University of
  Oxford", "Energy UK" / "C12 Energy") — trigram similarity alone put ~290
  such pairs in the queue (1,657 → 1,375 after the fix, Sep 2026).
- **Title spelling is standardised** by `role_selection.tidy_title()` —
  co- roles hyphenated (cofounder -> co-founder), CEO/COO/CFO/CTO/CMO/CRO/
  CPO/CLO for their "Chief … Officer" (ambiguous CSO / CCO / Chief
  Investment stay spelled out, Evan's call), VP/SVP/EVP, a spaced "&" ->
  "and", short forms written out (Sr./Prof./Assoc./Rep./Sen./Gov., Ph.D.
  -> PhD), Director-General / Secretary-General / Editor-in-Chief
  hyphenated. The extractor applies it after the verbatim check (so stored
  titles are tidied, not verbatim, for these spellings), and display_title
  applies it too. ~1,350 stored titles were rewritten to match (Sep 2026).
  Chairman/Chairwoman were deliberately NOT changed to Chair.
- **Stats page, "Who the guests are"** (Sep 2026, `backend/org_stats.py`,
  `/api/stats/show-guest-mix|revolving-door|top-organizations|guest-mix-by-year|guest-roles`):
  guests only (episode_host.is_guest), current roles unless the chart is
  about former ones, an org's type falling back to its top parent's.
  A show's "house" organisation (BNEF on Switched On, Aurora on Energy
  Unplugged…) is inferred from who it books, since podcasts have no
  publisher column; Most-Booked leaves those guests out by default.
  Role kinds come from title wording (`org_stats.ROLE_KINDS`, first match
  wins) — approximate by design.
- **Politicians are one title at one organisation** (Evan, Sep 2026,
  `backend/politicians.py`): Representative @ U.S. House, Senator @ U.S.
  Senate, state roles @ "State of X", city roles @ "City of X" (London's
  mayor @ Greater London Authority), U.S. cabinet @ their department
  (deputy/under/assistant secretaries keep their own title), EPA
  Administrator, FERC Commissioner/Chair. The state comes from the title,
  the company, or the text around the name; unresolvable ones are left
  alone. Party labels are dropped from titles. The extractor applies it
  (even when the company was a place it would otherwise drop), and 309
  stored roles were rewritten (Sep 2026). "State of X" and "City of X" are
  government organisations, cities under their state.
  Pass 2 (`normalize_government_role`, same module; the extractor calls
  both via `normalize_any_government_role`): other countries' heads of
  government and ministers @ "Government of <Country>" (Scottish / Welsh /
  Victorian Government, Government of Alberta for sub-national), MPs @ UK
  House of Commons / Australian House of Representatives (constituency
  names like "Kingswood" are not organisations), U.S. envoys @ U.S.
  Department of State, UN / EU envoys @ United Nations / European Union,
  U.S. President and VP @ White House, and politicians' staff @ the body
  their politician is in (White House, State of X, U.S. Senate/House,
  Government of X). 227 roles rewritten; "Chilean government"-style
  variants merged into "Government of X"; White House councils sit under
  White House. Company presidents/VPs whose company wasn't named, and COP
  presidencies, are deliberately left alone.
- **"former" lives in the flag, not the title**: a leading "former"/"ex-"
  is stripped and the role marked former, at extraction and in the data
  (216 titles, Sep 2026).
- **Enrichment from outside the episode text** (`scraper/enrich.py`, Sep
  2026; migration `migrate_add_enrichment.sql`): `orgs` fills websites (a
  show-note link whose domain *is* the name, then Clearbit's still-live
  undocumented autocomplete or Wikidata depending on the kind of
  organisation; one-word names need agreement or review) and Wikidata
  facts (type when unset, country, HQ city + coordinates, founding year,
  Wikipedia/LinkedIn/X/Bluesky, Commons logo, parent links); `people-links`
  fills LinkedIn/X/Bluesky only from show-note links that carry the
  person's own name (nearness alone picked up co-guests' and hosts'
  accounts); `people-wiki` fills photo/Wikipedia/links from Wikidata only
  when the entry names an organisation we have for them (namesakes).
  Dry run by default (plan JSON + review CSV); `--apply` writes. Web
  responses cache in `scraper/.enrich_cache/` (gitignored). Values typed in
  Company/People Admin are marked `admin` (website_source / field_sources)
  and never overwritten. Wikidata people who died before 2010 or were born
  before 1900 are never matched (the naturalist John Muir is a Sierra Club
  member). Hand decisions after a dry run go in a `--manual` JSON
  (websites incl. full links, no_parent, no_wikidata).
  First run (Sep 25 2026): companies — 3,738 websites, 1,491 Wikidata
  matches (country, HQ, founding year, Wikipedia/LinkedIn/X, 660 Commons
  logos), ~400 newly typed, 51 parent links; people — LinkedIn 8 → 914,
  X 186 → 407, photos 246 → 401, Wikipedia 0 → 193. Undo snapshots of
  every touched row were kept from that session.
- **Company logos** (`/api/logo/{org_id}`, `OrgLogo.js`): logo.dev by the
  organisation's website with `LOGO_DEV_TOKEN` (the same logo.dev account as
  Colorado Current; set it on the Render backend), else its Wikimedia
  Commons logo, else its parent's; cached in memory and by browsers for a
  week; a miss shows the initial. Shown in Company Admin rows / panel /
  merge cards, People list and the public card's role line, and Most-Booked
  Organisations. `Credits.js` (sidebar and Stats) credits Logo.dev and
  Wikimedia Commons, as their free terms ask. The image proxy allows
  wikimedia.org for people's Commons photos.
- Not yet: a public company view.

## Tests

**`scraper/tests/` has a real pytest suite** (~300 cases) covering
`host_extractor.py`, `episode_name_scanner.py`, the title/date/dedupe logic
in `scraper.py`, `extract_affiliations.py` (`test_extract_affiliations.py`)
and the backend's current-role rule, role query and People-list filters
(`test_role_selection.py`). Run it with:

```
cd scraper
python3.11 -m venv venv && source venv/bin/activate   # needs 3.10+ for `X | None` syntax
pip install -r requirements-dev.txt
pytest
```

Most cases are pure-function tests over fixture strings (no DB). A handful —
alias resolution, honorific-based host merging, first-name host attribution,
affiliation recording, cascades on merge/delete, role queries — need real
tables and run against a disposable database. Re-run `setup_test_db.sh`
after adding a migration; it applies the migrations it lists, so a new one
must be added to its list:

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

### Diagnostics page (`/admin/diagnostics`)

`GET /api/admin/diagnostics` (show coverage, host/guest balance, credits per
episode, mangled names) plus, since Sep 25 2026, `GET
/api/admin/diagnostics/pipeline`, loaded separately because it computes
everyone's current role (~6 s):
- **Scanner health**: episodes by week *published* (last 12 weeks; red when a
  finished week is under 75% of the median). The small "+N added" figure is
  by `created_at` and spikes on back-catalogue imports, so it is context only.
- **Scanner refresh bug (fixed Sep 25 2026)**: scheduled runs use `scrape
  --new-only` (shows with no episodes), so existing shows only got new
  episodes via `manager.backfill_episodes`, which fired on a 10+ episode
  gap vs Apple's trackCount. After the RSS back-catalogue import made most
  shows "complete", weekly shows went ~10 weeks without a refresh (90 recent
  episodes across 38 shows, incl. Volts/Catalyst/Shift Key, were missing).
  `refresh_limit()` now also fetches the latest 50 whenever Apple's show
  `releaseDate` (its newest episode) is newer than ours, and the backfill
  step stores that date in `podcast_tracking.latest_episode_date` (which
  `process_all_pending` used to fill with the scrape time). Diagnostics
  uses it: **"missing new episodes"** = Apple has newer (scanner problem);
  **"quiet"** = overdue by rhythm and Apple has nothing newer (show paused).
- **Overdue shows**: `_add_publishing_rhythm()` takes the median gap of each
  show's last 20 episodes; overdue past 3 gaps (min 21 days), "ended?" past
  15 gaps (min 180 days). Also a sortable "Last ep." column on the coverage
  table.
- **Roles**: current-role completeness for everyone ever credited as a guest
  (links to People Admin's `?filter=role_*`, which now deep-links), the
  extraction backlog by status, and per show the share of read guest
  appearances that gave a role (unread shows sort last).
- **Company data / People data** (`GET /api/admin/diagnostics/data`):
  organisation type / website / Wikidata / parent completeness (orgs with 3+
  people by default, or all), the live merge-queue size, guests' profile
  links, and person records whose name looks like an organisation
  (`ORG_LIKE_NAME_SQL` — a worklist; "Power"/"Cash" left out as real
  surnames). Links go to Company Admin `?view=untyped|no_website|not_org`
  and People Admin `?filter=no_linkedin|no_image|org_like_name` (new
  filters/views, both pages now read them from the address bar).
- "Worth looking at" lists expand with Show all, and each show links to its
  episodes missing credits.
- Credits-per-episode bars link to Episodes `?credit_filter=count_N`
  (`no_credit` for 0). Mangled names have a **Fix** button:
  `POST /api/admin/people/{id}/repair-name` keeps every credit (unlike a
  People Admin rename, which unlinks inferred credits and rescans), then
  credits any episode that names the real spelling.

### Reaching the production database

The scrapers read `DATABASE_URL` through `os.getenv` and **do not load a .env
file themselves**, so putting one in place is not enough — it has to be
exported:

```
set -a; source .env; set +a          # repo-root .env, gitignored, mode 600
psql "$DATABASE_URL" -c '\dt'
```

Each worktree has its own `.env` — they are gitignored, so they are not
shared. `DATABASE_URL` lives in `podcast-network-scanner/.env`;
`ANTHROPIC_API_KEY` in `podcast-network-affiliations/.env`.

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
scraper changes have to be merged to `main` before a dispatched run picks
them up. A full sweep takes 20-45 minutes.

### Several sessions, one database

Work happens in several git worktrees against one production database and
one repo. Git keeps the branches apart; nothing keeps the database apart.

```
~/projects/podcast-network               main
~/projects/podcast-network-scanner       claude/gracious-darwin-vbul7z
~/projects/podcast-network-affiliations  claude/podcast-guest-metadata-extraction-xvby8l
```

- Only one session runs migrations or bulk writes at a time. The `ALTER TABLE`
  lock cascade above is exactly what concurrent schema work reproduces, and it
  is much harder to diagnose with two actors.
- Don't start a scrape while the other session is mid-migration, or vice
  versa.
- Before merging to `main`, fetch it and merge it into your branch first;
  run the tests and `CI=true npm run build` on the merged result, since it
  includes the other session's work. After another session merges to
  `main`, bring `main` into your branch (`git pull origin main`) before your
  next commit — a plain `--ff-only` fails once the branches have diverged.
- Before a migration or bulk write, check nothing else is running:
  `gh run list --workflow scrape.yml --limit 1` and non-idle rows in
  `pg_stat_activity`.

## Open threads

- Free Postgres expires around 2026-10-11 — migration decision needed
- ~3,300 suggestions pending review
- 3 shows skipped in the RSS backfill (~560 episodes); The Hydrogen Podcast
  feed 404s; 7 shows capped at 50 episodes were never re-measured
- Node `channel` / `genre` are podcast properties being shown on people, which
  makes those two graph filters somewhat arbitrary
- `react-scripts` 5.0.1 is the latest release and is unmaintained; a Vite
  migration is the real fix
- Guest affiliations, in the agreed order: `scrape.yml` extract step (+
  `ANTHROPIC_API_KEY` GitHub secret) → stage 2 backfill (~$12, needs
  approval) → admin review lists for `appears_on_episode = false` (feeds the
  public-figures cleanup) and status `host` (feeds the separate hosts
  process) → Company Admin (`organizations` / aliases / matching / review
  page). Possibly show "as of <year>" on the public card for old roles.
- Suspected false guest credits surfaced by the extraction: Joe Biden on
  three POLITICO Energy episodes (talked about, not appearing) — and the
  whole `appears_on_episode = false` set once stage 2 runs.
