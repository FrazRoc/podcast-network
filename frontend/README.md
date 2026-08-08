# Colorado Current: Podcast Network

A tool that scrapes clean energy podcasts, extracts guest and host relationships per episode, and builds a force-directed network graph showing connections between people across the ecosystem. Built as a networking feature for [coloradocurrent.com](https://coloradocurrent.com).

**Branch:** `clean-energy-podcasts`

**Current stats:** 86 shows tracked, 4,000+ episodes, 400+ people, 800+ connections across 30+ podcasts.

---

## Architecture

```
scraper/        Python pipeline: pulls episodes + credits from Apple Podcasts
backend/        FastAPI: serves graph data + admin endpoints
frontend/       React: force-directed network graph + admin review UI
```

Data flows in this order:
1. **manager.py** hits iTunes API → populates `episodes` table
2. **apple_credits_scraper.py** scrapes Apple Podcasts → populates `hosts`, `episode_host`, `host_podcast`
3. **host_extractor.py** extracts hosts from channel names, titles, descriptions → `host_podcast`
4. **episode_name_scanner.py** scans episodes for known names → `episode_host`; or finds new names → `suggestions`
5. **Admin UI at `/admin`** — human reviews suggestions one by one, approve creates host + scans all episodes
6. **FastAPI backend** serves `/api/host-connections`, `/api/people`, `/api/podcasts`, `/api/admin/*`
7. **React frontend** renders network graph at `/`, admin at `/admin`

---

## Prerequisites

```bash
brew install postgresql@15
brew services start postgresql@15
# Python 3.10+ and Node.js 18+ also required
```

---

## 1. Database Setup

```bash
# Create fresh database
createdb podcast_db
psql podcast_db < scraper/podcast-schema.sql

# Run migrations (if setting up from scratch after initial schema)
psql podcast_db < scraper/migrate_add_data_source.sql
psql podcast_db < scraper/migrate_add_suggestions.sql

# Verify
psql podcast_db -c "\dt"
psql podcast_db -c "SELECT COUNT(*) FROM podcast_tracking;"

# Full reset (nuclear option)
dropdb podcast_db && createdb podcast_db
psql podcast_db < scraper/podcast-schema.sql
psql podcast_db < scraper/migrate_add_data_source.sql
psql podcast_db < scraper/migrate_add_suggestions.sql
```

---

## 2. Scraper Setup

```bash
cd scraper
python3 -m venv venv
source venv/bin/activate
pip install psycopg2-binary requests beautifulsoup4 python-dotenv feedparser ratelimit
```

---

## 3. manager.py — Episode Scraper

Pulls episode metadata (titles, dates, descriptions) from the iTunes API.

```bash
cd scraper && source venv/bin/activate

python3 manager.py status                          # show tracking summary
python3 manager.py scrape                          # scrape all pending shows
python3 manager.py scrape --new-only               # only shows with no episodes yet
python3 manager.py scrape --podcast "Volts"        # scrape one specific show
python3 manager.py scrape --max 5                  # limit to N shows (testing)
python3 manager.py add --apple-id 1593204897       # add show (title auto-fetched)
python3 manager.py add --apple-id 1321759767 --podchaser-id 595385
python3 manager.py reset                           # reset failed/stuck → pending
python3 manager.py backfill                        # fill gaps vs iTunes trackCount
python3 manager.py backfill --min-gap 50           # only shows with 50+ missing
python3 manager.py backfill --limit 200            # fetch up to 200 per run
python3 manager.py refresh-descriptions            # fetch show descriptions from RSS
```

**Typical workflow for new shows:**
```bash
python3 manager.py add --apple-id XXXXXXXXXX
python3 manager.py scrape --new-only
python3 manager.py backfill
```

**Permanent failures** (show removed from Apple — never retry):
```bash
psql podcast_db -c "UPDATE podcast_tracking SET status = 'failed',
  error_message = 'PERMANENT: Removed from Apple Podcasts directory'
  WHERE apple_podcast_id = 'XXXXXXXXXX';"
```

---

## 4. apple_credits_scraper.py — Host & Guest Credits

Scrapes "Hosts & Guests" from Apple Podcasts show and episode pages.

```bash
python3 apple_credits_scraper.py shows                          # scrape all show pages
python3 apple_credits_scraper.py shows --podcast "Catalyst"     # one show
python3 apple_credits_scraper.py episodes --hosts-only          # episodes from apple_verified shows only
python3 apple_credits_scraper.py episodes --podcast "Volts"     # one show
python3 apple_credits_scraper.py episodes --batch 50            # limit batch size
python3 apple_credits_scraper.py backfill                       # fill missing profile images
python3 apple_credits_scraper.py all --hosts-only               # run all steps
```

**Notes:**
- `--hosts-only` filters to shows with `data_source = 'apple_verified'` in `host_podcast` — skips shows with no Apple credits data
- Profile images captured when available; `null` otherwise
- 500 errors are normal; timeouts = rate limited

---

## 5. host_extractor.py — Extract Hosts from Metadata

Extracts host names from channel names (iTunes artistName), podcast titles, and show descriptions. Only processes shows without existing `host_podcast` entries.

```bash
python3 host_extractor.py dry-run    # preview without saving
python3 host_extractor.py run        # insert into DB (data_source = 'itunes_artist' or 'parsed_desc')
```

Run this after adding new shows and before scraping episode credits.

---

## 6. episode_name_scanner.py — Episode Name Scanning

Two modes:

**`run` mode** — scans episodes for names already in the `hosts` table and creates `episode_host` links:
```bash
python3 episode_name_scanner.py dry-run --title-only   # preview title matches
python3 episode_name_scanner.py run --title-only        # insert title matches
python3 episode_name_scanner.py dry-run                 # preview title + desc matches
python3 episode_name_scanner.py run                     # insert all matches
```

**`suggest` mode** — finds NEW names not in `hosts`, writes to `suggestions` queue for human review:
```bash
python3 episode_name_scanner.py suggest                        # scan all episodes
python3 episode_name_scanner.py suggest --show "Volts"         # one show
python3 episode_name_scanner.py suggest --title-only           # titles only (safer)
python3 episode_name_scanner.py suggest --limit 200            # limit episodes scanned
```

**Recommended pipeline:**
```bash
# 1. First pass with known names
python3 episode_name_scanner.py run --title-only
python3 episode_name_scanner.py run

# 2. Suggest new names for human review (one show at a time)
python3 episode_name_scanner.py suggest --show "Volts"
python3 episode_name_scanner.py suggest --show "Catalyst"
# ... then review in the admin UI at http://localhost:3001/admin
```

---

## 7. Admin UI — Suggestion Review

Visit `http://localhost:3001/admin` to review the suggestions queue one at a time.

For each suggestion:
- **Left side** — podcast cover art, episode title, date, existing credits, full episode description (with candidate highlighted in yellow, existing hosts/guests underlined green/blue)
- **Right side** — candidate name, source badge, matched context, approve/reject/skip buttons

**Keyboard shortcuts:** `A` = Approve, `R` = Reject + Blocklist, `S` = Skip

**On Approve:**
1. Creates `hosts` record (`data_source = 'approved_suggestion'`)
2. Links to the source episode
3. Scans ALL episodes for that name and links any matches
4. Shows breakdown of additional episodes found by podcast

**Rejected names** are added to `rejected_names` table and never suggested again.

---

## 8. Backend Setup

```bash
cd backend && python3 -m venv venv && source venv/bin/activate
pip install fastapi uvicorn psycopg2-binary python-dotenv
```

Create `backend/.env`:
```
DB_HOST=localhost
DB_NAME=podcast_db
DB_USER=YOUR_MAC_USERNAME
DB_PASSWORD=
```

```bash
python3 -m uvicorn main:app --reload --port 8000
```

**API Endpoints:**
- `GET /api/host-connections` — co-appearance pairs for the network graph
- `GET /api/people` — all unique people with episode counts and podcast lists
- `GET /api/podcasts` — all shows with episode and person counts
- `GET /api/admin/suggestions/next` — next pending suggestion with full episode context
- `GET /api/admin/suggestions/stats` — counts by status (pending/approved/rejected)
- `POST /api/admin/suggestions/:id/approve` — create host, scan all episodes, return summary
- `POST /api/admin/suggestions/:id/reject` — reject + add to permanent blocklist
- `POST /api/admin/suggestions/:id/skip` — push to back of queue

---

## 9. Frontend Setup

```bash
cd frontend && npm install
npm start   # http://localhost:3001
```

- **Network graph:** `http://localhost:3001`
- **Admin review:** `http://localhost:3001/admin`

---

## Running Everything Together

| Tab | Directory   | Command |
|-----|-------------|---------|
| 1   | `backend/`  | `source venv/bin/activate && python3 -m uvicorn main:app --reload --port 8000` |
| 2   | `frontend/` | `npm start` |
| 3   | `scraper/`  | `source venv/bin/activate && python3 manager.py status` |

---

## Data Sources & Provenance

All host/guest records have a `data_source` field tracking how they were found:

| Value | Source |
|-------|--------|
| `apple_verified` | Apple Podcasts "Hosts & Guests" section |
| `itunes_artist` | iTunes `artistName` field or podcast title pattern |
| `parsed_desc` | Extracted from show or episode description |
| `parsed_title` | Extracted from episode title |
| `approved_suggestion` | Human-approved via admin UI |
| `manual` | Manually entered |

---

## Useful DB Queries

```bash
# Network summary
psql podcast_db -c "
SELECT
  (SELECT COUNT(*) FROM hosts) as total_people,
  (SELECT COUNT(*) FROM episode_host) as total_credits,
  (SELECT COUNT(*) FROM suggestions WHERE status = 'pending') as pending_suggestions,
  (SELECT COUNT(*) FROM rejected_names) as rejected_names;
"

# People on multiple shows (graph bridges)
psql podcast_db -c "
SELECT h.first_name || ' ' || h.last_name as name,
       COUNT(DISTINCT e.podcast_id) as show_count,
       ARRAY_AGG(DISTINCT p.title ORDER BY p.title) as shows
FROM hosts h
JOIN episode_host eh ON h.host_id = eh.host_id
JOIN episodes e ON eh.episode_id = e.episode_id
JOIN podcasts p ON e.podcast_id = p.podcast_id
GROUP BY h.host_id, h.first_name, h.last_name
HAVING COUNT(DISTINCT e.podcast_id) > 1
ORDER BY show_count DESC;
"

# Episode counts by show
psql podcast_db -c "
SELECT p.title, COUNT(e.episode_id) as episodes
FROM podcasts p
LEFT JOIN episodes e ON e.podcast_id = p.podcast_id
GROUP BY p.title ORDER BY episodes DESC;
"

# Credits coverage by show
psql podcast_db -c "
SELECT p.title, COUNT(DISTINCT eh.host_id) as people, COUNT(eh.episode_id) as credits
FROM podcasts p
LEFT JOIN episodes e ON e.podcast_id = p.podcast_id
LEFT JOIN episode_host eh ON eh.episode_id = e.episode_id
GROUP BY p.title ORDER BY credits DESC;
"

# Suggestion queue status
psql podcast_db -c "SELECT status, COUNT(*) FROM suggestions GROUP BY status;"
```

---

## Sector Coverage

86 shows tracked across: Solar & Storage, Grid Software, Hydrogen, Geothermal, EV & Transportation, Home Electrification, Carbon Removal, Industrial Decarb, Research/Policy, Fusion, Community Solar.
