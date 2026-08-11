# Colorado Current: Podcast Network

A tool that scrapes clean energy podcasts, extracts guest and host relationships per episode, and builds a force-directed network graph showing connections between people across the ecosystem. Built as a networking feature for [coloradocurrent.com](https://coloradocurrent.com).

**Branch:** `clean-energy-podcasts`

**Current stats:** 86 shows tracked, 10,500+ episodes, 975+ people, 2,000+ connections across 51 podcasts.

---

## Architecture

```
scraper/        Python pipeline: pulls episodes + credits from Apple Podcasts
backend/        FastAPI: serves graph data + admin endpoints
frontend/       React: force-directed network graph + admin UI
```

**Data flow:**
1. **manager.py** → iTunes API → `episodes` table
2. **apple_credits_scraper.py** → Apple Podcasts → `hosts`, `episode_host`, `host_podcast`
3. **host_extractor.py** → channel names/titles/descriptions → `host_podcast`
4. **episode_name_scanner.py** → known names in episodes → `episode_host`; new names → `suggestions`
5. **Admin UI** at `/admin` → human reviews suggestions → approve creates host + scans all episodes
6. **FastAPI** serves graph + admin endpoints
7. **React frontend** → network graph at `/`, admin at `/admin`, `/admin/images`, `/admin/people`

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

# Run migrations
psql podcast_db < scraper/migrate_add_data_source.sql
psql podcast_db < scraper/migrate_add_suggestions.sql
psql podcast_db -c "ALTER TABLE hosts ADD COLUMN IF NOT EXISTS twitter_handle VARCHAR(100);"
psql podcast_db -c "ALTER TABLE hosts ADD COLUMN IF NOT EXISTS bluesky_handle VARCHAR(200);"

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

```bash
python3 manager.py status
python3 manager.py scrape
python3 manager.py scrape --new-only
python3 manager.py scrape --podcast "Volts"
python3 manager.py add --apple-id 1593204897
python3 manager.py reset
python3 manager.py backfill
python3 manager.py backfill --min-gap 50 --limit 200
python3 manager.py refresh-descriptions
```

**Typical workflow for new shows:**
```bash
python3 manager.py add --apple-id XXXXXXXXXX
python3 manager.py scrape --new-only
python3 manager.py backfill
```

**Permanent failures** (show removed from Apple):
```bash
psql podcast_db -c "UPDATE podcast_tracking SET status = 'failed',
  error_message = 'PERMANENT: Removed from Apple Podcasts directory'
  WHERE apple_podcast_id = 'XXXXXXXXXX';"
```

---

## 4. apple_credits_scraper.py — Host & Guest Credits

```bash
python3 apple_credits_scraper.py shows
python3 apple_credits_scraper.py shows --podcast "Catalyst with Shayle Kann"
python3 apple_credits_scraper.py episodes --hosts-only
python3 apple_credits_scraper.py episodes --podcast "Volts"
python3 apple_credits_scraper.py episodes --batch 50
python3 apple_credits_scraper.py backfill
python3 apple_credits_scraper.py all --hosts-only
```

---

## 5. host_extractor.py — Extract Hosts from Metadata

```bash
python3 host_extractor.py dry-run
python3 host_extractor.py run
```

---

## 6. episode_name_scanner.py — Episode Name Scanning

**`run` mode** — scans episodes for known names → `episode_host` links:
```bash
python3 episode_name_scanner.py dry-run --title-only
python3 episode_name_scanner.py run --title-only
python3 episode_name_scanner.py dry-run
python3 episode_name_scanner.py run
```

**`suggest` mode** — finds NEW names → `suggestions` queue for human review:
```bash
python3 episode_name_scanner.py suggest
python3 episode_name_scanner.py suggest --show "Volts"
python3 episode_name_scanner.py suggest --title-only
python3 episode_name_scanner.py suggest --limit 200
```

**Cleanup scripts:**
```bash
python3 cleanup_zero_guests.py          # dry run
python3 cleanup_zero_guests.py --run    # delete wrongly linked Zero guests
```

---

## 7. Backend Setup

```bash
cd backend && python3 -m venv venv && source venv/bin/activate
pip install fastapi uvicorn psycopg2-binary python-dotenv httpx
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

### Graph API Endpoints
- `GET /api/host-connections` — co-appearance pairs for the network graph
- `GET /api/people` — all people with episode counts
- `GET /api/podcasts` — all shows
- `GET /api/proxy/image?url=` — proxies external images to fix canvas CORS

### Admin — Suggestions (`/admin`)
- `GET /api/admin/suggestions/next` — next pending suggestion with full episode context + existing credits
- `GET /api/admin/suggestions/stats`
- `POST /api/admin/suggestions/:id/approve` — creates host, scans ALL episodes, returns summary
- `POST /api/admin/suggestions/:id/approve_only` — creates host, skips source episode, scans others
- `POST /api/admin/suggestions/:id/reject` — rejects + blocklists name permanently
- `POST /api/admin/suggestions/:id/skip` — pushes to back of queue

### Admin — Images (`/admin/images`)
- `GET /api/admin/images/next?skip=` — next person missing image (client-side skip list)
- `GET /api/admin/images/stats`
- `POST /api/admin/images/:id/set_twitter` — extract handle from X or Bluesky URL, fetch image
- `POST /api/admin/images/:id/approve` — saves image_url to hosts.profile_image_url

### Admin — People (`/admin/people`)
- `GET /api/admin/people?q=&filter=&sort=` — search/filter/sort
- `POST /api/admin/people` — create person + fetch image + scan episodes
- `PUT /api/admin/people/:id` — update name/handles, re-scan
- `DELETE /api/admin/people/:id` — cascade delete all links
- `GET /api/admin/people/:id/episodes` — episodes grouped by podcast
- `POST /api/admin/people/:id/scan` — scan all episodes for existing person

---

## 8. Frontend Setup

```bash
cd frontend && npm install
npm start   # http://localhost:3001
```

- **Network graph:** `http://localhost:3001`
- **Suggestion review:** `http://localhost:3001/admin`
- **Image review:** `http://localhost:3001/admin/images`
- **People management:** `http://localhost:3001/admin/people`

---

## Admin UIs

### `/admin` — Suggestion Review
- Left: podcast cover, episode title/date, existing credits (green=host, blue=guest, Apple badge), full description (candidate highlighted yellow, credited names underlined)
- Right: candidate name (editable inline), source badge, matched context, Approve/Approve Person Only/Reject/Skip buttons
- **Keyboard shortcuts:** `A` = Approve, `P` = Approve Person Only, `R` = Reject, `S` = Skip
- Approve auto-scans all episodes, returns breakdown by podcast
- Approve Person Only: creates person but skips the source episode link (use when person is referenced, not a guest)
- Rejected names added to `rejected_names` table and never suggested again

### `/admin/images` — Image Review
- Shows next person missing profile image (ordered by most appearances)
- Paste Twitter/X URL (`https://x.com/handle`) or Bluesky URL (`https://bsky.app/profile/handle`)
- Preview fetched image, Approve saves to `hosts.profile_image_url`
- Client-side skip list so skipped people don't repeat this session
- Bluesky images fetched from `public.api.bsky.app/xrpc/app.bsky.actor.getProfile`

### `/admin/people` — People Management
- Two-column: left = searchable/filterable/sortable list, right = add/edit panel
- **Filters:** All | 0 appearances | Parsed only | No image
- **Sort:** most/fewest appearances, name A–Z/Z–A, newest
- Click person → edit panel shows profile card + episode list (collapsible by show)
- Edit: update name/handles, re-scan (if name changed, clears parsed links first)
- Delete with trash icon + confirm click

---

## Running Everything Together

| Tab | Directory   | Command |
|-----|-------------|---------|
| 1   | `backend/`  | `source venv/bin/activate && python3 -m uvicorn main:app --reload --port 8000` |
| 2   | `frontend/` | `npm start` |
| 3   | `scraper/`  | `source venv/bin/activate && python3 manager.py status` |

---

## Data Sources & Provenance

All host/guest records have a `data_source` field:

| Value | Source |
|-------|--------|
| `apple_verified` | Apple Podcasts "Hosts & Guests" section |
| `itunes_artist` | iTunes `artistName` field or podcast title pattern |
| `parsed_desc` | Extracted from episode description |
| `parsed_title` | Extracted from episode title |
| `approved_suggestion` | Human-approved via admin suggestion review |
| `manual` | Manually entered via admin people page or SQL |

---

## Useful DB Queries

```bash
# Network summary
psql podcast_db -c "
SELECT
  (SELECT COUNT(*) FROM hosts) as total_people,
  (SELECT COUNT(*) FROM episode_host) as total_credits,
  (SELECT COUNT(*) FROM suggestions WHERE status = 'pending') as pending_suggestions,
  (SELECT COUNT(*) FROM rejected_names) as rejected_names,
  (SELECT COUNT(*) FROM hosts WHERE profile_image_url IS NOT NULL) as with_image;
"

# People on multiple shows (graph bridges)
psql podcast_db -c "
SELECT h.first_name || ' ' || h.last_name as name,
       COUNT(DISTINCT e.podcast_id) as show_count,
       COUNT(DISTINCT eh.episode_id) as episodes
FROM hosts h
JOIN episode_host eh ON h.host_id = eh.host_id
JOIN episodes e ON eh.episode_id = e.episode_id
GROUP BY h.host_id, h.first_name, h.last_name
HAVING COUNT(DISTINCT e.podcast_id) > 2
ORDER BY show_count DESC, episodes DESC
LIMIT 20;
"

# Episode counts by show
psql podcast_db -c "
SELECT p.title, COUNT(e.episode_id) as episodes
FROM podcasts p
LEFT JOIN episodes e ON e.podcast_id = p.podcast_id
GROUP BY p.title ORDER BY episodes DESC;
"

# Suggestion queue status
psql podcast_db -c "SELECT status, COUNT(*) FROM suggestions GROUP BY status;"

# Image coverage
psql podcast_db -c "
SELECT COUNT(*) as total,
       COUNT(profile_image_url) as with_image,
       ROUND(COUNT(profile_image_url)::numeric / COUNT(*) * 100) as pct
FROM hosts;
"
```

---

## Sector Coverage

86 shows tracked across: Solar & Storage, Grid Software, Hydrogen, Geothermal, EV & Transportation, Home Electrification, Carbon Removal, Industrial Decarb, Research/Policy, Fusion, Community Solar.
