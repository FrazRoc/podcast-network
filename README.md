# Colorado Current: Podcast Network

A tool that scrapes clean energy podcasts, extracts guest and host relationships per episode, and builds a force-directed network graph showing connections between people across the ecosystem. Built as a networking feature for [coloradocurrent.com](https://coloradocurrent.com).

**Branch:** `clean-energy-podcasts`

---

## Architecture

```
scraper/        Python pipeline: pulls episodes + credits from Apple Podcasts
backend/        FastAPI: serves graph data via 3 API endpoints
frontend/       React: force-directed network graph visualization
```

Data flows in this order:
1. **manager.py** hits iTunes API → populates `episodes` table
2. **apple_credits_scraper.py** scrapes Apple Podcasts episode pages → populates `hosts` and `episode_host`
3. **FastAPI backend** queries DB and serves `/api/host-connections`, `/api/people`, `/api/podcasts`
4. **React frontend** renders the network graph

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

# Verify — should show 15 tables and 80+ seeded shows
psql podcast_db -c "\dt"
psql podcast_db -c "SELECT COUNT(*) FROM podcast_tracking;"

# Full reset (nuclear option)
dropdb podcast_db && createdb podcast_db
psql podcast_db < scraper/podcast-schema.sql
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

Pulls episode metadata (titles, dates, descriptions) from the iTunes API and RSS feeds.

```bash
cd scraper
source venv/bin/activate

# Show current status of all tracked podcasts
python3 manager.py status

# Scrape all pending shows (first-time run or after adding new shows)
python3 manager.py scrape

# Only scrape shows with no episodes yet (safe to run after adding new shows)
python3 manager.py scrape --new-only

# Scrape a specific show (resets it to pending first)
python3 manager.py scrape --podcast "Volts"

# Limit to N shows (useful for testing)
python3 manager.py scrape --max 5

# Add a new show by Apple ID (title auto-fetched from iTunes)
python3 manager.py add --apple-id 1593204897

# Add a new show with a known Podchaser ID too
python3 manager.py add --apple-id 1321759767 --podchaser-id 595385

# Reset failed/stuck shows back to pending
# (skips shows marked PERMANENT in error_message)
python3 manager.py reset

# Backfill historical episodes for shows where we have fewer than iTunes reports
# Compares DB count vs iTunes trackCount, re-scrapes gaps with limit=200
python3 manager.py backfill

# Backfill only shows with a gap of 50+ episodes
python3 manager.py backfill --min-gap 50

# Backfill with a custom episode fetch limit (iTunes max is 200)
python3 manager.py backfill --limit 200
```

**Typical workflow for adding new shows:**
```bash
python3 manager.py add --apple-id XXXXXXXXXX
python3 manager.py scrape --new-only
python3 manager.py status
```

**Typical workflow for keeping shows up to date:**
```bash
python3 manager.py backfill        # pick up new + historical episodes
python3 manager.py status
```

---

## 4. apple_credits_scraper.py — Host & Guest Credits

Scrapes the "Hosts & Guests" section from Apple Podcasts pages. Populates the `hosts`, `episode_host`, and `host_podcast` tables. This is what builds the network graph edges.

```bash
cd scraper
source venv/bin/activate

# Step 1: Scrape show pages for permanent hosts (~2 min, run once per show)
# Populates host_podcast table
python3 apple_credits_scraper.py shows

# Scrape show page for one specific podcast
python3 apple_credits_scraper.py shows --podcast "Catalyst with Shayle Kann"

# Step 2: Scrape all episode pages for credits (~40+ min for 2,000+ episodes)
# Only scrapes episodes from shows that have host data (recommended)
python3 apple_credits_scraper.py episodes --hosts-only

# Scrape all episodes regardless of whether show has host data
python3 apple_credits_scraper.py episodes

# Scrape episodes from one specific podcast
python3 apple_credits_scraper.py episodes --podcast "Volts"

# Scrape a limited batch (useful for testing)
python3 apple_credits_scraper.py episodes --batch 50

# Step 3: Backfill missing profile images for hosts
python3 apple_credits_scraper.py backfill

# Run all three steps in sequence
python3 apple_credits_scraper.py all
python3 apple_credits_scraper.py all --hosts-only
```

**Notes on Apple credits data:**
- Data is crowdsourced — coverage varies by show (~15% of episodes have credits)
- Shows with no "Hosts & Guests" section on the show page will have zero episode credits too
- Profile images are captured when available; initials-only avatars return `null`
- 500 errors are normal (pulled/regional episodes); timeouts mean you're being rate-limited

**After adding new shows and scraping episodes, re-run credits:**
```bash
python3 apple_credits_scraper.py shows --podcast "New Show Name"
python3 apple_credits_scraper.py episodes --podcast "New Show Name"
```

---

## 5. Backend Setup

```bash
cd backend
python3 -m venv venv
source venv/bin/activate
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
# Start the API server
python3 -m uvicorn main:app --reload --port 8000

# Test endpoints
curl http://localhost:8000/api/host-connections | python3 -m json.tool | head -40
curl http://localhost:8000/api/people | python3 -m json.tool | head -40
curl http://localhost:8000/api/podcasts | python3 -m json.tool | head -40
```

**Endpoints:**
- `GET /api/host-connections` — co-appearance pairs for the network graph
- `GET /api/people` — all unique people with episode counts and podcast lists
- `GET /api/podcasts` — all shows with episode and person counts

---

## 6. Frontend Setup

```bash
cd frontend
npm install
npm start   # opens at http://localhost:3001 (3000 is taken by Colorado Current)
```

Make sure the backend is running on port 8000 first.

---

## Running Everything Together

You need three terminal tabs:

| Tab | Directory  | Command |
|-----|------------|---------|
| 1   | `backend/` | `source venv/bin/activate && python3 -m uvicorn main:app --reload --port 8000` |
| 2   | `frontend/` | `npm start` |
| 3   | `scraper/`  | `source venv/bin/activate && python3 manager.py status` |

---

## Podcast List

80+ clean energy podcasts are tracked. The master list with Apple IDs, Podchaser IDs, hosts, and sector tags lives in `clean_energy_podcasts.xlsx`.

Sectors covered: Solar & Storage, Grid Software, Hydrogen, Geothermal, EV & Transportation, Home Electrification, Carbon Removal, Industrial Decarb, Research/Policy, Fusion.

To check which shows have Apple Credits data (and are therefore worth scraping for episode credits):
```bash
psql podcast_db -c "
SELECT p.title, COUNT(DISTINCT hp.host_id) as hosts
FROM podcasts p
LEFT JOIN host_podcast hp ON hp.podcast_id = p.podcast_id
GROUP BY p.title
ORDER BY hosts DESC;
"
```

---

## Useful DB Queries

```bash
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

# All unique people in the network
psql podcast_db -c "
SELECT h.first_name || ' ' || h.last_name as name,
       COUNT(DISTINCT eh.episode_id) as appearances
FROM hosts h
JOIN episode_host eh ON h.host_id = eh.host_id
GROUP BY h.host_id, h.first_name, h.last_name
ORDER BY appearances DESC;
"
```

---

## Permanent Failures

Some shows get removed from Apple Podcasts and will never scrape successfully. Mark them so `reset` skips them:

```bash
psql podcast_db -c "
UPDATE podcast_tracking
SET status = 'failed',
    error_message = 'PERMANENT: Removed from Apple Podcasts directory'
WHERE apple_podcast_id = 'XXXXXXXXXX';
"
```
