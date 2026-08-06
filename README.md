# Colorado Current: Podcast Network

A tool that scrapes clean energy podcasts, extracts guest and host relationships per episode, and builds a network graph showing connections between people across the ecosystem. Built as a feature for [coloradocurrent.com](https://coloradocurrent.com).

---

## Architecture

```
scraper/        Python pipeline: pulls episodes from Apple Podcasts + credits from Podchaser
backend/        FastAPI server: serves graph data to the frontend
frontend/       React app: force-directed network graph visualization
```

Data flows in this order:
1. **Scraper** hits iTunes API → populates `episodes` table
2. **Podchaser client** hits Podchaser GraphQL API → populates `episode_host` (guest/host credits)
3. **FastAPI backend** queries the DB and serves `/api/host-connections`
4. **React frontend** fetches from the API and renders the network graph

---

## Prerequisites

Install these if you don't have them:

```bash
# PostgreSQL
brew install postgresql@15
brew services start postgresql@15

# Python 3.10+
python3 --version

# Node.js 18+
node --version
```

---

## 1. Database Setup

```bash
# Create the database
createdb podcast_db

# Run the schema (creates all tables and seeds 49 clean energy podcasts)
psql podcast_db < scraper/podcast-schema.sql

# Verify tables and seed data
psql podcast_db -c "\dt"
psql podcast_db -c "SELECT COUNT(*) FROM podcast_tracking;"
```

Expected output: 15 tables, 49 rows in `podcast_tracking`.

To reset the database from scratch at any point:
```bash
dropdb podcast_db && createdb podcast_db
psql podcast_db < scraper/podcast-schema.sql
```

---

## 2. Scraper Setup

```bash
cd scraper

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install psycopg2-binary requests beautifulsoup4 python-dotenv feedparser ratelimit
```

Create `scraper/.env`:
```
DB_HOST=localhost
DB_NAME=podcast_db
DB_USER=YOUR_MAC_USERNAME
DB_PASSWORD=
```

> Your Mac username is the output of `whoami` in the terminal.

Test the DB connection:
```bash
python3 -c "
import psycopg2
conn = psycopg2.connect(host='localhost', database='podcast_db', user='YOUR_MAC_USERNAME')
print('✅ DB connected')
conn.close()
"
```

### Test the scraper against one show

```bash
python3 -c "
from scraper import PodcastScraper
s = PodcastScraper('postgresql://localhost/podcast_db')
result = s.process_podcast('1593204897')  # Catalyst with Shayle Kann
print(result)
"
```

### Run the full scrape

```bash
python3 manager.py
```

This seeds all 49 shows into the tracking table and processes them in order. The Podchaser enrichment steps (guest/host credits) are commented out by default in `manager.py` — uncomment them after the initial episode scrape completes.

---

## 3. Backend Setup

```bash
cd backend

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

Create `backend/.env`:
```
DB_HOST=localhost
DB_NAME=podcast_db
DB_USER=YOUR_MAC_USERNAME
DB_PASSWORD=
```

Start the server:
```bash
uvicorn main:app --reload --port 8000
```

Test it:
```bash
curl http://localhost:8000/api/host-connections
```

Should return `[]` until the scraper has run and populated episode/host data.

---

## 4. Frontend Setup

```bash
cd frontend
npm install
npm start
```

Opens at [http://localhost:3000](http://localhost:3000). The graph will be empty until the scraper and Podchaser enrichment have run.

---

## Running Everything Together

You'll need three terminal tabs:

| Tab | Directory | Command |
|-----|-----------|---------|
| 1 | `backend/` | `source venv/bin/activate && uvicorn main:app --reload --port 8000` |
| 2 | `frontend/` | `npm start` |
| 3 | `scraper/` | `source venv/bin/activate && python3 manager.py` |

---

## Podchaser API

The Podchaser API key lives in `scraper/manager.py`. It's used for two enrichment passes after the initial Apple/RSS scrape:

1. **Find Podchaser IDs** for shows that only have an Apple ID
2. **Sync episode credits** (guest and host names per episode) — this is what builds the network graph

Both passes are commented out in `manager.py` by default. Uncomment the relevant lines once the initial episode scrape is complete:

```python
# Uncomment to find missing Podchaser IDs:
# client.find_podcast_podchaser_ids(batch_size=10)

# Uncomment to sync episode host/guest credits:
# client.sync_episode_credits(batch_size=10)
```

---

## Podcast List

49 clean energy podcasts are seeded in `scraper/podcast-schema.sql`. The master tracking spreadsheet (with Apple IDs, Podchaser IDs, hosts, focus areas, and audience notes) lives in Google Sheets and is also available as `clean_energy_podcasts.xlsx`.

38 shows have both Apple and Podchaser IDs confirmed. 11 shows have Apple IDs only and will need Podchaser IDs looked up via the enrichment pass.

---

## Branch

Active development is on the `clean-energy-podcasts` branch:

```bash
git checkout clean-energy-podcasts
```
