# TO RUN LOCALLY
# switch to bash in the /podcast-network/backend/ folder
# source ~/.bash_profile
# python3 -m uvicorn main:app --reload

import re
import secrets

from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
from collections import OrderedDict
from datetime import date
import urllib.parse
import httpx
from urllib.parse import urlparse
from dotenv import load_dotenv

from description_cleaner import (
    clean_description, extract_labelled_credits, name_in_text, first_name_belongs_to_other,
    coarse_source, strip_html,
)
from role_selection import pick_current_role, format_for_display
from org_names import normalize_org_name, ambiguous_spelling
import org_stats
from org_suggestions import refresh_suggestions

load_dotenv()

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

# For dispatching the scrape workflow on demand ("Scrape Now" in Show Admin).
# GITHUB_REPO is "owner/repo"; the token needs Actions: write on that repo.
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")
SCRAPE_WORKFLOW_FILE = "scrape.yml"


def verify_admin(x_admin_password: str = Header(default=None)):
    """Dependency guarding every /api/admin/* route with a shared password.

    ADMIN_PASSWORD must be set in the environment — if it's missing, admin
    routes are refused entirely rather than silently left open.
    """
    if not ADMIN_PASSWORD:
        raise HTTPException(status_code=503, detail="Admin auth is not configured on the server")
    if not x_admin_password or not secrets.compare_digest(x_admin_password, ADMIN_PASSWORD):
        raise HTTPException(status_code=401, detail="Invalid or missing admin password")

# Hosts /api/proxy/image is allowed to fetch from — every domain profile
# images actually come from (Apple's CDN, Twitter avatars via unavatar.io,
# Bluesky avatars). Anything else is rejected to prevent the endpoint being
# used as an open proxy / SSRF vector.
# wikimedia.org: people's photos from Wikidata (commons.wikimedia.org's
# Special:FilePath redirects to upload.wikimedia.org) — scraper/enrich.py.
ALLOWED_IMAGE_HOST_SUFFIXES = ('mzstatic.com', 'unavatar.io', 'bsky.app', 'wikimedia.org')

# Whether /api/host-connections returns {nodes, links} (about a quarter of the
# size) or the legacy row-per-edge array. On since the deployed frontend was
# confirmed to read both shapes; set to "false" to fall back without a deploy.
GRAPH_PAYLOAD_NORMALISED = os.getenv("GRAPH_PAYLOAD_NORMALISED", "true").lower() == "true"


def is_allowed_image_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if parsed.scheme not in ('http', 'https'):
        return False
    host = (parsed.hostname or '').lower()
    return any(host == suffix or host.endswith('.' + suffix) for suffix in ALLOWED_IMAGE_HOST_SUFFIXES)


# Where a name splits into first/last is a guess, and different sources guess
# differently — Apple gave us "Amy Myers" + "Jaffe" while this admin UI stored
# "Amy" + "Myers Jaffe", producing two records for one person. Match on the
# whole name instead, ignoring case, spacing and punctuation.
NORMALIZED_NAME_SQL = "lower(regexp_replace(first_name || ' ' || last_name, '[^A-Za-z]', '', 'g'))"


def normalize_full_name(name: str) -> str:
    return re.sub(r'[^A-Za-z]', '', name or '').lower()


def find_host_by_full_name(cur, name: str):
    """Return the existing host_id for this person, however their name is written.

    Checks recorded aliases as well as the canonical name, so a person merged
    under one spelling is still found by the other — otherwise the next episode
    using the old spelling would create the duplicate all over again.
    """
    key = normalize_full_name(name)
    cur.execute(
        f"SELECT host_id FROM hosts WHERE {NORMALIZED_NAME_SQL} = %s ORDER BY host_id LIMIT 1",
        (key,)
    )
    row = cur.fetchone()
    if row:
        return row['host_id']

    cur.execute("SELECT host_id FROM host_aliases WHERE normalized_name = %s", (key,))
    row = cur.fetchone()
    return row['host_id'] if row else None


def _find_candidate_episodes(cur, name: str, exclude_episode_id: int = None) -> list:
    """Return raw (episode_id, podcast_id, podcast_title, title, description)
    rows whose RAW title or description contains `name` as a case-insensitive
    substring, excluding one episode if given.

    A fast pre-filter, not the final answer: clean_description() only ever
    removes text, so anything that matches after cleaning was already present
    in the raw text, but the reverse doesn't hold. This still avoids pulling
    all ~10k episodes' text over the wire — some descriptions run past
    100,000 characters (Volts' transcripts) — and typically only a handful of
    episodes contain any given name, so re-checking just those against real
    cleaning in Python (_verify_and_source) is cheap.
    """
    cur.execute("""
        SELECT e.episode_id, e.podcast_id, p.title AS podcast_title,
               e.title, e.description
        FROM episodes e
        JOIN podcasts p ON p.podcast_id = e.podcast_id
        WHERE (%(exclude_id)s IS NULL OR e.episode_id != %(exclude_id)s)
          AND (position(%(name_lower)s IN lower(e.title)) > 0
               OR position(%(name_lower)s IN lower(COALESCE(e.description, ''))) > 0)
    """, {'name_lower': name.lower(), 'exclude_id': exclude_episode_id})
    return cur.fetchall()


def _verify_and_source(rows: list, name: str) -> list:
    """Re-check each raw-text candidate against clean_description() before
    trusting it as a real appearance.

    Without this, approving a name that happens to appear inside a sponsor
    blurb or cross-show promo on some unrelated episode (see
    description_cleaner.py's docstring for real incidents this guards
    against — Political Climate, Shift Key's "Shocked" cross-promo, RCC's
    rotating past-episode references) would credit that episode too. Title
    matches don't need cleaning — the scanner never cleans titles either.

    The general text-presence check stays capped at DESC_SCAN_MAX_CHARS
    (clean_description()'s default) — lifting the cap for exact matching
    sounds safe but isn't: it would resurrect the original Joe Manchin/Volts
    bug clean_description.py's docstring for DESC_SCAN_MAX_CHARS describes,
    since a name buried in a huge transcript is an exact match too. Labelled
    credits ("Guest:", "Connect with [Name]") are checked separately against
    the FULL text instead, because those are precise statements rather than
    mentions — see extract_labelled_credits()'s docstring. Real incident:
    Kulsoom Khan, an already-known host, was named only in a "Connect with"
    footer past the cap, so "rescan" from her Edit Person page found nothing
    even though her name was plainly in the description.
    """
    name_lower = name.lower()
    verified = []
    for row in rows:
        if name_in_text(name, row['title'] or ''):
            verified.append({**row, 'source': 'parsed_title'})
            continue
        if name_in_text(name, clean_description(row['description'] or '')):
            verified.append({**row, 'source': 'parsed_desc'})
            continue
        full_desc = clean_description(row['description'] or '', max_chars=None)
        labelled_names = {n.lower() for n, _ in extract_labelled_credits(full_desc)}
        if name_lower in labelled_names:
            verified.append({**row, 'source': 'parsed_desc'})
    return verified


def link_matching_episodes(cur, name: str, host_id: int, exclude_episode_id: int = None) -> list:
    """Link host_id as a Guest on every OTHER episode that genuinely names
    them, and return the podcast title + matched source for each one
    actually inserted.
    """
    matches = _verify_and_source(_find_candidate_episodes(cur, name, exclude_episode_id), name)
    if not matches:
        return []

    cur.execute("""
        WITH inserted AS (
            INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
            SELECT v.episode_id, %(host_id)s, true, 'Guest', v.source
            FROM unnest(%(episode_ids)s::int[], %(sources)s::text[]) AS v(episode_id, source)
            ON CONFLICT (episode_id, host_id) DO NOTHING
            RETURNING episode_id
        )
        SELECT episode_id FROM inserted
    """, {
        'host_id': host_id,
        'episode_ids': [m['episode_id'] for m in matches],
        'sources': [m['source'] for m in matches],
    })
    inserted_ids = {r['episode_id'] for r in cur.fetchall()}
    return [{'podcast_title': m['podcast_title'], 'source': m['source']}
            for m in matches if m['episode_id'] in inserted_ids]


def link_matching_episodes_with_role(cur, name: str, host_id: int, exclude_episode_id: int = None) -> list:
    """Same as link_matching_episodes(), but credits "Host" instead of "Guest"
    on any show this person is already a registered host_podcast member of —
    used when creating/editing/rescanning a person directly, as opposed to
    approving a suggestion (which is always a Guest credit on the source show).
    """
    matches = _verify_and_source(_find_candidate_episodes(cur, name, exclude_episode_id), name)
    if not matches:
        return []

    cur.execute("SELECT podcast_id FROM host_podcast WHERE host_id = %s", (host_id,))
    show_host_podcast_ids = {r['podcast_id'] for r in cur.fetchall()}

    episode_ids, is_guests, roles, sources = [], [], [], []
    for m in matches:
        is_show_host = m['podcast_id'] in show_host_podcast_ids
        episode_ids.append(m['episode_id'])
        is_guests.append(not is_show_host)
        roles.append('Host' if is_show_host else 'Guest')
        sources.append(m['source'])

    cur.execute("""
        WITH inserted AS (
            INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
            SELECT v.episode_id, %(host_id)s, v.is_guest, v.role, v.source
            FROM unnest(%(episode_ids)s::int[], %(is_guests)s::bool[], %(roles)s::text[], %(sources)s::text[])
                AS v(episode_id, is_guest, role, source)
            ON CONFLICT (episode_id, host_id) DO NOTHING
            RETURNING episode_id
        )
        SELECT episode_id FROM inserted
    """, {
        'host_id': host_id, 'episode_ids': episode_ids,
        'is_guests': is_guests, 'roles': roles, 'sources': sources,
    })
    inserted_ids = {r['episode_id'] for r in cur.fetchall()}
    return [{'podcast_title': m['podcast_title'], 'source': m['source']}
            for m in matches if m['episode_id'] in inserted_ids]


def link_matching_episodes_by_first_name(cur, host_id: int, first_name: str,
                                          last_name: str, exclude_episode_id: int = None) -> list:
    """Credit host_id as Host on any OTHER episode of a show they're already
    a registered host_podcast member of, found by first name alone — the
    same matching episode_name_scanner.py's run() does via
    show_host_first_names(), which create/rescan/rename didn't otherwise
    reach (those only ever checked full names).

    Real incident: John Failla, created and registered as a host of Smart
    Energy Voices, was found on the 44 episodes that name him in full but
    not on ones that only say "John speaks with ..." — "rescan" from his
    Edit Person page found nothing on those, even though run() (the
    scheduled scanner) already matches this shape correctly.
    """
    if not first_name or len(first_name) < 3:
        return []

    # Only podcasts where this first name is unique among the show's OTHER
    # registered hosts — same rule show_host_first_names() applies: there's
    # no way to tell which host a bare first name means otherwise.
    cur.execute("""
        SELECT hp.podcast_id
        FROM host_podcast hp
        WHERE hp.host_id = %(host_id)s
          AND NOT EXISTS (
              SELECT 1 FROM host_podcast hp2
              JOIN hosts h2 ON h2.host_id = hp2.host_id
              WHERE hp2.podcast_id = hp.podcast_id
                AND hp2.host_id != %(host_id)s
                AND lower(h2.first_name) = lower(%(first_name)s)
          )
    """, {'host_id': host_id, 'first_name': first_name})
    podcast_ids = [r['podcast_id'] for r in cur.fetchall()]
    if not podcast_ids:
        return []

    cur.execute("""
        SELECT e.episode_id, e.podcast_id, p.title AS podcast_title, e.title, e.description
        FROM episodes e
        JOIN podcasts p ON p.podcast_id = e.podcast_id
        WHERE e.podcast_id = ANY(%(podcast_ids)s)
          AND (%(exclude_id)s IS NULL OR e.episode_id != %(exclude_id)s)
          AND (position(lower(%(first_name)s) IN lower(e.title)) > 0
               OR position(lower(%(first_name)s) IN lower(COALESCE(e.description, ''))) > 0)
    """, {'podcast_ids': podcast_ids, 'first_name': first_name, 'exclude_id': exclude_episode_id})
    candidates = cur.fetchall()

    matches = []
    for row in candidates:
        haystack = (row['title'] or '') + '\n' + clean_description(row['description'] or '')
        if not name_in_text(first_name, haystack):
            continue
        if first_name_belongs_to_other(first_name, last_name or '', haystack):
            continue
        matches.append(row)

    if not matches:
        return []

    cur.execute("""
        WITH inserted AS (
            INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
            SELECT v.episode_id, %(host_id)s, false, 'Host', 'host_first_name'
            FROM unnest(%(episode_ids)s::int[]) AS v(episode_id)
            ON CONFLICT (episode_id, host_id) DO NOTHING
            RETURNING episode_id
        )
        SELECT episode_id FROM inserted
    """, {'host_id': host_id, 'episode_ids': [m['episode_id'] for m in matches]})
    inserted_ids = {r['episode_id'] for r in cur.fetchall()}
    return [{'podcast_title': m['podcast_title'], 'source': 'host_first_name'}
            for m in matches if m['episode_id'] in inserted_ids]


app = FastAPI()


class AliasRequest(BaseModel):
    alias_name: Optional[str] = None


class RolePinRequest(BaseModel):
    title: Optional[str] = None
    company: Optional[str] = None


class CompanyUpdateRequest(BaseModel):
    name: Optional[str] = None
    org_type: Optional[str] = None
    website_domain: Optional[str] = None
    parent_org_id: Optional[int] = None
    not_an_org: Optional[bool] = None


class NotSameOrgRequest(BaseModel):
    org_a: int
    org_b: int


class DismissPairRequest(BaseModel):
    host_id_a: int
    host_id_b: int


class NameOverrideRequest(BaseModel):
    name: Optional[str] = None

DEFAULT_ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:3001",
    "https://coloradocurrent.com",
    "https://www.coloradocurrent.com",
]
extra_origins = [o.strip() for o in os.getenv("ADDITIONAL_ALLOWED_ORIGINS", "").split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=DEFAULT_ALLOWED_ORIGINS + extra_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "podcast_db"),
        user=os.getenv("DB_USER", ""),
        password=os.getenv("DB_PASSWORD", ""),
        cursor_factory=RealDictCursor
    )

@app.get("/api/host-connections")
async def get_host_connections():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            WITH
            -- What each person actually did on each show, from the per-episode
            -- credits rather than whether they are registered as a show host.
            -- host_podcast only covers people someone has explicitly registered
            -- — 110 of 1,952 here — so reading role from it left everyone else
            -- with none, and the frontend's `|| 'Host'` fallback then labelled
            -- the whole graph "Host". Ticking Guest returned nothing.
            person_show_role AS (
                SELECT eh.host_id, e.podcast_id,
                       CASE WHEN bool_or(NOT eh.is_guest) THEN 'Host' ELSE 'Guest' END AS role
                FROM episode_host eh
                JOIN episodes e ON e.episode_id = eh.episode_id
                GROUP BY eh.host_id, e.podcast_id
            ),
            host_connections AS (
                SELECT
                    h1.host_id as source_id,
                    h1.first_name || ' ' || h1.last_name as source_name,
                    h1.profile_image_url as source_image,
                    psr1.role as source_role,
                    c1.name as source_channel,
                    g1.name as source_genre,
                    h2.host_id as target_id,
                    h2.first_name || ' ' || h2.last_name as target_name,
                    h2.profile_image_url as target_image,
                    psr2.role as target_role,
                    c1.name as target_channel,
                    g1.name as target_genre,
                    p.title as podcast_title,
                    COUNT(DISTINCT e.episode_id) as episodes_together
                FROM hosts h1
                JOIN episode_host eh1 ON h1.host_id = eh1.host_id
                JOIN episodes e ON eh1.episode_id = e.episode_id
                JOIN podcasts p ON e.podcast_id = p.podcast_id
                LEFT JOIN channels c1 ON p.channel_id = c1.channel_id
                LEFT JOIN podcast_genres pg ON p.podcast_id = pg.podcast_id AND pg.is_primary = true
                LEFT JOIN genres g1 ON pg.genre_id = g1.genre_id
                LEFT JOIN person_show_role psr1 ON psr1.host_id = h1.host_id AND psr1.podcast_id = p.podcast_id
                JOIN episode_host eh2 ON e.episode_id = eh2.episode_id
                JOIN hosts h2 ON eh2.host_id = h2.host_id
                LEFT JOIN person_show_role psr2 ON psr2.host_id = h2.host_id AND psr2.podcast_id = p.podcast_id
                WHERE h1.host_id < h2.host_id
                GROUP BY
                    h1.host_id, h1.first_name, h1.last_name, h1.profile_image_url,
                    psr1.role, c1.name, g1.name,
                    h2.host_id, h2.first_name, h2.last_name, h2.profile_image_url,
                    psr2.role, p.title
            )
            SELECT *
            FROM host_connections
            ORDER BY episodes_together DESC;
        """)

        results = cur.fetchall()
        cur.close()
        conn.close()

        # Serve the row-per-edge array until every client can read {nodes,
        # links}. The browser bundle and this service deploy independently and
        # the backend lands first, so changing the shape here first left the
        # old frontend calling .forEach on an object and the graph empty. Flip
        # this once the deployed frontend is known to handle both.
        if not GRAPH_PAYLOAD_NORMALISED:
            return results

        # Return nodes and links separately rather than a row per edge. Every
        # row repeated both people's name, image, channel and genre in full, so
        # the same 1,952 people were described 5,133 times over: 2.6MB where
        # 0.55MB carries the same information.
        nodes = {}
        links = []
        for row in results:
            for side in ("source", "target"):
                node_id = row[f"{side}_id"]
                node = nodes.get(node_id)
                if node is None:
                    node = nodes[node_id] = {
                        "id": node_id,
                        "name": row[f"{side}_name"],
                        "image": row[f"{side}_image"],
                        "role": row[f"{side}_role"] or "Guest",
                        "channel": row[f"{side}_channel"],
                        "genre": row[f"{side}_genre"],
                    }
                # Someone who presents one show and guests on another is a host:
                # the alternative is letting row order decide.
                if row[f"{side}_role"] == "Host":
                    node["role"] = "Host"
                node.setdefault("channel", row[f"{side}_channel"])
                node.setdefault("genre", row[f"{side}_genre"])

            links.append({
                "source": row["source_id"],
                "target": row["target_id"],
                "podcast": row["podcast_title"],
                "value": row["episodes_together"],
            })

        return {"nodes": list(nodes.values()), "links": links}

    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/last-updated")
async def get_last_updated():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT last_run_at FROM scrape_status WHERE id = 1;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        return {"last_run_at": row["last_run_at"] if row else None}
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/people")
async def get_people():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                h.host_id,
                h.first_name || ' ' || h.last_name AS name,
                h.profile_image_url,
                h.linkedin_url,
                COUNT(DISTINCT eh.episode_id)                                        AS total_appearances,
                COUNT(DISTINCT CASE WHEN eh.is_guest = false THEN eh.episode_id END) AS as_host,
                COUNT(DISTINCT CASE WHEN eh.is_guest = true  THEN eh.episode_id END) AS as_guest,
                COUNT(DISTINCT e.podcast_id)                                         AS podcast_count,
                ARRAY_AGG(DISTINCT p.title ORDER BY p.title)                        AS podcasts
            FROM hosts h
            JOIN episode_host eh ON h.host_id = eh.host_id
            JOIN episodes e      ON eh.episode_id = e.episode_id
            JOIN podcasts p      ON e.podcast_id = p.podcast_id
            GROUP BY h.host_id, h.first_name, h.last_name, h.profile_image_url, h.linkedin_url
            ORDER BY total_appearances DESC;
        """)

        results = cur.fetchall()
        cur.close()
        conn.close()
        return results

    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/podcasts")
async def get_podcasts():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                p.podcast_id,
                p.title,
                p.focus_area,
                p.target_audience,
                p.cover_art_url,
                p.website_url,
                COUNT(DISTINCT e.episode_id)   AS episode_count,
                COUNT(DISTINCT eh.host_id)     AS person_count
            FROM podcasts p
            LEFT JOIN episodes e      ON e.podcast_id = p.podcast_id
            LEFT JOIN episode_host eh ON eh.episode_id = e.episode_id
            GROUP BY p.podcast_id, p.title, p.focus_area, p.target_audience,
                     p.cover_art_url, p.website_url
            ORDER BY person_count DESC, episode_count DESC;
        """)

        results = cur.fetchall()
        cur.close()
        conn.close()
        return results

    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------------------------------------------
# Stats from guests' roles and organisations (backend/org_stats.py)
# ------------------------------------------------------------------

def _org_stat(fn, *args, **kw):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        return fn(cur, *args, **kw)
    finally:
        cur.close()
        conn.close()


@app.get("/api/stats/show-guest-mix")
async def stats_show_guest_mix():
    """Who each show books: its guests by type of organisation."""
    return _org_stat(org_stats.show_guest_mix)


@app.get("/api/stats/revolving-door")
async def stats_revolving_door():
    """Guests whose former role was at one type of organisation and whose
    current role is at another."""
    return _org_stat(org_stats.revolving_door)


@app.get("/api/stats/top-organizations")
async def stats_top_organizations(by: str = "guests", exclude_in_house: bool = True,
                                  org_type: str = "", limit: int = 15, offset: int = 0):
    """Most-booked organisations, sub-organisations counted under their
    parent; optionally one type of organisation, a page at a time."""
    return _org_stat(org_stats.top_organisations, by="shows" if by == "shows" else "guests",
                     exclude_in_house=exclude_in_house,
                     org_type=org_type if org_type in org_stats.ORG_TYPES + ('none',) else None,
                     limit=max(1, min(limit, 100)), offset=max(0, offset))


@app.get("/api/stats/guest-mix-by-year")
async def stats_guest_mix_by_year():
    """Share of guest appearances by organisation type, per year."""
    return _org_stat(org_stats.guest_mix_by_year)


@app.get("/api/stats/guest-roles")
async def stats_guest_roles():
    """Guests by the kind of role they hold, overall and per show."""
    return _org_stat(org_stats.guest_roles)


@app.get("/api/stats/guest-appearances")
async def get_guest_appearance_stats(limit: int = 200):
    """Sorted list of people by GUEST episode appearance count — a
    permanent show host would otherwise dominate this (they're credited
    on every episode of their own show), which says nothing about how
    sought-after someone is across the ecosystem the way guest spots do."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT h.host_id,
                   h.first_name || ' ' || h.last_name AS name,
                   h.profile_image_url,
                   COUNT(DISTINCT eh.episode_id) AS appearances
            FROM hosts h
            JOIN episode_host eh ON eh.host_id = h.host_id
            WHERE eh.is_guest = true
            GROUP BY h.host_id, h.first_name, h.last_name, h.profile_image_url
            ORDER BY appearances DESC
            LIMIT %s;
        """, (limit,))
        items = cur.fetchall()

        cur.execute("""
            SELECT COUNT(DISTINCT h.host_id) AS total_people
            FROM hosts h JOIN episode_host eh ON eh.host_id = h.host_id
            WHERE eh.is_guest = true;
        """)
        total_people = cur.fetchone()["total_people"]

        cur.close()
        conn.close()
        return {"items": items, "total_people": total_people}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats/guest-reach")
async def get_guest_reach():
    """Each guest's appearances against the number of distinct shows.

    Separates two things a single count hides: people who turn up once on many
    different shows, and people who return repeatedly to the same one. Guests
    with a single appearance are the large majority and all sit on the same
    point, so they are counted rather than listed.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT h.host_id,
                   h.first_name || ' ' || h.last_name AS name,
                   COUNT(*)                            AS appearances,
                   COUNT(DISTINCT e.podcast_id)        AS shows
            FROM episode_host eh
            JOIN hosts h    ON h.host_id = eh.host_id
            JOIN episodes e ON e.episode_id = eh.episode_id
            WHERE eh.is_guest
            GROUP BY h.host_id, h.first_name, h.last_name
            HAVING COUNT(*) >= 2
            ORDER BY COUNT(DISTINCT e.podcast_id) DESC, COUNT(*) DESC
        """)
        guests = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*) AS single_appearance FROM (
                SELECT eh.host_id FROM episode_host eh
                WHERE eh.is_guest GROUP BY eh.host_id HAVING COUNT(*) = 1
            ) one_off
        """)
        single = cur.fetchone()["single_appearance"]

        cur.close()
        conn.close()
        return {"guests": guests, "single_appearance_guests": single}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/show-connections")
async def get_show_connections():
    """The network one level up: shows as nodes, shared people as edges.

    The person graph buries the communities it contains — at 1,952 nodes you
    cannot see that Factor This, SunCast and Clean Power Hour form a solar
    trade cluster distinct from the Volts/Inevitable/Catalyst interview
    circuit. At 91 nodes that is the first thing visible.

    Each edge carries two weights. `value` is the raw count of people both
    shows have had on, which is intuitive but rewards size: Volts and
    Inevitable share 44 largely because each has had around 200 guests.
    `jaccard` divides that by the size of the combined guest pool, so two
    small shows sharing most of their guests outrank two large ones sharing a
    slice. Raw answers "who overlaps most", normalised answers "whose overlap
    is surprising".
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            WITH person_show AS (
                SELECT DISTINCT eh.host_id, e.podcast_id
                FROM episode_host eh JOIN episodes e USING (episode_id)
            ),
            show_size AS (
                SELECT podcast_id, COUNT(*) AS people FROM person_show GROUP BY 1
            ),
            shared AS (
                SELECT a.podcast_id AS s1, b.podcast_id AS s2, COUNT(*) AS n
                FROM person_show a
                JOIN person_show b
                  ON a.host_id = b.host_id AND a.podcast_id < b.podcast_id
                GROUP BY 1, 2
            )
            SELECT sh.s1 AS source, sh.s2 AS target, sh.n AS value,
                   ROUND(sh.n::numeric / (z1.people + z2.people - sh.n), 4) AS jaccard
            FROM shared sh
            JOIN show_size z1 ON z1.podcast_id = sh.s1
            JOIN show_size z2 ON z2.podcast_id = sh.s2
            ORDER BY sh.n DESC
        """)
        links = cur.fetchall()

        cur.execute("""
            SELECT p.podcast_id AS id, p.title AS name, p.cover_art_url AS image,
                   COUNT(DISTINCT e.episode_id) AS episodes,
                   COUNT(DISTINCT eh.host_id)   AS people,
                   MAX(e.published_date)        AS latest_episode
            FROM podcasts p
            LEFT JOIN episodes e     ON e.podcast_id = p.podcast_id
            LEFT JOIN episode_host eh ON eh.episode_id = e.episode_id
            GROUP BY p.podcast_id, p.title, p.cover_art_url
            ORDER BY p.title
        """)
        nodes = cur.fetchall()
        cur.close()
        conn.close()
        return {"nodes": nodes, "links": links}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats/guest-cohorts")
async def get_guest_cohorts():
    """Each year's guest roster, split by the year those guests first appeared.

    The headline is that first-timers fall from 94% of the roster to 40%, but
    as a single line that hides what is actually happening. Around a quarter of
    each year's intake comes back the following year, and those who do then
    stay: the 2019 cohort has put 61 to 82 people on air every year since,
    seven years running. The network is not so much consolidating as accreting
    a stable core, one layer at a time.

    Anyone whose first appearance predates 2019 is folded into the 2019 band —
    there are few of them and they would otherwise need six more bands of
    almost nothing.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            WITH firsts AS (
                SELECT eh.host_id,
                       MIN(EXTRACT(YEAR FROM e.published_date))::int AS debut
                FROM episode_host eh JOIN episodes e USING (episode_id)
                WHERE eh.is_guest AND e.published_date IS NOT NULL
                GROUP BY 1
            ),
            yearly AS (
                SELECT DISTINCT EXTRACT(YEAR FROM e.published_date)::int AS yr,
                       eh.host_id
                FROM episode_host eh JOIN episodes e USING (episode_id)
                WHERE eh.is_guest AND e.published_date >= DATE '2019-01-01'
            )
            SELECT y.yr, GREATEST(f.debut, 2019) AS cohort, COUNT(*) AS n
            FROM yearly y JOIN firsts f USING (host_id)
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        rows = cur.fetchall()
        cur.execute("SELECT EXTRACT(YEAR FROM MAX(published_date))::int AS y FROM episodes")
        last_year = cur.fetchone()["y"]
        cur.close()
        conn.close()

        years = list(range(2019, last_year + 1))
        index = {y: i for i, y in enumerate(years)}
        # One row per cohort, aligned to the same year axis, so a band is just
        # an array the chart can stack without any lookup.
        cohorts = [
            {"cohort": c, "counts": [0] * len(years)}
            for c in years
        ]
        by_cohort = {c["cohort"]: c for c in cohorts}
        for r in rows:
            band = by_cohort.get(r["cohort"])
            if band is not None and r["yr"] in index:
                band["counts"][index[r["yr"]]] = r["n"]

        totals = [sum(c["counts"][i] for c in cohorts) for i in range(len(years))]
        return {
            "years": years,
            "cohorts": cohorts,
            "totals": totals,
            "partial_year": last_year,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats/guest-momentum")
async def get_guest_momentum():
    """Appearances per year for the guests most present recently.

    A ranking chart was the obvious shape and does not work here: 44 people
    reach some year's top eight and only 10 manage it twice, so the lines
    would be mostly disconnected dots. The churn is the finding, and a small
    chart each shows it — a flat line then a cliff is a new arrival, a
    mountain now descending is someone fading, and both are legible at a
    glance where sixteen crossing lines would not be.

    Ordered by appearances in the last 18 months, so the grid is "who is
    around now" and the shape says whether that is new. Sorting by growth
    rate instead would put anyone going from one appearance to three at the
    top, which is noise at these counts.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            WITH app AS (
                SELECT eh.host_id,
                       EXTRACT(YEAR FROM e.published_date)::int AS yr,
                       e.published_date,
                       e.podcast_id
                FROM episode_host eh
                JOIN episodes e USING (episode_id)
                WHERE eh.is_guest
                  AND e.published_date IS NOT NULL
                  AND e.published_date >= DATE '2019-01-01'
            ),
            scored AS (
                SELECT host_id,
                       COUNT(*) FILTER (
                         WHERE published_date >= (CURRENT_DATE - INTERVAL '18 months')
                       ) AS recent,
                       COUNT(*) FILTER (
                         WHERE published_date <  (CURRENT_DATE - INTERVAL '18 months')
                       ) AS earlier
                FROM app
                GROUP BY 1
                -- Four shows minimum. Without it the top of the list is people
                -- with a dozen appearances on one podcast — a recurring
                -- contributor or a miscredited co-host, not a guest moving
                -- through the network, which is what "trending" should mean.
                HAVING COUNT(DISTINCT podcast_id) >= 4
            ),
            top AS (
                SELECT host_id, recent, earlier FROM scored
                ORDER BY recent DESC, earlier ASC LIMIT 16
            )
            SELECT t.host_id,
                   h.first_name || ' ' || h.last_name AS name,
                   t.recent, t.earlier,
                   a.yr, COUNT(*) AS n
            FROM top t
            JOIN hosts h USING (host_id)
            JOIN app a  USING (host_id)
            GROUP BY t.host_id, h.first_name, h.last_name, t.recent, t.earlier, a.yr
            ORDER BY t.recent DESC, t.earlier ASC, a.yr
        """)
        rows = cur.fetchall()
        cur.execute("SELECT EXTRACT(YEAR FROM MAX(published_date))::int AS y FROM episodes")
        last_year = cur.fetchone()["y"]
        cur.close()
        conn.close()

        years = list(range(2019, last_year + 1))
        index = {y: i for i, y in enumerate(years)}
        guests, order = {}, []
        for r in rows:
            g = guests.get(r["host_id"])
            if g is None:
                g = guests[r["host_id"]] = {
                    "host_id": r["host_id"], "name": r["name"],
                    "recent": r["recent"], "earlier": r["earlier"],
                    "counts": [0] * len(years),
                }
                order.append(g)
            if r["yr"] in index:
                g["counts"][index[r["yr"]]] = r["n"]

        return {
            "years": years,
            # The current year is only partly published, so its point is low
            # for everyone. Said plainly rather than projected — a projection
            # would be inventing episodes that do not exist.
            "partial_year": last_year,
            "guests": order,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats/bridges")
async def get_bridges():
    """The two routes to reaching most of the network.

    Reach — shows you can get to in one step, through anyone you shared an
    episode with — turns out to barely vary at the top: everyone here lands
    between about 57 and 69 of 88. It saturates, because the guest pool
    circulates among the same shows. David Roberts has sat with twice as many
    people as Cody Simms for 12% more reach.

    What does vary is how they got there. Jigar Shah appears on 32 shows and
    reaches 59. Cody Simms hosts one show, has never appeared anywhere else,
    and reaches 59 because 103 guests came to him. Ordering by own_shows rather
    than by reach puts those two routes at opposite ends, which is the thing
    worth seeing — a ranking by reach would just be a flat list.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            WITH person_show AS (
                SELECT DISTINCT eh.host_id, e.podcast_id
                FROM episode_host eh JOIN episodes e USING (episode_id)
            ),
            pair AS (
                SELECT DISTINCT a.host_id AS me, b.host_id AS them
                FROM episode_host a
                JOIN episode_host b
                  ON a.episode_id = b.episode_id AND a.host_id <> b.host_id
            ),
            own AS (
                SELECT host_id, COUNT(*) AS own_shows FROM person_show GROUP BY 1
            ),
            reach AS (
                SELECT p.me AS host_id, COUNT(DISTINCT ps.podcast_id) AS reached
                FROM pair p JOIN person_show ps ON ps.host_id = p.them
                GROUP BY 1
            ),
            partners AS (
                SELECT me AS host_id, COUNT(*) AS people_sat_with FROM pair GROUP BY 1
            ),
            top AS (
                SELECT h.host_id,
                       h.first_name || ' ' || h.last_name AS name,
                       r.reached,
                       o.own_shows,
                       GREATEST(r.reached - o.own_shows, 0) AS via_others,
                       pt.people_sat_with
                FROM reach r
                JOIN own      o  USING (host_id)
                JOIN partners pt USING (host_id)
                JOIN hosts    h  USING (host_id)
                ORDER BY r.reached DESC
                LIMIT 20
            )
            SELECT * FROM top ORDER BY own_shows DESC, reached DESC
        """)
        people = cur.fetchall()
        cur.execute("SELECT COUNT(*) AS n FROM podcasts")
        total_shows = cur.fetchone()["n"]
        cur.close()
        conn.close()
        return {"people": people, "total_shows": total_shows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats/show-overlap")
async def get_show_overlap_stats(top_n: int = 25):
    """Pairwise shared-guest counts among the top_n most-connected shows —
    the data behind the show overlap matrix on the public Stats page."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT p.podcast_id, p.title, p.cover_art_url,
                   COUNT(DISTINCT eh.host_id) AS people_count
            FROM podcasts p
            JOIN episodes e ON e.podcast_id = p.podcast_id
            JOIN episode_host eh ON eh.episode_id = e.episode_id
            GROUP BY p.podcast_id, p.title, p.cover_art_url
            ORDER BY people_count DESC
            LIMIT %s;
        """, (top_n,))
        shows = cur.fetchall()
        show_ids = [s["podcast_id"] for s in shows]

        if not show_ids:
            cur.close()
            conn.close()
            return {"shows": [], "pairs": []}

        cur.execute("""
            SELECT a.podcast_id AS podcast_a, b.podcast_id AS podcast_b,
                   COUNT(DISTINCT a.host_id) AS shared
            FROM (
                SELECT DISTINCT eh.host_id, e.podcast_id
                FROM episode_host eh JOIN episodes e ON eh.episode_id = e.episode_id
                WHERE e.podcast_id = ANY(%(ids)s)
            ) a
            JOIN (
                SELECT DISTINCT eh.host_id, e.podcast_id
                FROM episode_host eh JOIN episodes e ON eh.episode_id = e.episode_id
                WHERE e.podcast_id = ANY(%(ids)s)
            ) b ON a.host_id = b.host_id AND a.podcast_id < b.podcast_id
            GROUP BY a.podcast_id, b.podcast_id;
        """, {"ids": show_ids})
        pairs = cur.fetchall()

        cur.close()
        conn.close()
        return {"shows": shows, "pairs": pairs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats/show-timeline")
async def get_show_timeline_stats():
    """Per-show publishing history for the timeline on the Stats page.

    Returns a monthly count per show rather than a list of every episode
    date. Drawing one mark per episode does not survive the density: across
    600px of chart, 34 of the 91 shows put down more than a dot every three
    pixels and merge into a solid bar, and POLITICO Energy manages 2.6
    episodes per pixel. The cadence the chart exists to show is exactly what
    that overplotting destroys.

    Counts are aligned to one shared month axis so every show's array indexes
    the same way, which keeps the payload small and the rendering trivial.

    episode_dates is still included for one deploy cycle. Backend and frontend
    deploy separately and the backend lands first, so removing it in the same
    release that stops using it would break the live page in between.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT p.podcast_id, p.apple_podcast_id, p.title, p.cover_art_url,
                   MIN(e.published_date) AS earliest_episode_date,
                   MAX(e.published_date) AS latest_episode_date,
                   ARRAY_AGG(e.published_date ORDER BY e.published_date) AS episode_dates
            FROM podcasts p
            JOIN episodes e ON e.podcast_id = p.podcast_id
            WHERE e.published_date IS NOT NULL
            GROUP BY p.podcast_id, p.apple_podcast_id, p.title, p.cover_art_url
            ORDER BY earliest_episode_date ASC;
        """)
        shows = cur.fetchall()

        cur.execute("""
            SELECT p.podcast_id,
                   to_char(date_trunc('month', e.published_date), 'YYYY-MM') AS month,
                   COUNT(*) AS n
            FROM podcasts p
            JOIN episodes e ON e.podcast_id = p.podcast_id
            WHERE e.published_date IS NOT NULL
              -- Everything before 2019 is 111 episodes, 0.7% of the data, but
              -- 68 months of axis. Carrying it would spend 42% of the chart's
              -- width on a near-empty stretch and squeeze the years that matter.
              AND e.published_date >= DATE '2019-01-01'
            GROUP BY 1, 2
        """)
        by_month = cur.fetchall()
        cur.close()
        conn.close()

        months = sorted({r["month"] for r in by_month})
        if months:
            first_y, first_m = (int(x) for x in months[0].split("-"))
            last_y, last_m = (int(x) for x in months[-1].split("-"))
            axis = []
            y, m = first_y, first_m
            while (y, m) <= (last_y, last_m):
                axis.append(f"{y:04d}-{m:02d}")
                y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        else:
            axis = []
        index = {mo: i for i, mo in enumerate(axis)}

        counts = {s["podcast_id"]: [0] * len(axis) for s in shows}
        for r in by_month:
            row = counts.get(r["podcast_id"])
            if row is not None:
                row[index[r["month"]]] = r["n"]
        for s in shows:
            s["monthly"] = counts[s["podcast_id"]]

        return {"shows": shows, "months": axis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats/episode-calendar")
async def get_episode_calendar_stats():
    """Episode count per day across all shows — the data behind the
    calendar heatmap on the public Stats page."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT published_date, COUNT(*) AS count
            FROM episodes
            WHERE published_date IS NOT NULL
            GROUP BY published_date
            ORDER BY published_date;
        """)
        days = cur.fetchall()
        cur.close()
        conn.close()
        return {"days": days}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


# ==================================================================
# ADMIN ENDPOINTS — Suggestions queue
# ==================================================================

def _load_suggestion(cur, suggestion_id: int = None, apple_podcast_id: str = None):
    """Fetch one suggestion with its episode context.

    With no id this is the review queue: the oldest still-pending one,
    optionally scoped to one show via apple_podcast_id. With an id it is that
    specific suggestion whatever its status, so a link to one keeps working
    after it has been approved or rejected — the show filter doesn't apply
    there, since a direct link should always resolve.
    """
    if suggestion_id is None:
        where = "s.status = 'pending'"
        params = ()
        if apple_podcast_id:
            where += " AND p.apple_podcast_id = %s"
            params = (apple_podcast_id,)
    else:
        where, params = "s.suggestion_id = %s", (suggestion_id,)

    total_pending_where = "status = 'pending'"
    total_pending_params = ()
    if suggestion_id is None and apple_podcast_id:
        total_pending_where += (
            " AND episode_id IN (SELECT e2.episode_id FROM episodes e2 "
            "JOIN podcasts p2 ON e2.podcast_id = p2.podcast_id WHERE p2.apple_podcast_id = %s)"
        )
        total_pending_params = (apple_podcast_id,)

    cur.execute(f"""
            SELECT
                s.suggestion_id,
                s.candidate_name,
                s.first_name,
                s.last_name,
                s.source,
                s.matched_text,
                s.status,
                e.episode_id,
                e.title       AS episode_title,
                e.description AS episode_description,
                e.published_date,
                p.title       AS podcast_title,
                p.cover_art_url AS podcast_cover_art,
                p.apple_podcast_id,
                (SELECT COUNT(*) FROM suggestions WHERE {total_pending_where}) AS total_pending
            FROM suggestions s
            JOIN episodes e ON s.episode_id = e.episode_id
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE {where}
            ORDER BY s.created_at ASC
            LIMIT 1
    """, (*total_pending_params, *params))

    row = cur.fetchone()
    if not row:
        return None

    # Get existing credits for this episode
    cur.execute("""
        SELECT
            h.host_id,
            h.first_name || ' ' || h.last_name AS name,
            h.profile_image_url,
            eh.is_guest,
            eh.role,
            eh.data_source
        FROM episode_host eh
        JOIN hosts h ON h.host_id = eh.host_id
        WHERE eh.episode_id = %s
        ORDER BY eh.is_guest ASC, h.last_name ASC
    """, (row['episode_id'],))
    existing_credits = cur.fetchall()

    # Also get how many other episodes this person has been suggested for
    cur.execute("""
        SELECT COUNT(*) as other_suggestions
        FROM suggestions
        WHERE LOWER(candidate_name) = LOWER(%s)
          AND episode_id != %s
          AND status = 'pending'
    """, (row['candidate_name'], row['episode_id']))
    other_count = cur.fetchone()['other_suggestions']

    # Other names already queued for review on this same episode, so the
    # admin UI can distinguish "still pending" names from unhandled ones
    cur.execute("""
        SELECT DISTINCT candidate_name
        FROM suggestions
        WHERE episode_id = %s
          AND suggestion_id != %s
          AND status = 'pending'
    """, (row['episode_id'], row['suggestion_id']))
    other_pending_names = [r['candidate_name'] for r in cur.fetchall()]

    return {
        "done": False,
        **row,
        "existing_credits": existing_credits,
        "other_pending_suggestions": other_count,
        "other_pending_names": other_pending_names,
    }


@app.get("/api/admin/suggestions/next", dependencies=[Depends(verify_admin)])
async def get_next_suggestion(apple_podcast_id: str = None):
    """Next pending suggestion for review, with full episode context.

    Optionally scoped to one show via apple_podcast_id.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        result = _load_suggestion(cur, apple_podcast_id=apple_podcast_id)
        cur.close()
        conn.close()
        return result or {"done": True, "message": "No more suggestions to review!"}
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/suggestions/id/{suggestion_id}", dependencies=[Depends(verify_admin)])
async def get_suggestion_by_id(suggestion_id: int):
    """One specific suggestion, so a link to it can be shared and reopened.

    Returns it whatever its status — a link to an already-handled suggestion
    should still show what it was, rather than 404.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        result = _load_suggestion(cur, suggestion_id)
        cur.close()
        conn.close()
        if not result:
            raise HTTPException(status_code=404, detail=f"No suggestion with id {suggestion_id}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/suggestions/stats", dependencies=[Depends(verify_admin)])
async def get_suggestion_stats():
    """Return counts by status."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT status, COUNT(*) as count
            FROM suggestions
            GROUP BY status
            ORDER BY status
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {r['status']: r['count'] for r in rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/suggestions/sources", dependencies=[Depends(verify_admin)])
async def get_suggestion_sources(apple_podcast_id: str = None, status: str = "pending"):
    """Counts of pending suggestions grouped by the specific pattern that
    found them (source, e.g. "title_dash", "desc_bio_sentence" — see
    extract_candidate_names_tagged() in the scanner). Powers the filter
    chips on the list view: an admin picks a pattern they trust, sees how
    many suggestions it covers, and reviews that batch as a group instead
    of one row at a time.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT s.source, COUNT(*) AS count
            FROM suggestions s
            JOIN episodes e ON s.episode_id = e.episode_id
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE s.status = %(status)s
              AND (%(apple_podcast_id)s IS NULL OR p.apple_podcast_id = %(apple_podcast_id)s)
            GROUP BY s.source
            ORDER BY count DESC
        """, {"status": status, "apple_podcast_id": apple_podcast_id})
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/suggestions/list", dependencies=[Depends(verify_admin)])
async def list_suggestions(apple_podcast_id: str = None, source: str = None,
                            search: str = "", status: str = "pending",
                            sort: str = "newest", limit: int = 50, offset: int = 0,
                            episode_id: int = None):
    """Paginated, filterable suggestions — the table view behind bulk
    review, as opposed to /suggestions/next's one-row-at-a-time queue.
    Filtering by source (the specific pattern tag) is what lets an admin
    preview a batch before acting on it.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        sort_map = {
            "newest":    "s.created_at DESC",
            "oldest":    "s.created_at ASC",
            "name_asc":  "s.candidate_name ASC",
        }
        order = sort_map.get(sort, "s.created_at DESC")

        params = {
            "apple_podcast_id": apple_podcast_id,
            "source": source,
            "search": search,
            "status": status,
            "limit": limit,
            "offset": offset,
            "episode_id": episode_id,
        }
        where = """
            s.status = %(status)s
            AND (%(apple_podcast_id)s IS NULL OR p.apple_podcast_id = %(apple_podcast_id)s)
            AND (%(source)s IS NULL OR s.source = %(source)s)
            AND (%(search)s = '' OR s.candidate_name ILIKE '%%' || %(search)s || '%%')
            AND (%(episode_id)s IS NULL OR s.episode_id = %(episode_id)s)
        """

        cur.execute(f"""
            SELECT s.suggestion_id, s.candidate_name, s.source, s.matched_text,
                   s.created_at, e.episode_id, e.title AS episode_title,
                   p.title AS podcast_title, p.apple_podcast_id
            FROM suggestions s
            JOIN episodes e ON s.episode_id = e.episode_id
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE {where}
            ORDER BY {order}
            LIMIT %(limit)s OFFSET %(offset)s
        """, params)
        items = cur.fetchall()

        cur.execute(f"""
            SELECT COUNT(*) AS total
            FROM suggestions s
            JOIN episodes e ON s.episode_id = e.episode_id
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE {where}
        """, params)
        total = cur.fetchone()["total"]
        # Everything in this status, for "N of M" beside the filtered count.
        cur.execute("SELECT COUNT(*) AS all_total FROM suggestions WHERE status = %(status)s", params)
        all_total = cur.fetchone()["all_total"]

        cur.close()
        conn.close()
        return {"items": items, "total": total, "all_total": all_total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/suggestions/{suggestion_id}/approve", dependencies=[Depends(verify_admin)])
async def approve_suggestion(suggestion_id: int, body: NameOverrideRequest = None):
    """
    Approve a suggestion:
    1. Create host record (optionally with name override from UI)
    2. Link to the source episode
    3. Scan ALL episodes for this name and link any matches
    4. Return a summary of what was created
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get the suggestion
        cur.execute("""
            SELECT s.*, e.podcast_id
            FROM suggestions s
            JOIN episodes e ON s.episode_id = e.episode_id
            WHERE s.suggestion_id = %s AND s.status = 'pending'
        """, (suggestion_id,))
        suggestion = cur.fetchone()

        if not suggestion:
            raise HTTPException(status_code=404, detail="Suggestion not found or already reviewed")

        first_name = suggestion['first_name']
        last_name  = suggestion['last_name']
        name       = suggestion['candidate_name']
        episode_id = suggestion['episode_id']
        source     = suggestion['source']

        # Apply name override from UI if provided
        if body and body.name and body.name.strip():
            name = body.name.strip()
            parts = name.split(' ')
            first_name = ' '.join(parts[:-1]) if len(parts) > 1 else name
            last_name  = parts[-1] if len(parts) > 1 else ''

        # 1. Create or get host record
        host_id = find_host_by_full_name(cur, name)
        if host_id is None:
            cur.execute("""
                INSERT INTO hosts (first_name, last_name, data_source, created_at)
                VALUES (%s, %s, 'approved_suggestion', NOW())
                ON CONFLICT (first_name, last_name) DO UPDATE
                    SET first_name = EXCLUDED.first_name
                RETURNING host_id
            """, (first_name, last_name))
            host_id = cur.fetchone()['host_id']

        # 2. Link to the source episode. episode_host.data_source stays
        # coarse ("parsed_title"/"parsed_desc") even though suggestions.source
        # is now tagged with the specific pattern that fired — see
        # coarse_source()'s docstring for why several other places key off
        # the coarse value exactly.
        cur.execute("""
            INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
            VALUES (%s, %s, true, 'Guest', %s)
            ON CONFLICT (episode_id, host_id) DO NOTHING
        """, (episode_id, host_id, coarse_source(source)))

        # 3. Link every OTHER episode whose title/description names this person
        additional_links = link_matching_episodes(cur, name, host_id, episode_id)

        # 4. Mark suggestion approved
        cur.execute("""
            UPDATE suggestions
            SET status = 'approved', reviewed_at = NOW(), host_id = %s
            WHERE suggestion_id = %s
        """, (host_id, suggestion_id))

        # 5. Also mark any other pending suggestions for this name as approved
        cur.execute("""
            UPDATE suggestions
            SET status = 'approved', reviewed_at = NOW(), host_id = %s
            WHERE LOWER(candidate_name) = LOWER(%s) AND status = 'pending'
        """, (host_id, name))

        conn.commit()
        cur.close()
        conn.close()

        # Group additional links by podcast for the summary
        from collections import defaultdict
        by_podcast = defaultdict(int)
        for link in additional_links:
            by_podcast[link['podcast_title']] += 1

        return {
            "success": True,
            "host_id": host_id,
            "name": name,
            "source_episode_linked": True,
            "additional_episodes_linked": len(additional_links),
            "by_podcast": dict(by_podcast),
            "message": (
                f"Created {name}. "
                f"Found {len(additional_links)} additional episode appearance(s)"
                + (f" across: {', '.join(f'{p} ({n})' for p, n in by_podcast.items())}" if by_podcast else "")
            )
        }

    except HTTPException:
        raise
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))




@app.post("/api/admin/suggestions/{suggestion_id}/approve_only", dependencies=[Depends(verify_admin)])
async def approve_suggestion_only(suggestion_id: int, body: NameOverrideRequest = None):
    """
    Approve a suggestion as a person but don't link to the source episode.
    Scans ALL OTHER episodes for this name and links any matches.
    Use when the person is referenced/mentioned but not actually a guest on this episode.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT s.*, e.podcast_id
            FROM suggestions s
            JOIN episodes e ON s.episode_id = e.episode_id
            WHERE s.suggestion_id = %s AND s.status = 'pending'
        """, (suggestion_id,))
        suggestion = cur.fetchone()

        if not suggestion:
            raise HTTPException(status_code=404, detail="Suggestion not found or already reviewed")

        first_name = suggestion['first_name']
        last_name  = suggestion['last_name']
        name       = suggestion['candidate_name']
        episode_id = suggestion['episode_id']
        source     = suggestion['source']

        # Apply name override from UI if provided
        if body and body.name and body.name.strip():
            name = body.name.strip()
            parts = name.split(' ')
            first_name = ' '.join(parts[:-1]) if len(parts) > 1 else name
            last_name  = parts[-1] if len(parts) > 1 else ''

        # Create or get host record
        host_id = find_host_by_full_name(cur, name)
        if host_id is None:
            cur.execute("""
                INSERT INTO hosts (first_name, last_name, data_source, created_at)
                VALUES (%s, %s, 'approved_suggestion', NOW())
                ON CONFLICT (first_name, last_name) DO UPDATE
                    SET first_name = EXCLUDED.first_name
                RETURNING host_id
            """, (first_name, last_name))
            host_id = cur.fetchone()['host_id']

        # Link every OTHER episode whose title/description names this person
        additional_links = link_matching_episodes(cur, name, host_id, episode_id)

        # Mark suggestion approved
        cur.execute("""
            UPDATE suggestions SET status = 'approved', reviewed_at = NOW(), host_id = %s
            WHERE suggestion_id = %s
        """, (host_id, suggestion_id))

        # Mark other pending suggestions for same name as approved
        cur.execute("""
            UPDATE suggestions SET status = 'approved', reviewed_at = NOW(), host_id = %s
            WHERE LOWER(candidate_name) = LOWER(%s) AND status = 'pending'
        """, (host_id, name))

        conn.commit()
        cur.close()
        conn.close()

        from collections import defaultdict
        by_podcast = defaultdict(int)
        for l in additional_links:
            by_podcast[l['podcast_title']] += 1

        return {
            "success": True,
            "host_id": host_id,
            "name": name,
            "source_episode_linked": False,
            "additional_episodes_linked": len(additional_links),
            "by_podcast": dict(by_podcast),
            "message": (
                f"Created {name} (not linked to this episode). "
                f"Found {len(additional_links)} appearance(s) in other episodes"
                + (f": {', '.join(f'{p} ({n})' for p, n in by_podcast.items())}" if by_podcast else "")
            )
        }

    except HTTPException:
        raise
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/suggestions/{suggestion_id}/reject", dependencies=[Depends(verify_admin)])
async def reject_suggestion(suggestion_id: int):
    """
    Reject a suggestion:
    1. Mark it rejected
    2. Add to rejected_names so it never resurfaces
    3. Mark all other pending suggestions for this name as rejected too
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT candidate_name FROM suggestions
            WHERE suggestion_id = %s AND status = 'pending'
        """, (suggestion_id,))
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Suggestion not found or already reviewed")

        name = row['candidate_name']

        # Mark this suggestion rejected
        cur.execute("""
            UPDATE suggestions
            SET status = 'rejected', reviewed_at = NOW()
            WHERE suggestion_id = %s
        """, (suggestion_id,))

        # Mark all other pending suggestions for same name as rejected
        cur.execute("""
            UPDATE suggestions
            SET status = 'rejected', reviewed_at = NOW()
            WHERE LOWER(candidate_name) = LOWER(%s) AND status = 'pending'
        """, (name,))
        also_rejected = cur.rowcount

        # Add to permanent rejected_names blocklist
        cur.execute("""
            INSERT INTO rejected_names (candidate_name)
            VALUES (%s)
            ON CONFLICT (candidate_name) DO NOTHING
        """, (name,))

        conn.commit()
        cur.close()
        conn.close()

        return {
            "success": True,
            "name": name,
            "also_rejected": also_rejected,
            "message": f"Rejected '{name}' and added to permanent blocklist"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


class RejectNameRequest(BaseModel):
    candidate_name: str


@app.post("/api/admin/rejected-names", dependencies=[Depends(verify_admin)])
async def add_rejected_name(body: RejectNameRequest):
    """Pre-emptively blocklist a name with no suggestion yet to reject —
    e.g. a person who is discussed but never a guest (a head of state named
    in an episode's description), where a fix elsewhere might otherwise
    surface them later. Mirrors reject_suggestion's rejected_names insert,
    minus the parts that only make sense for an existing suggestion row."""
    try:
        name = body.candidate_name.strip()
        if not name:
            raise HTTPException(status_code=400, detail="candidate_name is required")

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            UPDATE suggestions
            SET status = 'rejected', reviewed_at = NOW()
            WHERE LOWER(candidate_name) = LOWER(%s) AND status = 'pending'
        """, (name,))
        also_rejected = cur.rowcount

        cur.execute("""
            INSERT INTO rejected_names (candidate_name)
            VALUES (%s)
            ON CONFLICT (candidate_name) DO NOTHING
        """, (name,))

        conn.commit()
        cur.close()
        conn.close()

        return {
            "success": True,
            "name": name,
            "also_rejected": also_rejected,
            "message": f"Added '{name}' to permanent blocklist",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/suggestions/{suggestion_id}/skip", dependencies=[Depends(verify_admin)])
async def skip_suggestion(suggestion_id: int):
    """Move a suggestion to the back of the queue."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE suggestions
            SET created_at = NOW()
            WHERE suggestion_id = %s AND status = 'pending'
        """, (suggestion_id,))
        conn.commit()
        cur.close()
        conn.close()
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================================================================
# ADMIN ENDPOINTS — Image suggestions queue
# ==================================================================

@app.get("/api/admin/images/next", dependencies=[Depends(verify_admin)])
async def get_next_image_person(skip: str = ""):
    """Get the next person without a profile image, ordered by most appearances.
    skip: comma-separated host_ids to exclude this session.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        skip_ids = [int(x) for x in skip.split(',') if x.strip().isdigit()]
        skip_clause = f"AND h.host_id NOT IN ({','.join(str(i) for i in skip_ids)})" if skip_ids else ""

        cur.execute(f"""
            SELECT
                h.host_id,
                h.first_name || ' ' || h.last_name AS host_name,
                h.twitter_handle,
                h.profile_image_url,
                (SELECT COUNT(*) FROM hosts WHERE profile_image_url IS NULL) AS total_missing,
                (
                    SELECT ARRAY_AGG(DISTINCT p.title ORDER BY p.title)
                    FROM episode_host eh
                    JOIN episodes e ON eh.episode_id = e.episode_id
                    JOIN podcasts p ON e.podcast_id = p.podcast_id
                    WHERE eh.host_id = h.host_id
                ) AS podcasts,
                (
                    SELECT COUNT(DISTINCT eh.episode_id)
                    FROM episode_host eh
                    WHERE eh.host_id = h.host_id
                ) AS appearances
            FROM hosts h
            WHERE h.profile_image_url IS NULL
            {skip_clause}
            ORDER BY appearances DESC NULLS LAST
            LIMIT 1
        """)

        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return {"done": True, "message": "Everyone has a profile image!"}

        return {"done": False, **row}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/images/stats", dependencies=[Depends(verify_admin)])
async def get_image_stats():
    """Return image coverage stats."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(profile_image_url) as with_image,
                COUNT(*) - COUNT(profile_image_url) as missing
            FROM hosts
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        return dict(row)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class TwitterHandleRequest(BaseModel):
    twitter_url: str


@app.post("/api/admin/images/{host_id}/set_twitter", dependencies=[Depends(verify_admin)])
async def set_twitter_handle(host_id: int, body: TwitterHandleRequest):
    """
    Extract handle from a Twitter/X URL, store handle and image URL on the host.
    Returns the image URL for preview before final approval.
    """
    try:
        import re
        # Extract handle from URL like https://x.com/shaylekann or https://twitter.com/drvolts
        match = re.search(r'(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)', body.twitter_url)
        if not match:
            raise HTTPException(status_code=400, detail="Could not extract Twitter handle from URL")

        handle = match.group(1)
        image_url = f'https://unavatar.io/twitter/{handle}'

        # Store the handle — image saved on approve
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE hosts SET twitter_handle = %s WHERE host_id = %s",
            (handle, host_id)
        )
        conn.commit()
        cur.close()
        conn.close()

        return {
            "success": True,
            "handle": handle,
            "image_url": image_url,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/images/{host_id}/approve", dependencies=[Depends(verify_admin)])
async def approve_image(host_id: int):
    """Save the Twitter image URL to hosts.profile_image_url."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            "SELECT twitter_handle, first_name || ' ' || last_name AS name FROM hosts WHERE host_id = %s",
            (host_id,)
        )
        row = cur.fetchone()
        if not row or not row['twitter_handle']:
            raise HTTPException(status_code=400, detail="No Twitter handle set for this host")

        image_url = f"https://unavatar.io/twitter/{row['twitter_handle']}"
        cur.execute(
            "UPDATE hosts SET profile_image_url = %s WHERE host_id = %s",
            (image_url, host_id)
        )
        conn.commit()
        cur.close()
        conn.close()

        return {"success": True, "name": row['name'], "image_url": image_url}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/images/{host_id}/skip", dependencies=[Depends(verify_admin)])
async def skip_image(host_id: int):
    """Skip this person — move them to the back by setting a placeholder."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Set a sentinel so they don't appear at top again this session
        cur.execute(
            "UPDATE hosts SET twitter_handle = COALESCE(twitter_handle, 'skipped') WHERE host_id = %s",
            (host_id,)
        )
        conn.commit()
        cur.close()
        conn.close()
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------------------------------------------
# Company logos
# ------------------------------------------------------------------
#
# logo.dev by the organisation's website (the same account and key as
# Colorado Current; fallback=404 so an unknown domain is a plain miss, not a
# generic monogram), else the freely licensed logo on Wikimedia Commons that
# Wikidata names, else the same for its parent ("Harvard Kennedy School"
# shows Harvard's). Served from here so the key never reaches a browser.
# Kept in memory, and cached by browsers for a week; a miss is a 404 and
# the page shows the organisation's initial instead.

LOGO_DEV_TOKEN = os.getenv("LOGO_DEV_TOKEN")
_LOGO_CACHE: "OrderedDict[tuple, tuple | None]" = OrderedDict()
_LOGO_CACHE_MAX = 3000
_LOGO_UA = 'PodcastNetwork/1.0 (https://github.com/FrazRoc/podcast-network) logo fetcher'


def _logo_sources(cur, org_id: int) -> list:
    """(kind, url) to try, in order, walking up to three parents."""
    urls, seen = [], set()
    while org_id and org_id not in seen and len(seen) < 4:
        seen.add(org_id)
        cur.execute("SELECT website_domain, commons_logo_url, parent_org_id FROM organizations "
                    "WHERE org_id = %s AND NOT not_an_org", (org_id,))
        row = cur.fetchone()
        if not row:
            break
        if row['website_domain'] and LOGO_DEV_TOKEN:
            urls.append(('logo.dev', f"https://img.logo.dev/{urllib.parse.quote(row['website_domain'])}"
                                     f"?token={LOGO_DEV_TOKEN}&size=128&format=png&fallback=404"))
        if row['commons_logo_url']:
            urls.append(('commons', row['commons_logo_url'] + '?width=128'))
        org_id = row['parent_org_id']
    return urls


async def _fetch_logo(sources: list):
    import httpx
    async with httpx.AsyncClient(follow_redirects=True, timeout=10, headers={'User-Agent': _LOGO_UA}) as client:
        for _, url in sources:
            try:
                r = await client.get(url)
            except Exception:
                continue
            ctype = r.headers.get('content-type', '')
            if r.status_code == 200 and ctype.startswith('image/') and r.content:
                return r.content, ctype
    return None


@app.get("/api/logo/{org_id}")
async def company_logo(org_id: int):
    from fastapi.responses import Response
    key = (org_id,)
    if key in _LOGO_CACHE:
        _LOGO_CACHE.move_to_end(key)
        hit = _LOGO_CACHE[key]
    else:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            sources = _logo_sources(cur, org_id)
        finally:
            cur.close()
            conn.close()
        hit = await _fetch_logo(sources) if sources else None
        _LOGO_CACHE[key] = hit
        if len(_LOGO_CACHE) > _LOGO_CACHE_MAX:
            _LOGO_CACHE.popitem(last=False)
    if not hit:
        # Cached briefly by browsers too, so a page of initials doesn't re-ask.
        return Response(status_code=404, headers={'Cache-Control': 'public, max-age=86400'})
    content, ctype = hit
    return Response(content=content, media_type=ctype,
                    headers={'Cache-Control': 'public, max-age=604800', 'Access-Control-Allow-Origin': '*'})


@app.get("/api/proxy/image")
async def proxy_image(url: str):
    """
    Proxy external images to avoid CORS issues in canvas rendering.
    Usage: /api/proxy/image?url=https://unavatar.io/twitter/drvolts
    """
    import httpx
    from fastapi.responses import Response

    if not is_allowed_image_url(url):
        raise HTTPException(status_code=400, detail="URL host is not an allowed image source")

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=10) as client:
            resp = await client.get(url, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            })
            return Response(
                content=resp.content,
                media_type=resp.headers.get('content-type', 'image/jpeg'),
                headers={'Access-Control-Allow-Origin': '*', 'Cache-Control': 'public, max-age=86400'},
            )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch image: {e}")


# ==================================================================
# ADMIN ENDPOINTS — People management
# ==================================================================

class CreatePersonRequest(BaseModel):
    first_name: str
    last_name: str
    twitter_url: Optional[str] = None
    bluesky_url: Optional[str] = None
    linkedin_url: Optional[str] = None


@app.post("/api/admin/people", dependencies=[Depends(verify_admin)])
async def create_person(body: CreatePersonRequest):
    """
    Create a new person, optionally fetch their profile image,
    then scan all episodes for name matches.
    """
    try:
        import re
        import httpx

        first_name = body.first_name.strip()
        last_name  = body.last_name.strip()
        full_name  = f"{first_name} {last_name}"

        conn = get_db_connection()
        cur  = conn.cursor()

        # Check if already exists — by whole name, so splitting a compound
        # surname differently doesn't slip a second record past this check.
        existing_id = find_host_by_full_name(cur, full_name)
        if existing_id:
            cur.close()
            conn.close()
            raise HTTPException(status_code=409, detail=f"{full_name} already exists (host_id={existing_id})")

        # Fetch image if handle provided
        image_url     = None
        twitter_handle = None
        bluesky_handle = None

        if body.twitter_url:
            tw_match = re.search(r'(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)', body.twitter_url)
            if tw_match:
                twitter_handle = tw_match.group(1)

        if body.bluesky_url:
            bsky_match = re.search(r'bsky\.app/profile/([A-Za-z0-9._-]+)', body.bluesky_url)
            if bsky_match:
                bluesky_handle = bsky_match.group(1)
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(
                        'https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile',
                        params={'actor': bluesky_handle}
                    )
                    if resp.status_code == 200:
                        image_url = resp.json().get('avatar')

        if not image_url and twitter_handle:
            image_url = f'https://unavatar.io/twitter/{twitter_handle}'

        # Create host record
        linkedin_url = body.linkedin_url.strip() if body.linkedin_url else None

        cur.execute("""
            INSERT INTO hosts (first_name, last_name, profile_image_url, twitter_handle, bluesky_handle, linkedin_url, data_source, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, 'manual', NOW())
            RETURNING host_id
        """, (first_name, last_name, image_url, twitter_handle, bluesky_handle, linkedin_url))
        host_id = cur.fetchone()['host_id']

        # Scan all episodes for name matches. A match on a show where this
        # person is already a recorded official host means "Host", not
        # "Guest" — same fix as episode_name_scanner.py's run() command.
        links = link_matching_episodes_with_role(cur, full_name, host_id)
        links += link_matching_episodes_by_first_name(cur, host_id, first_name, last_name)

        conn.commit()
        cur.close()
        conn.close()

        from collections import defaultdict
        by_podcast = defaultdict(int)
        for l in links:
            by_podcast[l['podcast_title']] += 1

        return {
            "success": True,
            "host_id": host_id,
            "name": full_name,
            "image_url": image_url,
            "episodes_linked": len(links),
            "by_podcast": dict(by_podcast),
        }

    except HTTPException:
        raise
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


# Diminutives that aren't simply a prefix of the full name, so the prefix rule
# below can't catch them. Prefix handles Al/Albert, Nat/Nathaniel, Dan/Daniel,
# Matt/Matthew and most of the rest on its own.
_NICKNAMES = {
    ('bob', 'robert'), ('bill', 'william'), ('dick', 'richard'), ('jack', 'john'),
    ('peggy', 'margaret'), ('betty', 'elizabeth'), ('liz', 'elizabeth'),
    ('hank', 'henry'), ('chuck', 'charles'), ('rick', 'richard'), ('ted', 'edward'),
    ('ned', 'edward'), ('tony', 'anthony'), ('kate', 'katherine'),
    ('kathy', 'katherine'), ('sandy', 'sandra'), ('jim', 'james'),
    ('greg', 'gregory'), ('mike', 'michael'), ('joe', 'joseph'), ('tom', 'thomas'),
}


def _name_tokens(name: str) -> list:
    return [t for t in re.split(r'[^A-Za-z]+', (name or '').lower()) if t]


def _duplicate_kind(a_tokens: list, b_tokens: list):
    """Classify two names as a possible same-person pair, or None.

    Compares whole tokens, never substrings — an earlier substring-based pass
    matched "Jordan Yates" to "Dan Yates" because "Jordan" contains "Dan".
    """
    if not a_tokens or not b_tokens or a_tokens[-1] != b_tokens[-1]:
        return None  # require a shared surname

    sa, sb = set(a_tokens), set(b_tokens)
    if sa == sb:
        return None

    # One name carries a middle name or initial the other omits.
    if sa < sb or sb < sa:
        short, long_ = (a_tokens, b_tokens) if sa < sb else (b_tokens, a_tokens)
        return 'middle_name' if short[0] == long_[0] else None

    # Same shape, only the first name differs.
    if len(a_tokens) == len(b_tokens) and a_tokens[1:] == b_tokens[1:]:
        x, y = a_tokens[0], b_tokens[0]
        if len(x) == 1 or len(y) == 1:
            return 'initial' if x[0] == y[0] else None
        short, long_ = (x, y) if len(x) < len(y) else (y, x)
        if long_.startswith(short) and len(short) >= 2:
            return 'shortened'
        if (short, long_) in _NICKNAMES:
            return 'nickname'
    return None


def _duplicate_kind_prefix(a_tokens: list, b_tokens: list):
    """A second classification, checked within FIRST-name groups rather
    than _duplicate_kind's surname-blocking pass above: catches a record
    that's missing its last surname word entirely, not just a middle name.

    Real incident: "Amy Myers" (52 credits) and "Amy Myers Jaffe" (85
    credits) are the same person, but their last tokens ("myers" vs
    "jaffe") never match, so _duplicate_kind's surname grouping never even
    considered the pair — invisible on the Duplicates admin page until a
    human noticed by name alone. Requires the shorter name to have 2+
    tokens; a bare first name matching is far too weak a signal on its own
    and would flood the queue with unrelated people.
    """
    if len(a_tokens) < 2 or len(b_tokens) < 2 or len(a_tokens) == len(b_tokens):
        return None
    short, long_ = (a_tokens, b_tokens) if len(a_tokens) < len(b_tokens) else (b_tokens, a_tokens)
    if long_[:len(short)] == short:
        return 'compound_surname'
    return None


def _profile_richness(p) -> int:
    return sum(1 for f in ('profile_image_url', 'twitter_handle', 'bluesky_handle') if p.get(f))


def _suggest_survivor(a, b):
    """Suggest which of two records to keep, and say why.

    Only a suggestion — the reviewer can always keep the other one. Credit
    count is the main signal because it reflects which spelling the sources
    actually use, but plenty of pairs are 1-vs-1 and it settles nothing, so
    the tiebreaks run all the way down to host_id. Without that last step the
    suggestion would depend on row order and could flip between page loads.
    """
    if a['credits'] != b['credits']:
        keep, drop = (a, b) if a['credits'] > b['credits'] else (b, a)
        return keep, drop, f"more credits ({keep['credits']} vs {drop['credits']})"

    if a['shows'] != b['shows']:
        keep, drop = (a, b) if a['shows'] > b['shows'] else (b, a)
        return keep, drop, f"same credits, but appears on more shows ({keep['shows']} vs {drop['shows']})"

    ra, rb = _profile_richness(a), _profile_richness(b)
    if ra != rb:
        keep, drop = (a, b) if ra > rb else (b, a)
        return keep, drop, "same credits, but has more profile detail (image or social links)"

    keep, drop = (a, b) if a['host_id'] < b['host_id'] else (b, a)
    return keep, drop, "nothing separates them — pick by name"


@app.get("/api/admin/diagnostics", dependencies=[Depends(verify_admin)])
async def get_diagnostics():
    """Per-show data health, for the admin diagnostics page.

    Three things a chart can show that a query does not: which shows have no
    credits at all, which rest entirely on inference rather than Apple's own
    labels, and which are credited as all-guest — the signature of a show
    whose hosts were never registered.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT p.podcast_id,
                   p.title,
                   p.apple_podcast_id,
                   COUNT(DISTINCT e.episode_id)                                   AS episodes,
                   COUNT(DISTINCT e.episode_id) FILTER (WHERE c.n > 0)            AS episodes_with_credit,
                   COUNT(DISTINCT e.episode_id) FILTER (WHERE c.h > 0)            AS episodes_with_host,
                   COUNT(DISTINCT e.episode_id) FILTER (WHERE c.g > 0)            AS episodes_with_guest,
                   -- Denominator for guest coverage: an episode confirmed to
                   -- have no guest at all isn't a coverage gap, so it's
                   -- excluded from what "100%" means for that metric — see
                   -- migrate_add_no_guest_confirmed.sql.
                   COUNT(DISTINCT e.episode_id) FILTER (WHERE NOT e.no_guest_confirmed) AS episodes_guest_eligible,
                   COALESCE(SUM(c.n), 0)                                          AS credits,
                   COALESCE(SUM(c.g), 0)                                          AS guest_credits,
                   COALESCE(SUM(c.apple), 0)                                      AS apple_credits,
                   COALESCE(SUM(c.inferred), 0)                                   AS inferred_credits,
                   (SELECT COUNT(*) FROM host_podcast hp WHERE hp.podcast_id = p.podcast_id) AS registered_hosts,
                   COALESCE(p.scan_descriptions, TRUE) AS scan_descriptions
            FROM podcasts p
            LEFT JOIN episodes e ON e.podcast_id = p.podcast_id
            LEFT JOIN (
                SELECT episode_id,
                       COUNT(*)                                              AS n,
                       COUNT(*) FILTER (WHERE NOT is_guest)                  AS h,
                       COUNT(*) FILTER (WHERE is_guest)                      AS g,
                       COUNT(*) FILTER (WHERE data_source = 'apple_verified') AS apple,
                       COUNT(*) FILTER (WHERE data_source LIKE 'parsed%')     AS inferred
                FROM episode_host GROUP BY episode_id
            ) c ON c.episode_id = e.episode_id
            GROUP BY p.podcast_id, p.title, p.apple_podcast_id, p.scan_descriptions
            ORDER BY p.title
        """)
        shows = cur.fetchall()
        _add_publishing_rhythm(cur, shows)

        # How many people each episode is credited with. The zero bar is the
        # backlog; a long tail means a list of names was read as a cast.
        cur.execute("""
            SELECT credits, COUNT(*) AS episodes FROM (
                SELECT e.episode_id, COUNT(eh.host_id) AS credits
                FROM episodes e LEFT JOIN episode_host eh ON eh.episode_id = e.episode_id
                GROUP BY e.episode_id
            ) per_episode
            GROUP BY credits ORDER BY credits
        """)
        credits_per_episode = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*) AS episodes,
                   (SELECT COUNT(*) FROM episode_host) AS credits,
                   (SELECT COUNT(*) FROM hosts) AS people
            FROM episodes
        """)
        totals = cur.fetchone()

        # Names whose UTF-8 bytes were read as Latin-1 somewhere upstream and
        # re-encoded, so "Balázs" is stored as "BalÃ¡zs". The scanner can never
        # match the real spelling against the mangled one, so these people sit
        # frozen at whatever credits they arrived with. Ã and Â are the
        # signature of that double-encoding; â€ covers mangled punctuation and
        # ï¿½ the replacement character.
        cur.execute(r"""
            SELECT host_id,
                   first_name || ' ' || last_name AS stored,
                   convert_from(convert_to(first_name, 'LATIN1'), 'UTF8') || ' ' ||
                   convert_from(convert_to(last_name,  'LATIN1'), 'UTF8') AS repaired,
                   convert_from(convert_to(first_name, 'LATIN1'), 'UTF8') AS repaired_first,
                   convert_from(convert_to(last_name,  'LATIN1'), 'UTF8') AS repaired_last,
                   (SELECT COUNT(*) FROM episode_host eh WHERE eh.host_id = h.host_id) AS credits
            FROM hosts h
            WHERE first_name || ' ' || last_name ~ 'Ã|Â|â€|ï¿½'
              -- convert_to(...,'LATIN1') raises on anything outside that
              -- range, which would take the whole diagnostics page down with
              -- it. Mangled text only ever contains Latin-1 characters by
              -- definition, so requiring that loses nothing and cannot throw.
              --
              -- This string MUST stay a Python raw string (the r-prefix on
              -- cur.execute below) — a plain triple-quoted string interprets
              -- \00FF itself as a Python octal/null escape before it ever
              -- reaches Postgres, inserting a stray NUL into the query text
              -- and producing "unterminated quoted string" (a real
              -- incident: this broke the whole diagnostics endpoint in
              -- production).
              AND first_name || ' ' || last_name ~ ('^[ -' || U&'\00FF' || ']+$')
            ORDER BY credits DESC, stored
        """)
        mangled_names = cur.fetchall()

        cur.close()
        conn.close()
        return {
            "shows": shows,
            "credits_per_episode": credits_per_episode,
            "totals": totals,
            "mangled_names": mangled_names,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# How late a show is, judged against its own rhythm: the median gap between
# its last 20 episodes. Overdue once it's missed about three releases (and at
# least three weeks) — the scanner may have stopped picking it up; silent for
# fifteen releases (and at least six months), it has most likely ended.
OVERDUE_GAPS, OVERDUE_MIN_DAYS, ENDED_GAPS, ENDED_MIN_DAYS = 3, 21, 15, 180


def _add_publishing_rhythm(cur, shows: list) -> None:
    # Apple's newest-episode date per show, recorded by each scrape run's
    # backfill step (manager.backfill_episodes).
    cur.execute("""
        SELECT p.podcast_id, pt.latest_episode_date
        FROM podcasts p JOIN podcast_tracking pt ON pt.apple_podcast_id = p.apple_podcast_id
    """)
    apple_latest = {r['podcast_id']: r['latest_episode_date'] for r in cur.fetchall()}
    cur.execute("""
        SELECT podcast_id, published_date FROM (
            SELECT podcast_id, published_date,
                   ROW_NUMBER() OVER (PARTITION BY podcast_id ORDER BY published_date DESC) AS rn
            FROM episodes WHERE published_date IS NOT NULL
        ) x WHERE rn <= 21
        ORDER BY podcast_id, published_date DESC
    """)
    dates = {}
    for r in cur.fetchall():
        d = r['published_date']
        dates.setdefault(r['podcast_id'], []).append(d.date() if hasattr(d, 'date') else d)
    today = date.today()
    for s in shows:
        ds = dates.get(s['podcast_id'], [])
        s['last_episode'] = ds[0].isoformat() if ds else None
        s['days_since'] = (today - ds[0]).days if ds else None
        gaps = sorted((a - b).days for a, b in zip(ds, ds[1:]))
        s['typical_gap'] = gaps[len(gaps) // 2] if len(gaps) >= 4 else None
        status = None
        if s['days_since'] is not None and s['typical_gap']:
            if s['days_since'] > max(ENDED_GAPS * s['typical_gap'], ENDED_MIN_DAYS):
                status = 'ended'
            elif s['days_since'] > max(OVERDUE_GAPS * s['typical_gap'], OVERDUE_MIN_DAYS):
                status = 'overdue'
        s['freshness'] = status
        # Apple has a newer episode than we do: the scanner is behind, not the show.
        al = apple_latest.get(s['podcast_id'])
        al = al.date() if hasattr(al, 'date') else al
        s['apple_latest'] = al.isoformat() if al else None
        s['missing_newer'] = bool(al and ds and al > ds[0])


@app.get("/api/admin/diagnostics/pipeline", dependencies=[Depends(verify_admin)])
async def get_pipeline_diagnostics():
    """Scanner and role-extraction health, loaded after the main page:
    episodes by week (published, and added to the database), the role
    extraction backlog and results per show, and how complete the displayed
    current roles are."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            WITH weeks AS (
                SELECT generate_series(date_trunc('week', now()) - interval '11 weeks',
                                       date_trunc('week', now()), interval '1 week')::date AS week
            )
            SELECT w.week,
                   (SELECT COUNT(*) FROM episodes e
                     WHERE e.published_date >= w.week AND e.published_date < w.week + 7) AS published,
                   (SELECT COUNT(*) FROM episodes e
                     WHERE e.created_at >= w.week AND e.created_at < w.week + 7) AS added
            FROM weeks w ORDER BY w.week
        """)
        weeks = cur.fetchall()

        # Same rule as extract_affiliations.get_appearances_to_process: guest
        # credits, not on a show the person hosts.
        guest_appearances = """
            FROM episode_host eh
            JOIN episodes e ON e.episode_id = eh.episode_id
            LEFT JOIN affiliation_extractions ax ON ax.episode_id = eh.episode_id AND ax.host_id = eh.host_id
            WHERE eh.is_guest AND NOT EXISTS (
                SELECT 1 FROM host_podcast hp WHERE hp.host_id = eh.host_id AND hp.podcast_id = e.podcast_id)
        """
        cur.execute(f"""
            SELECT COALESCE(ax.status, 'waiting') AS status, COUNT(*) AS n {guest_appearances}
            GROUP BY 1 ORDER BY 2 DESC
        """)
        extraction = cur.fetchall()
        cur.execute(f"""
            SELECT p.podcast_id, p.title, p.apple_podcast_id,
                   COUNT(*) AS appearances,
                   COUNT(*) FILTER (WHERE ax.status IS NULL OR ax.status = 'retry') AS waiting,
                   COUNT(*) FILTER (WHERE ax.status = 'done') AS done,
                   COUNT(*) FILTER (WHERE ax.status = 'done' AND EXISTS (
                       SELECT 1 FROM host_affiliations ha
                       WHERE ha.episode_id = eh.episode_id AND ha.host_id = eh.host_id)) AS with_role
            {guest_appearances.replace('JOIN episodes e ON e.episode_id = eh.episode_id',
                                       'JOIN episodes e ON e.episode_id = eh.episode_id JOIN podcasts p ON p.podcast_id = e.podcast_id')}
            GROUP BY p.podcast_id, p.title, p.apple_podcast_id
            HAVING COUNT(*) >= 10
        """)
        role_by_show = cur.fetchall()
        cur.execute("""
            SELECT COUNT(DISTINCT eh.host_id) AS n FROM episode_host eh
            WHERE eh.is_guest AND NOT EXISTS (SELECT 1 FROM host_affiliations ha WHERE ha.host_id = eh.host_id)
        """)
        guests_without_role = cur.fetchone()['n']

        # The displayed current role, for everyone ever credited as a guest —
        # the same buckets as People Admin's role filters.
        roles = _all_current_roles(cur)
        cur.execute("SELECT DISTINCT host_id FROM episode_host WHERE is_guest")
        quality = {'full': 0, 'title_only': 0, 'company_only': 0, 'none': 0}
        for r in cur.fetchall():
            title, company, _ = roles.get(r['host_id'], (None, None, None))
            quality['full' if title and company else 'title_only' if title else
                    'company_only' if company else 'none'] += 1
        cur.close()
        conn.close()
        return {"weeks": weeks, "extraction": extraction, "role_by_show": role_by_show,
                "guests_without_role": guests_without_role, "role_quality": quality}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# A person record whose name reads like an organisation or a show ("Planet
# Money", "Your Solar Cash") — the scanner took a credit line for a person.
# A worklist, not proof: surnames like Power or Cash are real, so those words
# are left out. Used by Diagnostics and People Admin's org_like_name filter.
ORG_LIKE_NAME_SQL = r"""
    lower(h.first_name) IN ('the', 'your', 'our', 'my', 'team')
    -- The whole name is a known organisation's spelling (normalize_org_name,
    -- approximated: lower case, punctuation to spaces).
    OR EXISTS (SELECT 1 FROM organization_aliases a JOIN organizations o ON o.org_id = a.org_id
               WHERE NOT o.not_an_org
                 AND a.normalized_name = btrim(regexp_replace(regexp_replace(lower(h.first_name || ' ' || h.last_name),
                                                  '[^[:alnum:][:space:]]', ' ', 'g'), '\s+', ' ', 'g')))
    OR (h.first_name || ' ' || h.last_name) ~* '\m(podcasts?|inc|llc|ltd|corp|corporation|company|institute|foundation|university|association|council|agency|network|media|news|show|team|money|solar|energy|capital|partners|group|labs|ventures|coalition|alliance|project|radio|tv|studios?)\M'
"""


@app.get("/api/admin/diagnostics/data", dependencies=[Depends(verify_admin)])
async def get_data_diagnostics():
    """Company and people data completeness: how many organisations have a
    type, website, Wikidata match and parent (all, and those 3+ people work
    at, where a gap shows most), the open merge queue, how many guests have
    each profile link, and person records whose name looks like an
    organisation."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            WITH people AS (
                SELECT oa.org_id, COUNT(DISTINCT ha.host_id) AS n
                FROM organization_aliases oa JOIN host_affiliations ha ON ha.company_key = oa.normalized_name
                GROUP BY oa.org_id
            )
            SELECT (COALESCE(p.n, 0) >= 3) AS busy,
                   COUNT(*) AS orgs,
                   COUNT(o.org_type) AS typed,
                   COUNT(o.website_domain) AS website,
                   COUNT(o.wikidata_id) AS wikidata,
                   COUNT(o.parent_org_id) AS parent
            FROM organizations o LEFT JOIN people p ON p.org_id = o.org_id
            WHERE NOT o.not_an_org
            GROUP BY 1
        """)
        by = {r['busy']: r for r in cur.fetchall()}
        keys = ('orgs', 'typed', 'website', 'wikidata', 'parent')
        orgs = {'all': {k: sum(by[b][k] for b in by) for k in keys},
                'busy': {k: (by.get(True) or {}).get(k, 0) for k in keys}}
        cur.execute(f"SELECT COUNT(*) AS n {_LIVE_SUGGESTIONS}")
        merge_queue = cur.fetchone()['n']
        cur.execute("SELECT COUNT(*) AS n FROM organizations WHERE not_an_org")
        not_orgs = cur.fetchone()['n']

        # Profile links for everyone ever credited as a guest.
        cur.execute("""
            SELECT COUNT(*) AS people,
                   COUNT(h.linkedin_url) AS linkedin,
                   COUNT(h.twitter_handle) AS twitter,
                   COUNT(h.bluesky_handle) AS bluesky,
                   COUNT(h.profile_image_url) AS photo,
                   COUNT(h.wikipedia) AS wikipedia,
                   COUNT(h.wikidata_id) AS wikidata
            FROM hosts h
            WHERE EXISTS (SELECT 1 FROM episode_host eh WHERE eh.host_id = h.host_id AND eh.is_guest)
        """)
        people = cur.fetchone()
        cur.execute(f"""
            SELECT h.host_id, h.first_name || ' ' || h.last_name AS name,
                   (SELECT COUNT(*) FROM episode_host eh WHERE eh.host_id = h.host_id) AS credits
            FROM hosts h WHERE {ORG_LIKE_NAME_SQL}
            ORDER BY credits DESC, name
        """)
        org_like = cur.fetchall()

        # A person whose whole name is a known organisation's spelling: a
        # company from an episode title read as a guest ("Freyr Battery",
        # "Nippon Steel" — 17 found by hand, Sep 2026), or the reverse, a
        # person filed as an organisation ("John Doerr").
        cur.execute("""
            SELECT a.normalized_name, o.org_id, o.name, o.org_type
            FROM organization_aliases a JOIN organizations o ON o.org_id = a.org_id
            WHERE NOT o.not_an_org
        """)
        by_key = {r['normalized_name']: r for r in cur.fetchall()}
        cur.execute("""
            SELECT h.host_id, h.first_name || ' ' || h.last_name AS name,
                   (SELECT COUNT(*) FROM episode_host eh WHERE eh.host_id = h.host_id) AS credits
            FROM hosts h
        """)
        name_matches_org = []
        for r in cur.fetchall():
            org = by_key.get(normalize_org_name(r['name']))
            if org:
                name_matches_org.append({**r, 'org_id': org['org_id'], 'org_name': org['name'],
                                         'org_type': org['org_type']})
        name_matches_org.sort(key=lambda r: -r['credits'])
        cur.close()
        conn.close()
        return {"orgs": orgs, "merge_queue": merge_queue, "not_orgs": not_orgs,
                "people": people, "org_like_names": org_like, "name_matches_org": name_matches_org}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Inferred guest credits the role extractor read as a mention, not a part in
# the episode (a politician discussed, a book's author, a thank-you). ~85% of
# a hand-checked sample were false credits, so each is reviewed here rather
# than removed in bulk. References to *other* episodes are removed
# automatically by scraper/credit_cleanup.py and never reach this list.
_MENTION_ONLY = """
    FROM episode_host eh
    JOIN affiliation_extractions ax ON ax.episode_id = eh.episode_id AND ax.host_id = eh.host_id
    JOIN episodes e ON e.episode_id = eh.episode_id
    JOIN podcasts p ON p.podcast_id = e.podcast_id
    JOIN hosts h ON h.host_id = eh.host_id
    WHERE eh.is_guest AND eh.data_source LIKE 'parsed%%' AND ax.appears_on_episode = false
"""


@app.get("/api/admin/diagnostics/mentions", dependencies=[Depends(verify_admin)])
async def list_mention_only_credits(limit: int = 25, offset: int = 0):
    """The review list: newest episodes first, with the text the extractor read."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) AS n {_MENTION_ONLY}")
        total = cur.fetchone()['n']
        cur.execute(f"""
            SELECT eh.episode_id, eh.host_id, h.first_name || ' ' || h.last_name AS name,
                   e.title AS episode_title, e.published_date, p.title AS show, p.apple_podcast_id,
                   ax.snippet,
                   (SELECT COUNT(*) FROM episode_host x WHERE x.host_id = eh.host_id) AS credits
            {_MENTION_ONLY}
            ORDER BY e.published_date DESC NULLS LAST, eh.episode_id DESC, eh.host_id
            LIMIT %(limit)s OFFSET %(offset)s
        """, {"limit": max(1, min(limit, 100)), "offset": max(0, offset)})
        items = cur.fetchall()
        cur.close()
        conn.close()
        return {"items": items, "total": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/episodes/{episode_id}/credits/{host_id}/confirm-appears",
          dependencies=[Depends(verify_admin)])
async def confirm_credit_appears(episode_id: int, host_id: int):
    """"Keep" on the review list: the person does take part (the extractor
    was wrong), so the credit leaves the list."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""UPDATE affiliation_extractions SET appears_on_episode = true
                       WHERE episode_id = %s AND host_id = %s""", (episode_id, host_id))
        conn.commit()
        return {"success": True, "updated": cur.rowcount}
    finally:
        cur.close()
        conn.close()


@app.post("/api/admin/people/{host_id}/repair-name", dependencies=[Depends(verify_admin)])
async def repair_mangled_name(host_id: int):
    """Fix a name stored with its UTF-8 read as Latin-1 ("BalÃ¡zs" ->
    "Balázs"), keeping every credit, then credit any episode that names the
    person with the real spelling. Unlike a rename in People Admin, nothing
    is unlinked first: the existing credits were right, only the stored
    spelling was not."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(r"""
            SELECT first_name, last_name,
                   convert_from(convert_to(first_name, 'LATIN1'), 'UTF8') AS first_fixed,
                   convert_from(convert_to(last_name,  'LATIN1'), 'UTF8') AS last_fixed
            FROM hosts WHERE host_id = %s
              AND first_name || ' ' || last_name ~ ('^[ -' || U&'\00FF' || ']+$')
        """, (host_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No repairable name for this person")
        if (row['first_fixed'], row['last_fixed']) == (row['first_name'], row['last_name']):
            raise HTTPException(status_code=400, detail="Name is not mangled")
        cur.execute("UPDATE hosts SET first_name = %s, last_name = %s WHERE host_id = %s",
                    (row['first_fixed'], row['last_fixed'], host_id))
        conn.commit()
        full = f"{row['first_fixed']} {row['last_fixed']}"
        links = link_matching_episodes(cur, full, host_id, exclude_episode_id=None)
        conn.commit()
        return {"success": True, "name": full, "episodes_linked": len(links)}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()


@app.get("/api/admin/people/duplicates", dependencies=[Depends(verify_admin)])
async def find_duplicate_people():
    """Surface possible duplicate people for a human to judge.

    Never merges anything. Unlike an exact-name duplicate, a shortened first
    name is not proof: "Jay Smith" and "Jayson Smith" may be two people, and
    any rule loose enough to catch Al/Albert Gore also catches those.
    """
    try:
        conn = get_db_connection()
        cur  = conn.cursor()

        cur.execute("""
            SELECT h.host_id, h.first_name, h.last_name,
                   h.first_name || ' ' || h.last_name AS name,
                   h.twitter_handle, h.bluesky_handle, h.profile_image_url,
                   h.data_source,
                   (SELECT COUNT(*) FROM episode_host eh WHERE eh.host_id = h.host_id) AS credits,
                   (SELECT COUNT(DISTINCT e.podcast_id) FROM episode_host eh
                      JOIN episodes e ON e.episode_id = eh.episode_id
                     WHERE eh.host_id = h.host_id) AS shows
            FROM hosts h
            ORDER BY h.host_id
        """)
        people = cur.fetchall()

        cur.execute("SELECT host_id_a, host_id_b FROM not_duplicate_pairs")
        dismissed = {(r['host_id_a'], r['host_id_b']) for r in cur.fetchall()}

        # Block on surname so this stays linear-ish rather than comparing all
        # ~2,000 people against each other.
        by_surname = {}
        for p in people:
            p['tokens'] = _name_tokens(p['name'])
            if p['tokens']:
                by_surname.setdefault(p['tokens'][-1], []).append(p)

        # Second blocking pass, by first name — catches a record missing its
        # last surname word entirely ("Amy Myers" / "Amy Myers Jaffe"),
        # which the surname grouping above can never compare since their
        # last tokens differ. See _duplicate_kind_prefix's docstring.
        by_first_name = {}
        for p in people:
            if len(p['tokens']) >= 2:
                by_first_name.setdefault(p['tokens'][0], []).append(p)

        candidates = []
        seen_pairs = set()
        for group in by_surname.values():
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    a, b = group[i], group[j]
                    kind = _duplicate_kind(a['tokens'], b['tokens'])
                    if not kind:
                        continue
                    pair = tuple(sorted((a['host_id'], b['host_id'])))
                    if pair in dismissed or pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)
                    candidates.append((kind, a, b))

        for group in by_first_name.values():
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    a, b = group[i], group[j]
                    kind = _duplicate_kind_prefix(a['tokens'], b['tokens'])
                    if not kind:
                        continue
                    pair = tuple(sorted((a['host_id'], b['host_id'])))
                    if pair in dismissed or pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)
                    candidates.append((kind, a, b))

        if not candidates:
            cur.close()
            conn.close()
            return {"items": [], "total": 0}

        pair_ids = [(a['host_id'], b['host_id']) for _, a, b in candidates]

        cur.execute("""
            SELECT p.a, p.b,
                   COUNT(DISTINCT ea.episode_id) AS shared_episodes
            FROM (SELECT * FROM unnest(%s::int[], %s::int[]) AS t(a, b)) p
            JOIN episode_host ea ON ea.host_id = p.a
            JOIN episode_host eb ON eb.host_id = p.b AND eb.episode_id = ea.episode_id
            GROUP BY p.a, p.b
        """, ([x for x, _ in pair_ids], [y for _, y in pair_ids]))
        shared_eps = {(r['a'], r['b']): r['shared_episodes'] for r in cur.fetchall()}

        cur.execute("""
            SELECT p.a, p.b, COUNT(DISTINCT e1.podcast_id) AS shared_shows
            FROM (SELECT * FROM unnest(%s::int[], %s::int[]) AS t(a, b)) p
            JOIN episode_host h1 ON h1.host_id = p.a
            JOIN episodes e1 ON e1.episode_id = h1.episode_id
            JOIN episode_host h2 ON h2.host_id = p.b
            JOIN episodes e2 ON e2.episode_id = h2.episode_id AND e2.podcast_id = e1.podcast_id
            GROUP BY p.a, p.b
        """, ([x for x, _ in pair_ids], [y for _, y in pair_ids]))
        shared_shows = {(r['a'], r['b']): r['shared_shows'] for r in cur.fetchall()}

        items = []
        for kind, a, b in candidates:
            key = (a['host_id'], b['host_id'])
            eps   = shared_eps.get(key, 0)
            shows = shared_shows.get(key, 0)
            same_social = bool(
                (a['twitter_handle'] and a['twitter_handle'] == b['twitter_handle']) or
                (a['bluesky_handle'] and a['bluesky_handle'] == b['bluesky_handle']) or
                (a['profile_image_url'] and a['profile_image_url'] == b['profile_image_url'])
            )

            # Both spellings credited on one episode almost always means one
            # episode's text was read two ways, not that two people appeared.
            if same_social or (eps > 0 and shows > 0):
                confidence = 'strong'
            elif eps > 0 or shows > 0:
                confidence = 'likely'
            else:
                confidence = 'review'

            keep, drop, keep_reason = _suggest_survivor(a, b)
            items.append({
                "kind": kind,
                "confidence": confidence,
                "suggested_keep_id": keep['host_id'],
                "suggested_keep_name": keep['name'],
                "suggested_drop_id": drop['host_id'],
                "suggested_drop_name": drop['name'],
                "keep_reason": keep_reason,
                "keep_credits": keep['credits'],
                "drop_credits": drop['credits'],
                "keep_shows": keep['shows'],
                "drop_shows": drop['shows'],
                "shared_episodes": eps,
                "shared_shows": shows,
                "same_social": same_social,
            })

        rank = {'strong': 0, 'likely': 1, 'review': 2}
        items.sort(key=lambda i: (rank[i['confidence']], -(i['keep_credits'] + i['drop_credits'])))

        cur.close()
        conn.close()
        return {"items": items, "total": len(items)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/people/duplicates/dismiss", dependencies=[Depends(verify_admin)])
async def dismiss_duplicate_pair(body: DismissPairRequest):
    """Mark two people as genuinely different so the pair stops resurfacing."""
    a, b = sorted((body.host_id_a, body.host_id_b))
    if a == b:
        raise HTTPException(status_code=400, detail="Need two different people")
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO not_duplicate_pairs (host_id_a, host_id_b)
        VALUES (%s, %s) ON CONFLICT DO NOTHING
    """, (a, b))
    conn.commit()
    cur.close()
    conn.close()
    return {"success": True}


@app.get("/api/admin/people/{host_id}", dependencies=[Depends(verify_admin)])
async def get_person(host_id: int):
    """Single-person summary — used for deep-linking to a person who may
    not be in the default (top-100) list, e.g. by appearance count."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT h.host_id,
                   h.first_name || ' ' || h.last_name AS full_name,
                   h.first_name, h.last_name,
                   h.profile_image_url,
                   h.twitter_handle, h.bluesky_handle, h.linkedin_url,
                   h.data_source,
                   COUNT(DISTINCT eh.episode_id) AS appearances,
                   COUNT(DISTINCT e.podcast_id)  AS podcast_count
            FROM hosts h
            LEFT JOIN episode_host eh ON eh.host_id = h.host_id
            LEFT JOIN episodes e ON e.episode_id = eh.episode_id
            WHERE h.host_id = %s
            GROUP BY h.host_id, h.first_name, h.last_name, h.profile_image_url,
                     h.twitter_handle, h.bluesky_handle, h.linkedin_url, h.data_source
        """, (host_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Person not found")
        return row
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/people", dependencies=[Depends(verify_admin)])
async def list_people(q: str = "", filter: str = "all", sort: str = "appearances_desc",
                      limit: int = 100, offset: int = 0):
    """List/search people with filtering and sorting."""
    try:
        conn = get_db_connection()
        cur  = conn.cursor()

        sort_map = {
            "appearances_desc": "appearances DESC, h.last_name ASC",
            "appearances_asc":  "appearances ASC, h.last_name ASC",
            "shows_desc":       "podcast_count DESC, appearances DESC",
            "name_asc":         "h.last_name ASC, h.first_name ASC",
            "name_desc":        "h.last_name DESC, h.first_name DESC",
            "newest":           "h.created_at DESC",
            "company_asc":      "LOWER(cr.company) ASC NULLS LAST, h.last_name ASC",
        }
        # host_id last so pages never overlap or skip people who tie.
        order = sort_map.get(sort, "appearances DESC, h.last_name ASC") + ", h.host_id"

        extra_where  = ""
        having_clause = ""
        if filter == "zero":
            having_clause = "HAVING COUNT(DISTINCT eh.episode_id) = 0"
        elif filter == "parsed":
            extra_where = "AND h.data_source IN ('parsed_desc','parsed_title','approved_suggestion')"
        elif filter == "no_image":
            extra_where = "AND h.profile_image_url IS NULL"
        elif filter == "concentrated":
            # 5+ guest credits piled onto 3 or fewer shows — the shape a real
            # public-figure over-crediting incident keeps taking (mentioned
            # in a footer/citation on every episode of a couple of shows) as
            # opposed to a real recurring guest, who tends to spread wider.
            # Not proof either way — every batch investigated this way
            # turned up a mix of real recurring guests, misfiled hosts, and
            # production/staff credits — just a worklist of who's worth a
            # closer look.
            having_clause = """
                HAVING COUNT(DISTINCT eh.episode_id) FILTER (WHERE eh.is_guest) >= 5
                   AND COUNT(DISTINCT e.podcast_id) FILTER (WHERE eh.is_guest) BETWEEN 1 AND 3
            """
        # On the displayed current role (pin, else derived — see
        # role_selection.py), not on any role ever extracted.
        elif filter == "role_title_company":
            extra_where = "AND cr.title IS NOT NULL AND cr.company IS NOT NULL"
        elif filter == "role_title_only":
            extra_where = "AND cr.title IS NOT NULL AND cr.company IS NULL"
        elif filter == "role_company_only":
            extra_where = "AND cr.title IS NULL AND cr.company IS NOT NULL"
        elif filter == "role_none":
            extra_where = "AND cr.title IS NULL AND cr.company IS NULL"
        elif filter == "no_linkedin":
            extra_where = "AND h.linkedin_url IS NULL"
        elif filter == "org_like_name":
            extra_where = f"AND ({ORG_LIKE_NAME_SQL})"

        roles = _all_current_roles(cur)
        role_ids = list(roles)
        role_params = {
            "role_ids": role_ids,
            "role_titles": [roles[i][0] for i in role_ids],
            "role_companies": [roles[i][1] for i in role_ids],
            "role_orgs": [roles[i][2] for i in role_ids],
        }

        cur.execute(f"""
            SELECT h.host_id,
                   h.first_name || ' ' || h.last_name AS full_name,
                   h.first_name, h.last_name,
                   h.profile_image_url,
                   h.twitter_handle, h.bluesky_handle, h.linkedin_url,
                   h.data_source,
                   cr.title   AS current_title,
                   cr.company AS current_company,
                   cr.org_id  AS current_org_id,
                   COUNT(DISTINCT eh.episode_id) AS appearances,
                   COUNT(DISTINCT e.podcast_id)  AS podcast_count,
                   -- Everyone matching the search and filters, before LIMIT.
                   COUNT(*) OVER () AS matching
            FROM hosts h
            LEFT JOIN unnest(%(role_ids)s::int[], %(role_titles)s::text[], %(role_companies)s::text[],
                             %(role_orgs)s::int[])
                 AS cr(host_id, title, company, org_id) ON cr.host_id = h.host_id
            LEFT JOIN episode_host eh ON eh.host_id = h.host_id
            LEFT JOIN episodes e ON e.episode_id = eh.episode_id
            WHERE (%(q)s = '' OR (h.first_name || ' ' || h.last_name) ILIKE '%%' || %(q)s || '%%')
            {extra_where}
            GROUP BY h.host_id, h.first_name, h.last_name, h.profile_image_url,
                     h.twitter_handle, h.bluesky_handle, h.data_source, h.created_at,
                     cr.title, cr.company, cr.org_id
            {having_clause}
            ORDER BY {order}
            LIMIT %(limit)s OFFSET %(offset)s
        """, {"q": q, "limit": max(1, min(limit, 500)), "offset": max(0, offset), **role_params})
        rows = cur.fetchall()
        total = rows[0]['matching'] if rows else 0
        cur.execute("SELECT COUNT(*) FROM hosts")
        all_total = cur.fetchone()['count']

        cur.close()
        conn.close()
        return {"items": list(rows), "total": total, "all_total": all_total}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/admin/people/{host_id}", dependencies=[Depends(verify_admin)])
async def update_person(host_id: int, body: CreatePersonRequest):
    """
    Update a person's name and/or social handles.
    If name changed: deletes all parsed episode links then re-scans.
    Always re-fetches profile image if a new handle is provided.
    """
    try:
        import re
        import httpx

        conn = get_db_connection()
        cur  = conn.cursor()

        cur.execute("SELECT first_name, last_name FROM hosts WHERE host_id = %s", (host_id,))
        existing = cur.fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="Person not found")

        first_name = body.first_name.strip()
        last_name  = body.last_name.strip()
        full_name  = f"{first_name} {last_name}"
        name_changed = (first_name != existing['first_name'] or last_name != existing['last_name'])

        # Extract both handles independently
        image_url      = None
        twitter_handle = None
        bluesky_handle = None

        if body.twitter_url:
            m = re.search(r'(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)', body.twitter_url)
            if m:
                twitter_handle = m.group(1)

        if body.bluesky_url:
            m = re.search(r'bsky\.app/profile/([A-Za-z0-9._-]+)', body.bluesky_url)
            if m:
                bluesky_handle = m.group(1)
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(
                        'https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile',
                        params={'actor': bluesky_handle}
                    )
                    if resp.status_code == 200:
                        image_url = resp.json().get('avatar')

        # Fall back to Twitter for image if no Bluesky image
        if not image_url and twitter_handle:
            image_url = f'https://unavatar.io/twitter/{twitter_handle}'

        linkedin_url = body.linkedin_url.strip() if body.linkedin_url else None

        # Update host record
        cur.execute("""
            UPDATE hosts SET
                first_name     = %s,
                last_name      = %s,
                twitter_handle = COALESCE(%s, twitter_handle),
                bluesky_handle = COALESCE(%s, bluesky_handle),
                linkedin_url   = COALESCE(%s, linkedin_url),
                profile_image_url = COALESCE(%s, profile_image_url)
            WHERE host_id = %s
        """, (first_name, last_name, twitter_handle, bluesky_handle, linkedin_url, image_url, host_id))
        # Typed here, so scraper/enrich.py never overwrites them.
        typed = [f for f, v in (('twitter_handle', twitter_handle), ('bluesky_handle', bluesky_handle),
                                ('linkedin_url', linkedin_url)) if v]
        if typed:
            cur.execute("UPDATE hosts SET field_sources = field_sources || %s::jsonb WHERE host_id = %s",
                        (json.dumps({f: 'admin' for f in typed}), host_id))

        # If name changed: clear parsed links, then re-scan with new name
        if name_changed:
            cur.execute("""
                DELETE FROM episode_host
                WHERE host_id = %s
                  AND data_source IN ('parsed_desc', 'parsed_title', 'approved_suggestion', 'host_first_name')
            """, (host_id,))

        conn.commit()

        # Re-scan with new name
        links = link_matching_episodes(cur, full_name, host_id, exclude_episode_id=None)
        links += link_matching_episodes_by_first_name(cur, host_id, first_name, last_name)

        conn.commit()
        cur.close()
        conn.close()

        from collections import defaultdict
        by_podcast = defaultdict(int)
        for l in links:
            by_podcast[l['podcast_title']] += 1

        return {
            "success": True,
            "host_id": host_id,
            "name": full_name,
            "image_url": image_url,
            "episodes_linked": len(links),
            "by_podcast": dict(by_podcast),
        }

    except HTTPException:
        raise
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/admin/people/{host_id}", dependencies=[Depends(verify_admin)])
async def delete_person(host_id: int):
    """Delete a person and all their episode/show links."""
    try:
        conn = get_db_connection()
        cur  = conn.cursor()

        cur.execute("SELECT first_name || ' ' || last_name AS name FROM hosts WHERE host_id = %s", (host_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Person not found")
        name = row['name']

        cur.execute("DELETE FROM episode_host WHERE host_id = %s", (host_id,))
        cur.execute("DELETE FROM host_podcast WHERE host_id = %s", (host_id,))
        cur.execute("DELETE FROM suggestions WHERE host_id = %s", (host_id,))
        cur.execute("DELETE FROM image_suggestions WHERE host_id = %s", (host_id,))
        cur.execute("DELETE FROM hosts WHERE host_id = %s", (host_id,))

        conn.commit()
        cur.close()
        conn.close()

        return {"success": True, "name": name, "message": f"Deleted {name} and all their links"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/people/{keep_id}/merge/{drop_id}", dependencies=[Depends(verify_admin)])
async def merge_people(keep_id: int, drop_id: int):
    """Fold one person's record into another and keep their name matchable.

    The dropped record's name is recorded as an alias in the same transaction.
    That is the point of the whole operation: without it the next episode that
    spells the person the old way would match nothing and the duplicate would
    come straight back.
    """
    if keep_id == drop_id:
        raise HTTPException(status_code=400, detail="Cannot merge a person into themselves")

    try:
        conn = get_db_connection()
        cur  = conn.cursor()

        cur.execute("""
            SELECT host_id, first_name || ' ' || last_name AS name,
                   profile_image_url, twitter_handle, bluesky_handle, linkedin_url,
                   bio, wikipedia, website_url, email, podchaser_id
            FROM hosts WHERE host_id IN (%s, %s)
        """, (keep_id, drop_id))
        rows = {r['host_id']: r for r in cur.fetchall()}
        if keep_id not in rows or drop_id not in rows:
            raise HTTPException(status_code=404, detail="One or both people not found")
        keep_name, drop_name = rows[keep_id]['name'], rows[drop_id]['name']
        drop_profile = rows[drop_id]

        # Where both are credited on the same episode, Apple's human-curated
        # label outranks anything we inferred, so let it win before we drop the
        # duplicate row.
        cur.execute("""
            UPDATE episode_host k
            SET is_guest = d.is_guest, role = d.role, data_source = d.data_source
            FROM episode_host d
            WHERE k.host_id = %s AND d.host_id = %s AND k.episode_id = d.episode_id
              AND d.data_source = 'apple_verified' AND k.data_source <> 'apple_verified'
        """, (keep_id, drop_id))
        labels_corrected = cur.rowcount

        cur.execute("""
            DELETE FROM episode_host d USING episode_host k
            WHERE d.host_id = %s AND k.host_id = %s AND k.episode_id = d.episode_id
        """, (drop_id, keep_id))
        duplicate_credits = cur.rowcount

        cur.execute("UPDATE episode_host SET host_id = %s WHERE host_id = %s", (keep_id, drop_id))
        credits_moved = cur.rowcount

        cur.execute("""
            DELETE FROM host_podcast d USING host_podcast k
            WHERE d.host_id = %s AND k.host_id = %s AND k.podcast_id = d.podcast_id
        """, (drop_id, keep_id))
        cur.execute("UPDATE host_podcast SET host_id = %s WHERE host_id = %s", (keep_id, drop_id))
        shows_moved = cur.rowcount

        cur.execute("UPDATE suggestions SET host_id = %s WHERE host_id = %s", (keep_id, drop_id))

        # These three are unique per host on a second column, so drop the rows
        # that would collide before re-pointing the rest — a bare UPDATE would
        # hit the constraint and roll the whole merge back.
        for table, col in (('image_suggestions', 'image_url'),
                           ('host_social_links', 'platform'),
                           ('host_roles', 'podcast_name')):
            cur.execute(
                f"DELETE FROM {table} d USING {table} k "
                f"WHERE d.host_id = %s AND k.host_id = %s AND k.{col} IS NOT DISTINCT FROM d.{col}",
                (drop_id, keep_id)
            )
            cur.execute(f"UPDATE {table} SET host_id = %s WHERE host_id = %s", (keep_id, drop_id))

        # A pinned display role follows the dropped record only if the
        # survivor has none of its own; otherwise it goes with the row.
        cur.execute("""
            UPDATE host_role_pins SET host_id = %s
            WHERE host_id = %s AND NOT EXISTS (SELECT 1 FROM host_role_pins WHERE host_id = %s)
        """, (keep_id, drop_id, keep_id))

        # Any alias pointing at the dropped record has to follow it.
        cur.execute("UPDATE host_aliases SET host_id = %s WHERE host_id = %s", (keep_id, drop_id))

        # Record the spelling we are about to delete, unless it normalizes to
        # the same string as the survivor's name (nothing to remember then).
        alias_added = False
        if normalize_full_name(drop_name) != normalize_full_name(keep_name):
            cur.execute("""
                INSERT INTO host_aliases (host_id, alias_name, normalized_name, source)
                VALUES (%s, %s, %s, 'merge')
                ON CONFLICT (normalized_name) DO NOTHING
            """, (keep_id, drop_name, normalize_full_name(drop_name)))
            alias_added = cur.rowcount > 0

        cur.execute("DELETE FROM hosts WHERE host_id = %s", (drop_id,))

        # Keep every piece of profile detail across both records: the survivor
        # wins where it already has a value and picks up the rest from the
        # record being merged away, so a handle on either one is preserved.
        #
        # This has to run after the delete. email and podchaser_id are UNIQUE,
        # so copying them across while both rows still exist trips the
        # constraint and rolls the whole merge back. NULLIF covers fields a
        # form might submit as an empty string, which COALESCE would otherwise
        # treat as a real value and keep over the other record's actual data.
        profile_fields = ('profile_image_url', 'twitter_handle', 'bluesky_handle',
                          'linkedin_url', 'bio', 'wikipedia', 'website_url',
                          'email', 'podchaser_id')
        cur.execute(
            "UPDATE hosts SET " + ", ".join(
                f"{f} = COALESCE(NULLIF({f}, ''), %s)" for f in profile_fields
            ) + " WHERE host_id = %s",
            tuple(drop_profile[f] for f in profile_fields) + (keep_id,)
        )

        # A show-level host whose episode credits still say Guest is the
        # mislabelling we correct everywhere else; merging often exposes more.
        cur.execute("""
            UPDATE episode_host eh
            SET is_guest = false, role = 'Host'
            FROM episodes e, host_podcast hp
            WHERE eh.episode_id = e.episode_id
              AND hp.host_id = eh.host_id AND hp.podcast_id = e.podcast_id
              AND eh.host_id = %s
              AND eh.is_guest = true
              AND eh.data_source NOT IN ('apple_verified', 'manual')
        """, (keep_id,))
        roles_reconciled = cur.rowcount

        conn.commit()
        cur.close()
        conn.close()

        return {
            "success": True,
            "kept": keep_name,
            "merged": drop_name,
            "alias_added": alias_added,
            "credits_moved": credits_moved,
            "duplicate_credits_removed": duplicate_credits,
            "labels_corrected": labels_corrected,
            "shows_moved": shows_moved,
            "roles_reconciled": roles_reconciled,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/people/{host_id}/aliases", dependencies=[Depends(verify_admin)])
async def list_aliases(host_id: int):
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute("""
        SELECT alias_id, alias_name, source, created_at
        FROM host_aliases WHERE host_id = %s ORDER BY alias_name
    """, (host_id,))
    items = cur.fetchall()
    cur.close()
    conn.close()
    return {"items": items}


@app.post("/api/admin/people/{host_id}/aliases", dependencies=[Depends(verify_admin)])
async def add_alias(host_id: int, body: AliasRequest):
    """Record another spelling for someone, so scans pick up either form."""
    name = (body.alias_name or '').strip()
    if not name:
        raise HTTPException(status_code=400, detail="alias_name is required")

    key = normalize_full_name(name)
    if not key:
        raise HTTPException(status_code=400, detail="alias_name must contain letters")

    try:
        conn = get_db_connection()
        cur  = conn.cursor()

        cur.execute("SELECT 1 FROM hosts WHERE host_id = %s", (host_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Person not found")

        # An alias must not collide with a real person, or scans would credit
        # the wrong record.
        cur.execute(
            f"SELECT host_id, first_name || ' ' || last_name AS name FROM hosts WHERE {NORMALIZED_NAME_SQL} = %s",
            (key,)
        )
        clash = cur.fetchone()
        if clash and clash['host_id'] != host_id:
            raise HTTPException(
                status_code=409,
                detail=f"{name} is already a person ({clash['name']}, host_id={clash['host_id']}). Merge them instead."
            )

        cur.execute("""
            INSERT INTO host_aliases (host_id, alias_name, normalized_name, source)
            VALUES (%s, %s, %s, 'manual')
            ON CONFLICT (normalized_name) DO NOTHING
            RETURNING alias_id
        """, (host_id, name, key))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=409, detail=f"{name} is already an alias of someone")

        conn.commit()
        cur.close()
        conn.close()
        return {"success": True, "alias_id": row['alias_id'], "alias_name": name}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/admin/people/aliases/{alias_id}", dependencies=[Depends(verify_admin)])
async def delete_alias(alias_id: int):
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM host_aliases WHERE alias_id = %s RETURNING alias_name", (alias_id,))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Alias not found")
    return {"success": True, "alias_name": row['alias_name']}


# ------------------------------------------------------------------
# ROLES — job title / organisation per appearance (host_affiliations, filled
# by scraper/extract_affiliations.py) and the one current role shown for a
# person (role_selection.pick_current_role, overridable by a pin).
# ------------------------------------------------------------------

def _role_rows(cur, host_id: int) -> list:
    cur.execute("""
        SELECT ha.affiliation_id, ha.episode_id, ha.title,
               -- Shown under the organisation's own name, so merges and
               -- renames in Company Admin reach every role line. A company
               -- marked "not an organisation" (a state, a fragment) is never
               -- shown as anyone's company.
               CASE WHEN org.not_an_org THEN NULL ELSE COALESCE(org.name, ha.company) END AS company,
               ha.company AS company_as_written,
               CASE WHEN org.not_an_org THEN NULL ELSE org.org_id END AS org_id, ha.title_kind,
               ha.is_former, ha.data_source,
               ax.appears_on_episode, ax.from_other_episode,
               e.published_date, e.title AS episode_title, p.title AS podcast_title, e.podcast_id,
               CASE WHEN org.not_an_org THEN NULL
                    ELSE COALESCE(org_gp.org_id, org_p.org_id, org.org_id) END AS top_org_id
        FROM host_affiliations ha
        JOIN affiliation_extractions ax
          ON ax.episode_id = ha.episode_id AND ax.host_id = ha.host_id
        LEFT JOIN organization_aliases oa ON oa.normalized_name = ha.company_key
        LEFT JOIN organizations org ON org.org_id = oa.org_id
        LEFT JOIN organizations org_p ON org_p.org_id = org.parent_org_id
        LEFT JOIN organizations org_gp ON org_gp.org_id = org_p.parent_org_id
        JOIN episodes e ON e.episode_id = ha.episode_id
        JOIN podcasts p ON p.podcast_id = e.podcast_id
        WHERE ha.host_id = %s
        ORDER BY e.published_date DESC NULLS LAST, ha.episode_id DESC, ha.affiliation_id
    """, (host_id,))
    return cur.fetchall()


def _role_pin(cur, host_id: int):
    cur.execute("SELECT title, company, updated_at FROM host_role_pins WHERE host_id = %s", (host_id,))
    return cur.fetchone()


def _all_current_roles(cur) -> dict:
    """{host_id: (title, company)} for everyone with a current role, using the
    same rule as a single person's panel. One pass over host_affiliations
    and host_role_pins rather than a query per person, so the People list
    can filter and sort on it. Row order matches _role_rows(), so ties
    resolve the same way on the list and on the panel.
    """
    cur.execute("""
        SELECT ha.host_id, ha.episode_id, ha.title,
               CASE WHEN org.not_an_org THEN NULL ELSE COALESCE(org.name, ha.company) END AS company,
               CASE WHEN org.not_an_org THEN NULL ELSE org.org_id END AS org_id, ha.title_kind, ha.is_former, ax.from_other_episode, e.published_date,
               e.podcast_id,
               CASE WHEN org.not_an_org THEN NULL
                    ELSE COALESCE(org_gp.org_id, org_p.org_id, org.org_id) END AS top_org_id
        FROM host_affiliations ha
        JOIN affiliation_extractions ax
          ON ax.episode_id = ha.episode_id AND ax.host_id = ha.host_id
        LEFT JOIN organization_aliases oa ON oa.normalized_name = ha.company_key
        LEFT JOIN organizations org ON org.org_id = oa.org_id
        LEFT JOIN organizations org_p ON org_p.org_id = org.parent_org_id
        LEFT JOIN organizations org_gp ON org_gp.org_id = org_p.parent_org_id
        JOIN episodes e ON e.episode_id = ha.episode_id
        ORDER BY ha.host_id, e.published_date DESC NULLS LAST, ha.episode_id DESC, ha.affiliation_id
    """)
    by_host = {}
    for r in cur.fetchall():
        by_host.setdefault(r['host_id'], []).append(r)
    cur.execute("SELECT host_id, title, company FROM host_role_pins")
    pins = {r['host_id']: r for r in cur.fetchall()}

    roles = {}
    for host_id in by_host.keys() | pins.keys():
        role = format_for_display(pick_current_role(by_host.get(host_id, []), pins.get(host_id)))
        if role:
            # org_id is None for a pin (free text) or an unlinked company.
            roles[host_id] = (role.get('title'), role.get('company'), role.get('org_id'))
    return roles


def _public_role(role):
    if not role:
        return None
    # org_id: the company's logo (/api/logo/{org_id}); none for a pin.
    return {k: role.get(k) for k in ('title', 'company', 'org_id', 'source', 'published_date')}


@app.get("/api/people/{host_id}/current-role")
async def get_current_role(host_id: int):
    """The one role shown on the public person card, or null."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        role = format_for_display(pick_current_role(_role_rows(cur, host_id), _role_pin(cur, host_id)))
        return {"host_id": host_id, "current_role": _public_role(role)}
    finally:
        cur.close()
        conn.close()


@app.get("/api/admin/people/{host_id}/roles", dependencies=[Depends(verify_admin)])
async def get_person_roles(host_id: int):
    """Current role, the pin behind it if any, and every role on record."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        rows = _role_rows(cur, host_id)
        pin = _role_pin(cur, host_id)
        return {
            "current": format_for_display(pick_current_role(rows, pin)),
            # What the rule alone would show, so a pin can be judged against it.
            "derived": format_for_display(pick_current_role(rows)),
            "pin": pin,
            "history": rows,
        }
    finally:
        cur.close()
        conn.close()


@app.put("/api/admin/people/{host_id}/role-pin", dependencies=[Depends(verify_admin)])
async def set_role_pin(host_id: int, body: RolePinRequest):
    title = (body.title or '').strip() or None
    company = (body.company or '').strip() or None
    if not title and not company:
        raise HTTPException(status_code=400, detail="Give a title, a company, or both")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM hosts WHERE host_id = %s", (host_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Person not found")
        cur.execute("""
            INSERT INTO host_role_pins (host_id, title, company) VALUES (%s, %s, %s)
            ON CONFLICT (host_id) DO UPDATE
            SET title = EXCLUDED.title, company = EXCLUDED.company, updated_at = now()
        """, (host_id, title, company))
        conn.commit()
        return {"success": True, "title": title, "company": company}
    finally:
        cur.close()
        conn.close()


@app.delete("/api/admin/people/{host_id}/role-pin", dependencies=[Depends(verify_admin)])
async def clear_role_pin(host_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM host_role_pins WHERE host_id = %s", (host_id,))
        conn.commit()
        return {"success": True, "cleared": cur.rowcount > 0}
    finally:
        cur.close()
        conn.close()


# ------------------------------------------------------------------
# COMPANIES — organizations / organization_aliases (migrate_add_organizations.sql).
# Roles link to an organisation through their normalised company string:
# host_affiliations.company_key = organization_aliases.normalized_name.
# New spellings are created as organisations by scraper/organizations.py
# sync; everything here is review: edit, merge, parent, "not the same".
# ------------------------------------------------------------------

ORG_TYPES = ('company', 'nonprofit', 'government', 'academic', 'research',
             'media', 'investor', 'association', 'other')


def _org_alias_keys(cur, org_ids: list) -> list:
    cur.execute("SELECT normalized_name FROM organization_aliases WHERE org_id = ANY(%s)", (org_ids,))
    return [r['normalized_name'] for r in cur.fetchall()]


def _org_family(cur, org_id: int) -> list:
    """org_id plus every organisation below it, at any depth."""
    cur.execute("""
        WITH RECURSIVE fam AS (
            SELECT org_id FROM organizations WHERE org_id = %s
            UNION SELECT o.org_id FROM organizations o JOIN fam ON o.parent_org_id = fam.org_id
        ) SELECT org_id FROM fam
    """, (org_id,))
    return [r['org_id'] for r in cur.fetchall()]


@app.get("/api/admin/companies", dependencies=[Depends(verify_admin)])
async def list_companies(q: str = "", org_type: str = "", sort: str = "people_desc",
                         view: str = "active", limit: int = 200):
    """Company list. view: 'active' (default), 'not_org' (marked not an
    organisation), 'untyped' (active, no type yet). people = distinct people
    with any role there, sub-organisations not included."""
    sort_map = {
        "people_desc": "people DESC, lower(o.name)",
        "name_asc":    "lower(o.name)",
        "newest":      "o.created_at DESC, o.org_id DESC",
    }
    where = ["o.not_an_org = %(not_org)s"]
    if org_type in ORG_TYPES:
        where.append("o.org_type = %(org_type)s")
    if view == "untyped":
        where.append("o.org_type IS NULL")
    elif view == "no_website":
        where.append("o.website_domain IS NULL")
    if q.strip():
        where.append("""(o.name ILIKE '%%' || %(q)s || '%%' OR EXISTS (
            SELECT 1 FROM organization_aliases a
            WHERE a.org_id = o.org_id AND a.alias_name ILIKE '%%' || %(q)s || '%%'))""")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            WITH counts AS (
                SELECT oa.org_id, COUNT(DISTINCT ha.host_id) AS people, COUNT(*) AS roles
                FROM organization_aliases oa
                JOIN host_affiliations ha ON ha.company_key = oa.normalized_name
                GROUP BY oa.org_id
            )
            SELECT o.org_id, o.name, o.org_type, o.parent_org_id, p.name AS parent_name,
                   o.website_domain, o.not_an_org, o.created_at,
                   COALESCE(c.people, 0) AS people, COALESCE(c.roles, 0) AS roles,
                   (SELECT COUNT(*) FROM organization_aliases a WHERE a.org_id = o.org_id) AS alias_count,
                   (SELECT COUNT(*) FROM organizations ch WHERE ch.parent_org_id = o.org_id) AS child_count,
                   COUNT(*) OVER () AS matching,
                   (SELECT COUNT(*) {_LIVE_SUGGESTIONS}
                      AND o.org_id IN (s.org_a, s.org_b)) AS suggestion_count
            FROM organizations o
            LEFT JOIN organizations p ON p.org_id = o.parent_org_id
            LEFT JOIN counts c ON c.org_id = o.org_id
            WHERE {' AND '.join(where)}
            ORDER BY {sort_map.get(sort, sort_map['people_desc'])}
            LIMIT %(limit)s
        """, {"q": q.strip(), "org_type": org_type, "not_org": view == "not_org",
              "limit": max(1, min(limit, 1000))})
        items = cur.fetchall()
        cur.execute("SELECT COUNT(*) FILTER (WHERE NOT not_an_org) AS active, "
                    "COUNT(*) FILTER (WHERE not_an_org) AS not_org, "
                    "COUNT(*) FILTER (WHERE NOT not_an_org AND org_type IS NULL) AS untyped "
                    "FROM organizations")
        totals = cur.fetchone()
        return {"items": items, "totals": totals,
                # Matching the search and filters (before LIMIT), and the whole view.
                "total": items[0]['matching'] if items else 0,
                "all_total": totals['not_org'] if view == 'not_org' else totals['active']}
    finally:
        cur.close()
        conn.close()


@app.get("/api/admin/companies/{org_id}", dependencies=[Depends(verify_admin)])
async def get_company(org_id: int, include_sub: bool = False):
    """One organisation: its aliases, parent and sub-organisations, and the
    people with a role there (optionally including sub-organisations'),
    each marked current when their displayed current role is here."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT o.*, p.name AS parent_name FROM organizations o
            LEFT JOIN organizations p ON p.org_id = o.parent_org_id WHERE o.org_id = %s
        """, (org_id,))
        org = cur.fetchone()
        if not org:
            raise HTTPException(status_code=404, detail="Company not found")

        cur.execute("""
            SELECT a.alias_id, a.alias_name, a.normalized_name, a.source,
                   (SELECT COUNT(*) FROM host_affiliations ha WHERE ha.company_key = a.normalized_name) AS roles
            FROM organization_aliases a WHERE a.org_id = %s ORDER BY roles DESC, a.alias_name
        """, (org_id,))
        aliases = cur.fetchall()

        cur.execute("""
            SELECT o.org_id, o.name, o.org_type,
                   (SELECT COUNT(DISTINCT ha.host_id) FROM organization_aliases a
                    JOIN host_affiliations ha ON ha.company_key = a.normalized_name
                    WHERE a.org_id = o.org_id) AS people
            FROM organizations o WHERE o.parent_org_id = %s ORDER BY lower(o.name)
        """, (org_id,))
        children = cur.fetchall()

        family = _org_family(cur, org_id) if include_sub else [org_id]
        keys = _org_alias_keys(cur, family)
        cur.execute("""
            SELECT ha.host_id, h.first_name || ' ' || h.last_name AS name, ha.title, ha.company,
                   ha.is_former, e.published_date, p.title AS podcast_title
            FROM host_affiliations ha
            JOIN hosts h ON h.host_id = ha.host_id
            JOIN episodes e ON e.episode_id = ha.episode_id
            JOIN podcasts p ON p.podcast_id = e.podcast_id
            WHERE ha.company_key = ANY(%s)
            ORDER BY e.published_date DESC NULLS LAST
        """, (keys,))
        role_rows = cur.fetchall()
        key_set = set(keys)
        # Linked roles match on the organisation itself; a pin is free text,
        # so it matches on spelling.
        family_ids = set(family)
        current = {h for h, (_, c, oid) in _all_current_roles(cur).items()
                   if (oid in family_ids if oid else normalize_org_name(c) in key_set)}
        people = {}
        for r in role_rows:
            person = people.setdefault(r['host_id'], {
                "host_id": r['host_id'], "name": r['name'], "roles": [],
                "latest": r['published_date'], "is_current": r['host_id'] in current,
            })
            person["roles"].append({k: r[k] for k in ('title', 'company', 'is_former',
                                                     'published_date', 'podcast_title')})
        ordered = sorted(people.values(), key=lambda p: (not p['is_current'], p['name']))
        cur.execute(f"""
            SELECT CASE WHEN s.org_a = %(id)s THEN b.org_id ELSE a.org_id END AS org_id,
                   CASE WHEN s.org_a = %(id)s THEN b.name ELSE a.name END AS name,
                   CASE WHEN s.org_a = %(id)s THEN b.org_type ELSE a.org_type END AS org_type,
                   s.reason, s.score
            {_LIVE_SUGGESTIONS} AND %(id)s IN (s.org_a, s.org_b)
            ORDER BY s.people DESC, s.score DESC
        """, {"id": org_id})
        suggestions = cur.fetchall()
        if suggestions:
            cur.execute("""
                SELECT a.org_id, COUNT(DISTINCT ha.host_id) AS people FROM organization_aliases a
                JOIN host_affiliations ha ON ha.company_key = a.normalized_name
                WHERE a.org_id = ANY(%s) GROUP BY a.org_id
            """, ([r['org_id'] for r in suggestions],))
            counts = {r['org_id']: r['people'] for r in cur.fetchall()}
            for r in suggestions:
                r['people'] = counts.get(r['org_id'], 0)
        return {"org": org, "aliases": aliases, "children": children, "people": ordered,
                "suggestions": suggestions}
    finally:
        cur.close()
        conn.close()


@app.put("/api/admin/companies/{org_id}", dependencies=[Depends(verify_admin)])
async def update_company(org_id: int, body: CompanyUpdateRequest):
    """Edit name / type / domain / parent / not-an-organisation. Only the
    fields sent are changed; send parent_org_id 0 to clear the parent."""
    fields = body.model_dump(exclude_unset=True) if hasattr(body, 'model_dump') else body.dict(exclude_unset=True)
    sets, params = [], {"org_id": org_id}
    if 'name' in fields:
        name = (fields['name'] or '').strip()
        if not name:
            raise HTTPException(status_code=400, detail="Name cannot be empty")
        sets.append("name = %(name)s"); params['name'] = name
    if 'org_type' in fields:
        t = fields['org_type'] or None
        if t is not None and t not in ORG_TYPES:
            raise HTTPException(status_code=400, detail=f"org_type must be one of {', '.join(ORG_TYPES)}")
        sets.append("org_type = %(org_type)s"); params['org_type'] = t
    if 'website_domain' in fields:
        # A bare domain ("wartsila.com") or a full link to the organisation's
        # own page ("https://www.wartsila.com/energy"): the domain is kept for
        # the logo, the link only when it has a path.
        raw = (fields['website_domain'] or '').strip()
        with_scheme = raw if re.match(r'^https?://', raw, re.I) else f'https://{raw}'
        parts = urllib.parse.urlsplit(with_scheme) if raw else None
        d = (parts.hostname or '').lower().removeprefix('www.') or None if parts else None
        url = with_scheme if parts and parts.path.strip('/') else None
        sets.append("website_domain = %(website_domain)s"); params['website_domain'] = d
        sets.append("website_url = %(website_url)s"); params['website_url'] = url
        # Changed here, so scraper/enrich.py never overwrites it (the form sends
        # the website on every save, so only a changed value counts; cleared =
        # open to enrichment again). SET sees the row as it was before.
        sets.append("""website_source = CASE WHEN website_domain IS NOT DISTINCT FROM %(website_domain)s
                                                AND website_url IS NOT DISTINCT FROM %(website_url)s
                                           THEN website_source
                                           WHEN %(website_domain)s IS NULL THEN NULL ELSE 'admin' END""")
    if 'not_an_org' in fields:
        sets.append("not_an_org = %(not_an_org)s"); params['not_an_org'] = bool(fields['not_an_org'])

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM organizations WHERE org_id = %s", (org_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Company not found")
        if 'parent_org_id' in fields:
            parent = fields['parent_org_id'] or None
            if parent is not None:
                if parent == org_id or parent in _org_family(cur, org_id):
                    raise HTTPException(status_code=400,
                                        detail="That would make the company its own ancestor")
                cur.execute("SELECT 1 FROM organizations WHERE org_id = %s", (parent,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Parent company not found")
            sets.append("parent_org_id = %(parent)s"); params['parent'] = parent
        if not sets:
            raise HTTPException(status_code=400, detail="Nothing to update")
        cur.execute(f"UPDATE organizations SET {', '.join(sets)}, updated_at = now() WHERE org_id = %(org_id)s",
                    params)
        conn.commit()
        return {"success": True}
    finally:
        cur.close()
        conn.close()


class MergeCompaniesRequest(BaseModel):
    # Spellings of the dropped company NOT to carry across: each becomes its
    # own company instead (see ambiguous_spelling).
    split_alias_ids: Optional[list[int]] = None


def _spellings_moving(cur, keep: dict, drop_id: int) -> list:
    """The dropped company's spellings, each flagged when it's ambiguous as a
    spelling of the company it would join."""
    cur.execute("""
        SELECT a.alias_id, a.alias_name,
               (SELECT COUNT(*) FROM host_affiliations ha WHERE ha.company_key = a.normalized_name) AS roles
        FROM organization_aliases a WHERE a.org_id = %s ORDER BY roles DESC, a.alias_name
    """, (drop_id,))
    return [{**r, 'ambiguous': ambiguous_spelling(r['alias_name'], keep['name'])} for r in cur.fetchall()]


def _split_alias(cur, alias_id: int) -> int:
    """Move one spelling (and every role using it) into a new company of its own."""
    cur.execute("SELECT alias_name, org_id FROM organization_aliases WHERE alias_id = %s", (alias_id,))
    a = cur.fetchone()
    if not a:
        raise HTTPException(status_code=404, detail="Spelling not found")
    cur.execute("INSERT INTO organizations (name) VALUES (%s) RETURNING org_id", (a['alias_name'],))
    new_id = cur.fetchone()['org_id']
    cur.execute("UPDATE organization_aliases SET org_id = %s, source = 'manual' WHERE alias_id = %s", (new_id, alias_id))
    return new_id


@app.get("/api/admin/companies/{keep_id}/merge/{drop_id}/preview", dependencies=[Depends(verify_admin)])
async def merge_companies_preview(keep_id: int, drop_id: int):
    """What a merge would carry across, with ambiguous spellings flagged, so
    the page can ask before "Aurora" becomes a spelling of Aurora Solar."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT org_id, name FROM organizations WHERE org_id = %s", (keep_id,))
        keep = cur.fetchone()
        if not keep:
            raise HTTPException(status_code=404, detail="Company not found")
        return {"keep": keep, "spellings": _spellings_moving(cur, keep, drop_id)}
    finally:
        cur.close()
        conn.close()


@app.post("/api/admin/companies/{org_id}/aliases/{alias_id}/split", dependencies=[Depends(verify_admin)])
async def split_company_alias(org_id: int, alias_id: int):
    """Split one spelling off into a company of its own — the fix when a
    merge carried across a word that means another organisation too."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) AS n FROM organization_aliases WHERE org_id = %s", (org_id,))
        if cur.fetchone()['n'] < 2:
            raise HTTPException(status_code=400, detail="A company's only spelling can't be split off")
        cur.execute("SELECT 1 FROM organization_aliases WHERE alias_id = %s AND org_id = %s", (alias_id, org_id))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Spelling not found on this company")
        new_id = _split_alias(cur, alias_id)
        conn.commit()
        return {"success": True, "org_id": new_id}
    finally:
        cur.close()
        conn.close()


@app.post("/api/admin/companies/{keep_id}/merge/{drop_id}", dependencies=[Depends(verify_admin)])
async def merge_companies(keep_id: int, drop_id: int, body: Optional[MergeCompaniesRequest] = None):
    """Fold one organisation into another. Every spelling of the dropped one
    becomes an alias of the survivor, so all its roles re-link at once —
    except any listed in split_alias_ids, which become companies of their own."""
    if keep_id == drop_id:
        raise HTTPException(status_code=400, detail="Cannot merge a company into itself")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM organizations WHERE org_id IN (%s, %s)", (keep_id, drop_id))
        rows = {r['org_id']: r for r in cur.fetchall()}
        if keep_id not in rows or drop_id not in rows:
            raise HTTPException(status_code=404, detail="One or both companies not found")
        keep, drop = rows[keep_id], rows[drop_id]
        keep_family = set(_org_family(cur, keep_id))

        split = set((body.split_alias_ids if body else None) or [])
        cur.execute("SELECT alias_id FROM organization_aliases WHERE org_id = %s", (drop_id,))
        for alias_id in [r['alias_id'] for r in cur.fetchall() if r['alias_id'] in split]:
            _split_alias(cur, alias_id)

        cur.execute("UPDATE organization_aliases SET org_id = %s, "
                    "source = CASE WHEN source = 'auto' THEN 'merge' ELSE source END "
                    "WHERE org_id = %s", (keep_id, drop_id))
        aliases_moved = cur.rowcount
        # Sub-organisations of the dropped one now hang off the survivor; a
        # survivor that was the dropped one's child keeps no self-parent.
        cur.execute("UPDATE organizations SET parent_org_id = %s WHERE parent_org_id = %s AND org_id <> %s",
                    (keep_id, drop_id, keep_id))
        if keep['parent_org_id'] == drop_id:
            cur.execute("UPDATE organizations SET parent_org_id = %s WHERE org_id = %s",
                        (drop['parent_org_id'], keep_id))
        # The survivor keeps its own details and picks up any it lacks.
        cur.execute("""
            UPDATE organizations SET
                org_type       = COALESCE(org_type, %s),
                website_domain = COALESCE(website_domain, %s),
                parent_org_id  = COALESCE(parent_org_id, %s),
                updated_at     = now()
            WHERE org_id = %s
        """, (drop['org_type'], drop['website_domain'],
              # Never inherit a parent from inside the survivor's own family.
              drop['parent_org_id'] if drop['parent_org_id'] not in keep_family else None, keep_id))
        cur.execute("DELETE FROM organizations WHERE org_id = %s", (drop_id,))
        conn.commit()
        return {"success": True, "kept": keep['name'], "merged": drop['name'], "aliases_moved": aliases_moved}
    finally:
        cur.close()
        conn.close()


@app.post("/api/admin/companies/not-same", dependencies=[Depends(verify_admin)])
async def mark_companies_not_same(body: NotSameOrgRequest):
    """Record that two organisations are different, so they stop being suggested."""
    a, b = sorted((body.org_a, body.org_b))
    if a == b:
        raise HTTPException(status_code=400, detail="Pick two different companies")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO not_same_org_pairs (org_a, org_b) VALUES (%s, %s) ON CONFLICT DO NOTHING", (a, b))
        cur.execute("DELETE FROM company_merge_suggestions WHERE org_a = %s AND org_b = %s", (a, b))
        conn.commit()
        return {"success": True}
    finally:
        cur.close()
        conn.close()


# A stored merge suggestion that still needs a decision: neither side marked
# not an organisation, and the two not already parent and child. Shared by
# the queue, the list's per-row count and the company panel.
_LIVE_SUGGESTIONS = """
    FROM company_merge_suggestions s
    JOIN organizations a ON a.org_id = s.org_a
    JOIN organizations b ON b.org_id = s.org_b
    WHERE NOT a.not_an_org AND NOT b.not_an_org
      AND a.parent_org_id IS DISTINCT FROM b.org_id
      AND b.parent_org_id IS DISTINCT FROM a.org_id
"""


@app.get("/api/admin/companies-suggestions", dependencies=[Depends(verify_admin)])
async def company_merge_suggestions(limit: int = 40, offset: int = 0):
    """The stored merge-suggestion queue (see org_suggestions.py), biggest
    first. Rows whose pair has since been decided are skipped: either side
    marked not an organisation, or the two now parent and child."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        live = _LIVE_SUGGESTIONS
        cur.execute(f"""
            SELECT s.org_a, s.org_b, s.reason, s.score, s.people {live}
            ORDER BY s.people DESC,
                     CASE s.reason WHEN 'acronym' THEN 0 WHEN 'similar' THEN 1 ELSE 2 END,
                     s.score DESC, s.org_a, s.org_b
            LIMIT %s OFFSET %s
        """, (max(1, min(limit, 200)), max(0, offset)))
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) AS total, MAX(s.computed_at) AS computed_at {live}")
        meta = cur.fetchone()

        ids = sorted({r['org_a'] for r in rows} | {r['org_b'] for r in rows})
        orgs = {}
        if ids:
            cur.execute("""
                SELECT o.org_id, o.name, o.org_type,
                       COUNT(DISTINCT ha.host_id) AS people,
                       ARRAY_REMOVE(ARRAY_AGG(DISTINCT a.alias_name), NULL) AS alias_names
                FROM organizations o
                LEFT JOIN organization_aliases a ON a.org_id = o.org_id
                LEFT JOIN host_affiliations ha ON ha.company_key = a.normalized_name
                WHERE o.org_id = ANY(%s)
                GROUP BY o.org_id
            """, (ids,))
            orgs = {r['org_id']: r for r in cur.fetchall()}
        return {
            "total": meta['total'],
            "computed_at": meta['computed_at'],
            "items": [{**r, 'a': orgs[r['org_a']], 'b': orgs[r['org_b']]} for r in rows],
        }
    finally:
        cur.close()
        conn.close()


def _refresh_suggestions_job():
    conn = get_db_connection()
    try:
        refresh_suggestions(conn)
    except Exception:
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


@app.post("/api/admin/companies-suggestions/refresh", dependencies=[Depends(verify_admin)])
async def refresh_company_merge_suggestions(background_tasks: BackgroundTasks):
    """Rebuild the queue after the response is sent — it takes a minute or two."""
    background_tasks.add_task(_refresh_suggestions_job)
    return {"started": True}


@app.post("/api/admin/people/{host_id}/scan", dependencies=[Depends(verify_admin)])
async def scan_person_episodes(host_id: int):
    """Scan all episodes for an existing person's name and link any matches."""
    try:
        conn = get_db_connection()
        cur  = conn.cursor()

        cur.execute(
            "SELECT first_name, last_name, first_name || ' ' || last_name AS name "
            "FROM hosts WHERE host_id = %s",
            (host_id,)
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Person not found")

        name = row['name']

        links = link_matching_episodes_with_role(cur, name, host_id)
        links += link_matching_episodes_by_first_name(cur, host_id, row['first_name'], row['last_name'])

        conn.commit()
        cur.close()
        conn.close()

        from collections import defaultdict
        by_podcast = defaultdict(int)
        for l in links:
            by_podcast[l['podcast_title']] += 1

        return {
            "success": True,
            "host_id": host_id,
            "name": name,
            "episodes_linked": len(links),
            "by_podcast": dict(by_podcast),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _match_snippet(full_name: str, title: str, description: str) -> str:
    """A short window of text around wherever full_name (or, failing that,
    its last name) turns up in the title or description — not a claim about
    why the credit exists, just enough for a human to see it at a glance
    instead of opening the episode. Plain substring search, not
    word-boundary-safe like name_in_text: a rough hint is fine here even
    if it occasionally lands on a substring match."""
    last_name = full_name.split()[-1] if full_name.split() else full_name
    for text in (title or '', strip_html(description or '')):
        low = text.lower()
        idx = low.find(full_name.lower())
        if idx == -1:
            idx = low.find(last_name.lower())
        if idx == -1:
            continue
        start = max(0, idx - 60)
        end = min(len(text), idx + len(full_name) + 60)
        snippet = text[start:end].strip()
        return snippet
    return ''


@app.get("/api/admin/people/{host_id}/episodes", dependencies=[Depends(verify_admin)])
async def get_person_episodes(host_id: int):
    """Get all episodes a person appears in, grouped by podcast."""
    try:
        conn = get_db_connection()
        cur  = conn.cursor()
        cur.execute("SELECT first_name || ' ' || last_name AS full_name FROM hosts WHERE host_id = %s", (host_id,))
        person = cur.fetchone()
        if not person:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Person not found")
        full_name = person['full_name']

        cur.execute("""
            SELECT p.title AS podcast_title,
                   p.cover_art_url,
                   p.apple_podcast_id,
                   e.episode_id,
                   e.title AS episode_title,
                   e.description,
                   e.published_date,
                   eh.is_guest,
                   eh.data_source
            FROM episode_host eh
            JOIN episodes e ON e.episode_id = eh.episode_id
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE eh.host_id = %s
            ORDER BY p.title, e.published_date DESC
        """, (host_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Group by podcast
        from collections import defaultdict
        by_podcast = defaultdict(list)
        covers = {}
        apple_ids = {}
        for r in rows:
            by_podcast[r['podcast_title']].append({
                'episode_id':    r['episode_id'],
                'episode_title': r['episode_title'],
                'published_date': str(r['published_date']) if r['published_date'] else None,
                'is_guest':      r['is_guest'],
                'data_source':   r['data_source'],
                'snippet':       _match_snippet(full_name, r['episode_title'], r['description']),
            })
            covers[r['podcast_title']] = r['cover_art_url']
            apple_ids[r['podcast_title']] = r['apple_podcast_id']

        return [
            {
                'podcast': show,
                'cover_art_url': covers[show],
                'apple_podcast_id': apple_ids[show],
                'episodes': eps,
                'count': len(eps),
            }
            for show, eps in sorted(by_podcast.items())
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================================================================
# ADMIN ENDPOINTS — Show management
# ==================================================================

class AddShowRequest(BaseModel):
    apple_podcast_id: str


@app.get("/api/admin/shows", dependencies=[Depends(verify_admin)])
async def get_shows():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                pt.apple_podcast_id,
                pt.podcast_title,
                pt.status,
                pt.last_scraped_at,
                pt.error_message,
                pt.total_episodes AS itunes_total_episodes,
                p.cover_art_url,
                COUNT(DISTINCT e.episode_id) AS episode_count,
                MIN(e.published_date) AS earliest_episode_date,
                MAX(e.published_date) AS latest_episode_date,
                COUNT(DISTINCT hp.host_id) AS host_count,
                COUNT(DISTINCT CASE WHEN eh.is_guest = true THEN eh.host_id END) AS guest_count,
                COUNT(DISTINCT CASE WHEN sug.status = 'pending' THEN sug.suggestion_id END) AS pending_suggestion_count
            FROM podcast_tracking pt
            LEFT JOIN podcasts p ON p.apple_podcast_id = pt.apple_podcast_id
            LEFT JOIN episodes e ON e.podcast_id = p.podcast_id
            LEFT JOIN episode_host eh ON eh.episode_id = e.episode_id
            LEFT JOIN host_podcast hp ON hp.podcast_id = p.podcast_id
            LEFT JOIN suggestions sug ON sug.episode_id = e.episode_id
            GROUP BY pt.tracking_id, pt.apple_podcast_id, pt.podcast_title,
                     pt.status, pt.last_scraped_at, pt.error_message, pt.total_episodes,
                     p.cover_art_url
            ORDER BY pt.podcast_title;
        """)
        results = cur.fetchall()
        cur.close()
        conn.close()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/shows", dependencies=[Depends(verify_admin)])
async def add_show(body: AddShowRequest):
    """Add a new show by Apple Podcast ID, mirroring manager.py's `add` command:
    look up the title via iTunes, then queue it as 'pending' for the scraper."""
    apple_id = body.apple_podcast_id.strip()
    if not apple_id:
        raise HTTPException(status_code=400, detail="apple_podcast_id is required")

    title = apple_id
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://itunes.apple.com/lookup",
                params={"id": apple_id, "entity": "podcast", "country": "US"},
            )
            data = resp.json()
            if data.get("resultCount", 0) > 0:
                title = data["results"][0].get("collectionName", apple_id)
    except Exception:
        pass  # fall back to using the ID as the title, same as manager.py

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO podcast_tracking (apple_podcast_id, podcast_title, status, created_at)
            VALUES (%s, %s, 'pending', NOW())
            ON CONFLICT (apple_podcast_id) DO UPDATE
                SET podcast_title = COALESCE(EXCLUDED.podcast_title, podcast_tracking.podcast_title)
        """, (apple_id, title))
        conn.commit()
        cur.close()
        conn.close()
        return {"success": True, "apple_podcast_id": apple_id, "title": title}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/shows/{apple_podcast_id}/scrape-now", dependencies=[Depends(verify_admin)])
async def scrape_show_now(apple_podcast_id: str):
    """Dispatch the scrape workflow scoped to one show (new show's initial
    scrape, or pulling more episodes for an existing one)."""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        raise HTTPException(status_code=503, detail="Scrape-now is not configured on the server")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT podcast_title FROM podcast_tracking WHERE apple_podcast_id = %s", (apple_podcast_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(status_code=404, detail="Show not found")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{SCRAPE_WORKFLOW_FILE}/dispatches",
                headers={
                    "Authorization": f"Bearer {GITHUB_TOKEN}",
                    "Accept": "application/vnd.github+json",
                },
                json={"ref": "main", "inputs": {"podcast_title": row["podcast_title"]}},
            )
        if resp.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"GitHub dispatch failed: {resp.status_code} {resp.text}")
        return {"success": True, "podcast_title": row["podcast_title"]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to dispatch scrape workflow: {e}")


class AddShowHostRequest(BaseModel):
    host_id: int


@app.get("/api/admin/shows/{apple_podcast_id}/hosts", dependencies=[Depends(verify_admin)])
async def get_show_hosts(apple_podcast_id: str):
    """Show-level permanent hosts (host_podcast), distinct from
    per-episode credits (episode_host)."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT h.host_id, h.first_name || ' ' || h.last_name AS name,
                   h.profile_image_url, hp.role, hp.data_source
            FROM host_podcast hp
            JOIN hosts h ON h.host_id = hp.host_id
            JOIN podcasts p ON p.podcast_id = hp.podcast_id
            WHERE p.apple_podcast_id = %s
            ORDER BY h.last_name ASC
        """, (apple_podcast_id,))
        hosts = cur.fetchall()
        cur.close()
        conn.close()
        return hosts
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/shows/{apple_podcast_id}/hosts", dependencies=[Depends(verify_admin)])
async def add_show_host(apple_podcast_id: str, body: AddShowHostRequest):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT podcast_id FROM podcasts WHERE apple_podcast_id = %s", (apple_podcast_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Show not found")

        cur.execute("""
            INSERT INTO host_podcast (host_id, podcast_id, role, data_source)
            VALUES (%s, %s, 'Host', 'manual')
            ON CONFLICT (host_id, podcast_id) DO UPDATE SET role = 'Host', data_source = 'manual'
        """, (body.host_id, row["podcast_id"]))

        # Reconcile existing episode credits: someone who hosts this show was
        # very likely mislabelled "Guest" on its episodes by the name scanner.
        # apple_verified rows are left alone — Apple's explicit per-episode
        # label is more trustworthy than this inference (a host really can
        # appear as a guest on a special episode of their own show).
        cur.execute("""
            UPDATE episode_host eh
            SET is_guest = false, role = 'Host'
            FROM episodes e
            WHERE eh.episode_id = e.episode_id
              AND eh.host_id = %s
              AND e.podcast_id = %s
              AND eh.is_guest = true
              -- 'manual' is a person's deliberate decision and outranks this
              -- inference, same as Apple's own label: a former host really can
              -- return as a guest, and that correction must survive.
              AND eh.data_source NOT IN ('apple_verified', 'manual')
        """, (body.host_id, row["podcast_id"]))
        credits_updated = cur.rowcount

        conn.commit()
        cur.close()
        conn.close()
        return {"success": True, "credits_updated": credits_updated}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/admin/shows/{apple_podcast_id}/hosts/{host_id}", dependencies=[Depends(verify_admin)])
async def remove_show_host(apple_podcast_id: str, host_id: int):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM host_podcast
            WHERE host_id = %s
              AND podcast_id = (SELECT podcast_id FROM podcasts WHERE apple_podcast_id = %s)
        """, (host_id, apple_podcast_id))
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        if deleted == 0:
            raise HTTPException(status_code=404, detail="Host link not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================================================================
# ADMIN ENDPOINTS — Episode management
# ==================================================================

class AddCreditRequest(BaseModel):
    host_id: int
    is_guest: bool = True


class SetNoGuestConfirmedRequest(BaseModel):
    no_guest_confirmed: bool


@app.get("/api/admin/episodes", dependencies=[Depends(verify_admin)])
async def list_episodes(q: str = "", show: str = "", sort: str = "newest", limit: int = 50, offset: int = 0,
                         credit_filter: str = ""):
    """List/search episodes with filtering, sorting, and pagination —
    the episode table is far larger than shows or people, so unlike
    those this can't just return everything and filter client-side.

    credit_filter narrows to episodes missing a specific kind of credit
    entirely ("no_guest"/"no_host"/"no_credit") — the worklist for finding
    real scanner misses to fix, as opposed to browsing everything.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        sort_map = {
            "newest":         "e.published_date DESC NULLS LAST",
            "oldest":         "e.published_date ASC NULLS LAST",
            "credits_desc":   "credit_count DESC",
            "credits_asc":    "credit_count ASC",
            "title_asc":      "e.title ASC",
        }
        order = sort_map.get(sort, "e.published_date DESC NULLS LAST")

        credit_filter_sql = """
              AND (
                %(credit_filter)s = ''
                OR (%(credit_filter)s = 'no_guest' AND NOT e.no_guest_confirmed AND NOT EXISTS (
                    SELECT 1 FROM episode_host eh2 WHERE eh2.episode_id = e.episode_id AND eh2.is_guest))
                OR (%(credit_filter)s = 'no_host' AND NOT EXISTS (
                    SELECT 1 FROM episode_host eh2 WHERE eh2.episode_id = e.episode_id AND NOT eh2.is_guest))
                OR (%(credit_filter)s = 'no_credit' AND NOT EXISTS (
                    SELECT 1 FROM episode_host eh2 WHERE eh2.episode_id = e.episode_id))
                -- count_N: exactly N people credited (Diagnostics' histogram bars).
                OR (%(credit_count)s::int IS NOT NULL AND (
                    SELECT COUNT(*) FROM episode_host eh2 WHERE eh2.episode_id = e.episode_id) = %(credit_count)s::int)
              )
        """
        m = re.fullmatch(r'count_(\d+)', credit_filter or '')
        credit_count = int(m.group(1)) if m else None

        cur.execute(f"""
            SELECT
                e.episode_id, e.title, e.published_date, e.no_guest_confirmed,
                p.podcast_id, p.title AS podcast_title, p.cover_art_url, p.apple_podcast_id,
                COUNT(DISTINCT eh.host_id) AS credit_count,
                COUNT(DISTINCT eh.host_id) FILTER (WHERE NOT eh.is_guest) AS host_count,
                COUNT(DISTINCT eh.host_id) FILTER (WHERE eh.is_guest) AS guest_count,
                (SELECT COUNT(*) FROM suggestions s
                 WHERE s.episode_id = e.episode_id AND s.status = 'pending') AS pending_suggestions
            FROM episodes e
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            LEFT JOIN episode_host eh ON eh.episode_id = e.episode_id
            WHERE (%(q)s = '' OR e.title ILIKE '%%' || %(q)s || '%%' OR p.title ILIKE '%%' || %(q)s || '%%')
              AND (%(show)s = '' OR p.title = %(show)s)
              {credit_filter_sql}
            GROUP BY e.episode_id, e.title, e.published_date, e.no_guest_confirmed, p.podcast_id, p.title, p.cover_art_url, p.apple_podcast_id
            ORDER BY {order}
            LIMIT %(limit)s OFFSET %(offset)s;
        """, {"q": q, "show": show, "limit": limit, "offset": offset, "credit_filter": credit_filter,
              "credit_count": credit_count})
        items = cur.fetchall()

        cur.execute(f"""
            SELECT COUNT(*) AS total
            FROM episodes e
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE (%(q)s = '' OR e.title ILIKE '%%' || %(q)s || '%%' OR p.title ILIKE '%%' || %(q)s || '%%')
              AND (%(show)s = '' OR p.title = %(show)s)
              {credit_filter_sql};
        """, {"q": q, "show": show, "credit_filter": credit_filter, "credit_count": credit_count})
        total = cur.fetchone()["total"]
        cur.execute("SELECT COUNT(*) AS all_total FROM episodes")
        all_total = cur.fetchone()["all_total"]

        cur.close()
        conn.close()
        return {"items": items, "total": total, "all_total": all_total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/episodes/{episode_id}", dependencies=[Depends(verify_admin)])
async def get_episode(episode_id: int):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT e.episode_id, e.title, e.description, e.published_date, e.no_guest_confirmed,
                   p.podcast_id, p.title AS podcast_title, p.cover_art_url, p.apple_podcast_id
            FROM episodes e
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE e.episode_id = %s
        """, (episode_id,))
        episode = cur.fetchone()
        if not episode:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Episode not found")

        cur.execute("""
            SELECT h.host_id, h.first_name || ' ' || h.last_name AS name,
                   h.profile_image_url, eh.is_guest, eh.role, eh.data_source
            FROM episode_host eh
            JOIN hosts h ON h.host_id = eh.host_id
            WHERE eh.episode_id = %s
            ORDER BY eh.is_guest ASC, h.last_name ASC
        """, (episode_id,))
        credits = cur.fetchall()

        cur.execute("""
            SELECT suggestion_id, candidate_name, source
            FROM suggestions
            WHERE episode_id = %s AND status = 'pending'
            ORDER BY created_at
        """, (episode_id,))
        pending_suggestions = cur.fetchall()

        cur.close()
        conn.close()
        return {**episode, "credits": credits, "pending_suggestions": pending_suggestions}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/episodes/{episode_id}/no_guest", dependencies=[Depends(verify_admin)])
async def set_no_guest_confirmed(episode_id: int, body: SetNoGuestConfirmedRequest):
    """Record a human's statement that this episode genuinely has no guest —
    see migrate_add_no_guest_confirmed.sql for why this needs to be distinct
    from "the scanner hasn't found one yet"."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE episodes SET no_guest_confirmed = %s WHERE episode_id = %s RETURNING episode_id",
            (body.no_guest_confirmed, episode_id),
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Episode not found")
        conn.commit()
        cur.close()
        conn.close()
        return {"success": True, "episode_id": episode_id, "no_guest_confirmed": body.no_guest_confirmed}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/episodes/{episode_id}/credits", dependencies=[Depends(verify_admin)])
async def add_episode_credit(episode_id: int, body: AddCreditRequest):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Adding a credit by hand overrides an earlier removal, so lift the
        # suppression first — otherwise the trigger silently drops this insert
        # and the admin gets a success with nothing written.
        cur.execute(
            "DELETE FROM credit_suppressions WHERE episode_id = %s AND host_id = %s",
            (episode_id, body.host_id),
        )
        cur.execute("""
            INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
            VALUES (%s, %s, %s, %s, 'manual')
            ON CONFLICT (episode_id, host_id) DO UPDATE
                SET is_guest = EXCLUDED.is_guest, data_source = 'manual'
        """, (episode_id, body.host_id, body.is_guest, 'Guest' if body.is_guest else 'Host'))
        conn.commit()
        cur.close()
        conn.close()
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/admin/episodes/{episode_id}/credits/{host_id}", dependencies=[Depends(verify_admin)])
async def remove_episode_credit(episode_id: int, host_id: int):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM episode_host WHERE episode_id = %s AND host_id = %s",
            (episode_id, host_id),
        )
        deleted = cur.rowcount
        # The delete alone does not hold: the next scan re-reads the same
        # description, derives the same name and puts the credit back. Record
        # the removal so the trigger on episode_host keeps refusing it.
        if deleted:
            cur.execute(
                """INSERT INTO credit_suppressions (episode_id, host_id, reason)
                   VALUES (%s, %s, 'removed via admin')
                   ON CONFLICT (episode_id, host_id) DO NOTHING""",
                (episode_id, host_id),
            )
        conn.commit()
        cur.close()
        conn.close()
        if deleted == 0:
            raise HTTPException(status_code=404, detail="Credit not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
