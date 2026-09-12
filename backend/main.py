# TO RUN LOCALLY
# switch to bash in the /podcast-network/backend/ folder
# source ~/.bash_profile
# python3 -m uvicorn main:app --reload

import secrets

from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import httpx
from urllib.parse import urlparse
from dotenv import load_dotenv

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
ALLOWED_IMAGE_HOST_SUFFIXES = ('mzstatic.com', 'unavatar.io', 'bsky.app')


def is_allowed_image_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if parsed.scheme not in ('http', 'https'):
        return False
    host = (parsed.hostname or '').lower()
    return any(host == suffix or host.endswith('.' + suffix) for suffix in ALLOWED_IMAGE_HOST_SUFFIXES)

app = FastAPI()


class NameOverrideRequest(BaseModel):
    name: str = None

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
            WITH host_connections AS (
                SELECT
                    h1.host_id as source_id,
                    h1.first_name || ' ' || h1.last_name as source_name,
                    h1.profile_image_url as source_image,
                    hp1.role as source_role,
                    c1.name as source_channel,
                    g1.name as source_genre,
                    h2.host_id as target_id,
                    h2.first_name || ' ' || h2.last_name as target_name,
                    h2.profile_image_url as target_image,
                    hp2.role as target_role,
                    c1.name as target_channel,
                    g1.name as target_genre,
                    p.title as podcast_title,
                    p.focus_area,
                    p.target_audience,
                    COUNT(DISTINCT e.episode_id) as episodes_together
                FROM hosts h1
                JOIN episode_host eh1 ON h1.host_id = eh1.host_id
                JOIN episodes e ON eh1.episode_id = e.episode_id
                JOIN podcasts p ON e.podcast_id = p.podcast_id
                LEFT JOIN channels c1 ON p.channel_id = c1.channel_id
                LEFT JOIN podcast_genres pg ON p.podcast_id = pg.podcast_id AND pg.is_primary = true
                LEFT JOIN genres g1 ON pg.genre_id = g1.genre_id
                LEFT JOIN host_podcast hp1 ON h1.host_id = hp1.host_id AND p.podcast_id = hp1.podcast_id
                JOIN episode_host eh2 ON e.episode_id = eh2.episode_id
                JOIN hosts h2 ON eh2.host_id = h2.host_id
                LEFT JOIN host_podcast hp2 ON h2.host_id = hp2.host_id AND p.podcast_id = hp2.podcast_id
                WHERE h1.host_id < h2.host_id
                GROUP BY
                    h1.host_id, h1.first_name, h1.last_name, h1.profile_image_url,
                    hp1.role, c1.name, g1.name,
                    h2.host_id, h2.first_name, h2.last_name, h2.profile_image_url,
                    hp2.role, p.title, p.focus_area, p.target_audience
            )
            SELECT *
            FROM host_connections
            ORDER BY episodes_together DESC;
        """)

        results = cur.fetchall()
        cur.close()
        conn.close()
        return results

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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


# ==================================================================
# ADMIN ENDPOINTS — Suggestions queue
# ==================================================================

@app.get("/api/admin/suggestions/next", dependencies=[Depends(verify_admin)])
async def get_next_suggestion():
    """
    Get the next pending suggestion for review.
    Returns the suggestion with full episode context.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
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
                (SELECT COUNT(*) FROM suggestions WHERE status = 'pending') AS total_pending
            FROM suggestions s
            JOIN episodes e ON s.episode_id = e.episode_id
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE s.status = 'pending'
            ORDER BY s.created_at ASC
            LIMIT 1
        """)

        row = cur.fetchone()

        if not row:
            cur.close()
            conn.close()
            return {"done": True, "message": "No more suggestions to review!"}

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

        cur.close()
        conn.close()

        return {
            "done": False,
            **row,
            "existing_credits": existing_credits,
            "other_pending_suggestions": other_count,
            "other_pending_names": other_pending_names,
        }

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
        cur.execute("""
            INSERT INTO hosts (first_name, last_name, data_source, created_at)
            VALUES (%s, %s, 'approved_suggestion', NOW())
            ON CONFLICT (first_name, last_name) DO UPDATE
                SET first_name = EXCLUDED.first_name
            RETURNING host_id
        """, (first_name, last_name))
        host_id = cur.fetchone()['host_id']

        # 2. Link to the source episode
        cur.execute("""
            INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
            VALUES (%s, %s, true, 'Guest', %s)
            ON CONFLICT (episode_id, host_id) DO NOTHING
        """, (episode_id, host_id, source))

        # 3. Scan ALL episodes for this name
        cur.execute("""
            SELECT e.episode_id, e.title, e.description,
                   p.podcast_id, p.title AS podcast_title
            FROM episodes e
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE e.episode_id != %s
        """, (episode_id,))
        all_episodes = cur.fetchall()

        additional_links = []
        name_lower = name.lower()

        for ep in all_episodes:
            matched_source = None
            title = (ep['title'] or '').lower()
            desc  = (ep['description'] or '').lower()

            if name_lower in title:
                matched_source = 'parsed_title'
            elif name_lower in desc:
                matched_source = 'parsed_desc'

            if matched_source:
                cur.execute("""
                    INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
                    VALUES (%s, %s, true, 'Guest', %s)
                    ON CONFLICT (episode_id, host_id) DO NOTHING
                """, (ep['episode_id'], host_id, matched_source))
                if cur.rowcount > 0:
                    additional_links.append({
                        'podcast': ep['podcast_title'],
                        'episode': ep['title'],
                        'source':  matched_source,
                    })

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
            by_podcast[link['podcast']] += 1

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
        cur.execute("""
            INSERT INTO hosts (first_name, last_name, data_source, created_at)
            VALUES (%s, %s, 'approved_suggestion', NOW())
            ON CONFLICT (first_name, last_name) DO UPDATE
                SET first_name = EXCLUDED.first_name
            RETURNING host_id
        """, (first_name, last_name))
        host_id = cur.fetchone()['host_id']

        # Scan ALL episodes EXCEPT the source episode
        cur.execute("""
            SELECT e.episode_id, e.title, e.description,
                   p.podcast_id, p.title AS podcast_title
            FROM episodes e
            JOIN podcasts p ON e.podcast_id = p.podcast_id
            WHERE e.episode_id != %s
        """, (episode_id,))
        all_episodes = cur.fetchall()

        additional_links = []
        name_lower = name.lower()

        for ep in all_episodes:
            matched_source = None
            title = (ep['title'] or '').lower()
            desc  = (ep['description'] or '').lower()
            if name_lower in title:
                matched_source = 'parsed_title'
            elif name_lower in desc:
                matched_source = 'parsed_desc'
            if matched_source:
                cur.execute("""
                    INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
                    VALUES (%s, %s, true, 'Guest', %s)
                    ON CONFLICT (episode_id, host_id) DO NOTHING
                """, (ep['episode_id'], host_id, matched_source))
                if cur.rowcount > 0:
                    additional_links.append({'podcast': ep['podcast_title']})

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
            by_podcast[l['podcast']] += 1

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
    twitter_url: str = None
    bluesky_url: str = None
    linkedin_url: str = None


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

        # Check if already exists
        cur.execute(
            "SELECT host_id FROM hosts WHERE first_name = %s AND last_name = %s",
            (first_name, last_name)
        )
        existing = cur.fetchone()
        if existing:
            cur.close()
            conn.close()
            raise HTTPException(status_code=409, detail=f"{full_name} already exists (host_id={existing['host_id']})")

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

        # Scan all episodes for name matches
        cur.execute("""
            SELECT e.episode_id, e.title, e.description, p.title AS podcast_title
            FROM episodes e
            JOIN podcasts p ON e.podcast_id = p.podcast_id
        """)
        all_episodes = cur.fetchall()

        name_lower = full_name.lower()
        links = []

        for ep in all_episodes:
            matched_source = None
            if name_lower in (ep['title'] or '').lower():
                matched_source = 'parsed_title'
            elif name_lower in (ep['description'] or '').lower():
                matched_source = 'parsed_desc'

            if matched_source:
                cur.execute("""
                    INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
                    VALUES (%s, %s, true, 'Guest', %s)
                    ON CONFLICT (episode_id, host_id) DO NOTHING
                """, (ep['episode_id'], host_id, matched_source))
                if cur.rowcount > 0:
                    links.append({'podcast': ep['podcast_title'], 'episode': ep['title']})

        conn.commit()
        cur.close()
        conn.close()

        from collections import defaultdict
        by_podcast = defaultdict(int)
        for l in links:
            by_podcast[l['podcast']] += 1

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


@app.get("/api/admin/people", dependencies=[Depends(verify_admin)])
async def list_people(q: str = "", filter: str = "all", sort: str = "appearances_desc"):
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
        }
        order = sort_map.get(sort, "appearances DESC, h.last_name ASC")

        extra_where  = ""
        having_clause = ""
        if filter == "zero":
            having_clause = "HAVING COUNT(DISTINCT eh.episode_id) = 0"
        elif filter == "parsed":
            extra_where = "AND h.data_source IN ('parsed_desc','parsed_title','approved_suggestion')"
        elif filter == "no_image":
            extra_where = "AND h.profile_image_url IS NULL"

        cur.execute(f"""
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
            WHERE (%(q)s = '' OR (h.first_name || ' ' || h.last_name) ILIKE '%%' || %(q)s || '%%')
            {extra_where}
            GROUP BY h.host_id, h.first_name, h.last_name, h.profile_image_url,
                     h.twitter_handle, h.bluesky_handle, h.data_source, h.created_at
            {having_clause}
            ORDER BY {order}
            LIMIT 100
        """, {"q": q})
        rows = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*) FROM hosts
            WHERE (%(q)s = '' OR (first_name || ' ' || last_name) ILIKE '%%' || %(q)s || '%%')
        """, {"q": q})
        total = cur.fetchone()['count']

        cur.close()
        conn.close()
        return {"items": list(rows), "total": total}
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

        # If name changed: clear parsed links, then re-scan with new name
        if name_changed:
            cur.execute("""
                DELETE FROM episode_host
                WHERE host_id = %s AND data_source IN ('parsed_desc', 'parsed_title', 'approved_suggestion')
            """, (host_id,))

        conn.commit()

        # Re-scan with new name
        cur.execute("""
            SELECT e.episode_id, e.title, e.description, p.title AS podcast_title
            FROM episodes e JOIN podcasts p ON e.podcast_id = p.podcast_id
        """)
        all_episodes = cur.fetchall()

        name_lower = full_name.lower()
        links = []
        for ep in all_episodes:
            matched_source = None
            if name_lower in (ep['title'] or '').lower():
                matched_source = 'parsed_title'
            elif name_lower in (ep['description'] or '').lower():
                matched_source = 'parsed_desc'
            if matched_source:
                cur.execute("""
                    INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
                    VALUES (%s, %s, true, 'Guest', %s)
                    ON CONFLICT (episode_id, host_id) DO NOTHING
                """, (ep['episode_id'], host_id, matched_source))
                if cur.rowcount > 0:
                    links.append({'podcast': ep['podcast_title']})

        conn.commit()
        cur.close()
        conn.close()

        from collections import defaultdict
        by_podcast = defaultdict(int)
        for l in links:
            by_podcast[l['podcast']] += 1

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


@app.post("/api/admin/people/{host_id}/scan", dependencies=[Depends(verify_admin)])
async def scan_person_episodes(host_id: int):
    """Scan all episodes for an existing person's name and link any matches."""
    try:
        conn = get_db_connection()
        cur  = conn.cursor()

        cur.execute(
            "SELECT first_name || ' ' || last_name AS name FROM hosts WHERE host_id = %s",
            (host_id,)
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Person not found")

        name       = row['name']
        name_lower = name.lower()

        cur.execute("""
            SELECT e.episode_id, e.title, e.description, p.title AS podcast_title
            FROM episodes e JOIN podcasts p ON e.podcast_id = p.podcast_id
        """)
        all_episodes = cur.fetchall()

        links = []
        for ep in all_episodes:
            matched_source = None
            if name_lower in (ep['title'] or '').lower():
                matched_source = 'parsed_title'
            elif name_lower in (ep['description'] or '').lower():
                matched_source = 'parsed_desc'
            if matched_source:
                cur.execute("""
                    INSERT INTO episode_host (episode_id, host_id, is_guest, role, data_source)
                    VALUES (%s, %s, true, 'Guest', %s)
                    ON CONFLICT (episode_id, host_id) DO NOTHING
                """, (ep['episode_id'], host_id, matched_source))
                if cur.rowcount > 0:
                    links.append({'podcast': ep['podcast_title']})

        conn.commit()
        cur.close()
        conn.close()

        from collections import defaultdict
        by_podcast = defaultdict(int)
        for l in links:
            by_podcast[l['podcast']] += 1

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


@app.get("/api/admin/people/{host_id}/episodes", dependencies=[Depends(verify_admin)])
async def get_person_episodes(host_id: int):
    """Get all episodes a person appears in, grouped by podcast."""
    try:
        conn = get_db_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT p.title AS podcast_title,
                   p.cover_art_url,
                   e.episode_id,
                   e.title AS episode_title,
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
        for r in rows:
            by_podcast[r['podcast_title']].append({
                'episode_id':    r['episode_id'],
                'episode_title': r['episode_title'],
                'published_date': str(r['published_date']) if r['published_date'] else None,
                'is_guest':      r['is_guest'],
                'data_source':   r['data_source'],
            })
            covers[r['podcast_title']] = r['cover_art_url']

        return [
            {
                'podcast': show,
                'cover_art_url': covers[show],
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
                COUNT(DISTINCT e.episode_id) AS episode_count
            FROM podcast_tracking pt
            LEFT JOIN podcasts p ON p.apple_podcast_id = pt.apple_podcast_id
            LEFT JOIN episodes e ON e.podcast_id = p.podcast_id
            GROUP BY pt.tracking_id, pt.apple_podcast_id, pt.podcast_title,
                     pt.status, pt.last_scraped_at, pt.error_message, pt.total_episodes
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
