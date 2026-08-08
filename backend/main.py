# TO RUN LOCALLY
# switch to bash in the /podcast-network/backend/ folder
# source ~/.bash_profile
# python3 -m uvicorn main:app --reload

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "https://coloradocurrent.com",
        "https://www.coloradocurrent.com",
    ],
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
