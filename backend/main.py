from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Configure CORS to allow requests from your React app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React app address
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "podcast_db"),
        user=os.getenv("DB_USER", "postgres"),
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
                    COUNT(DISTINCT e.episode_id) as episodes_together
                FROM hosts h1
                JOIN episode_host eh1 ON h1.host_id = eh1.host_id
                JOIN episodes e ON eh1.episode_id = e.episode_id
                JOIN podcasts p ON e.podcast_id = p.podcast_id
                JOIN channels c1 ON p.channel_id = c1.channel_id
                JOIN podcast_genres pg ON p.podcast_id = pg.podcast_id AND pg.is_primary = true
                JOIN genres g1 ON pg.genre_id = g1.genre_id
                LEFT JOIN host_podcast hp1 ON h1.host_id = hp1.host_id AND p.podcast_id = hp1.podcast_id
                JOIN episode_host eh2 ON e.episode_id = eh2.episode_id
                JOIN hosts h2 ON eh2.host_id = h2.host_id
                LEFT JOIN host_podcast hp2 ON h2.host_id = hp2.host_id AND p.podcast_id = hp2.podcast_id
                WHERE h1.host_id < h2.host_id
                GROUP BY 
                    h1.host_id, h1.first_name, h1.last_name, h1.profile_image_url,
                    hp1.role, c1.name, g1.name,
                    h2.host_id, h2.first_name, h2.last_name, h2.profile_image_url,
                    hp2.role, p.title
                --AVING COUNT(DISTINCT e.episode_id) > 1
            )
            SELECT 
                source_id,
                source_name,
                source_image,
                source_role,
                source_channel,
                source_genre,
                target_id,
                target_name,
                target_image,
                target_role,
                target_channel,
                target_genre,
                podcast_title,
                episodes_together
            FROM host_connections
            ORDER BY episodes_together DESC;
        """)
        
        results = cur.fetchall()
        return results
        
    except Exception as e:
        print (str(e))
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)