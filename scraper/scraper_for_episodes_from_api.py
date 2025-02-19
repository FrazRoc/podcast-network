import requests
import psycopg2
from datetime import datetime
import logging
import json
import time
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ApplePodcastScraper:
    def __init__(self, db_connection_string):
        """Initialize the scraper with database connection details"""
        self.conn = psycopg2.connect(db_connection_string)
        self.cursor = self.conn.cursor()
        self.base_url = "https://itunes.apple.com"
        
    def fetch_podcast_data(self, podcast_id: str) -> Optional[Dict]:
        """Fetch podcast metadata using iTunes Lookup API"""
        url = f"{self.base_url}/lookup"
        params = {
            'id': podcast_id,
            'entity': 'podcast',
            'country': 'US'
        }
        
        logger.info(f"Fetching podcast data for ID: {podcast_id}")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('resultCount', 0) > 0:
                return data['results'][0]
        return None

    def fetch_podcast_episodes(self, podcast_id: str) -> List[Dict]:
        """Fetch episodes using iTunes Lookup API"""
        url = f"{self.base_url}/lookup"
        params = {
            'id': podcast_id,
            'entity': 'podcastEpisode',
            'limit': 200,  # Maximum allowed by API
            'country': 'US'
        }
        
        logger.info(f"Fetching episodes for podcast ID: {podcast_id}")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            # First result is podcast info, rest are episodes
            return data['results'][1:] if data.get('resultCount', 0) > 1 else []
        return []

    def insert_podcast(self, podcast_data: Dict) -> int:
        """Insert podcast data into database"""
        query = """
        INSERT INTO podcasts (
            title, description, cover_art_url, website_url, 
            language, category
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (title) DO UPDATE 
        SET 
            description = EXCLUDED.description,
            cover_art_url = EXCLUDED.cover_art_url,
            website_url = EXCLUDED.website_url,
            language = EXCLUDED.language,
            category = EXCLUDED.category
        RETURNING podcast_id
        """
        
        values = (
            podcast_data.get('collectionName'),
            podcast_data.get('description', ''),
            podcast_data.get('artworkUrl600'),
            podcast_data.get('collectionViewUrl'),
            podcast_data.get('languageCode', 'en'),
            podcast_data.get('primaryGenreName')
        )
        
        self.cursor.execute(query, values)
        podcast_id = self.cursor.fetchone()[0]
        logger.info(f"Inserted/updated podcast with ID: {podcast_id}")
        return podcast_id

    def insert_episode(self, episode_data: Dict, podcast_id: int) -> int:
        """Insert episode data into database"""
        print(episode_data)
        query = """
        INSERT INTO episodes (
            podcast_id, title, description, audio_url, 
            duration_seconds, published_date
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (podcast_id, title) DO UPDATE 
        SET 
            description = EXCLUDED.description,
            audio_url = EXCLUDED.audio_url,
            duration_seconds = EXCLUDED.duration_seconds,
            published_date = EXCLUDED.published_date
        RETURNING episode_id
        """
        
        # Convert duration from milliseconds to seconds
        duration_ms = episode_data.get('trackTimeMillis', 0)
        duration_seconds = duration_ms // 1000 if duration_ms else None
        
        # Parse release date
        release_date = None
        if 'releaseDate' in episode_data:
            try:
                release_date = datetime.strptime(
                    episode_data['releaseDate'], 
                    '%Y-%m-%dT%H:%M:%SZ'
                ).date()
            except ValueError:
                logger.warning(f"Could not parse release date: {episode_data['releaseDate']}")
        
        values = (
            podcast_id,
            episode_data.get('trackName'),
            episode_data.get('description', ''),
            episode_data.get('episodeUrl'),  # This is the audio URL
            duration_seconds,
            release_date
        )
        
        self.cursor.execute(query, values)
        episode_id = self.cursor.fetchone()[0]
        logger.info(f"Inserted/updated episode with ID: {episode_id}")
        return episode_id

    def process_podcast(self, apple_podcast_id: str) -> Dict:
        """Process a podcast and all its episodes"""
        try:
            # Fetch podcast data
            podcast_data = self.fetch_podcast_data(apple_podcast_id)
            if not podcast_data:
                raise ValueError(f"Could not fetch podcast data for ID: {apple_podcast_id}")
            
            # Insert podcast into database
            db_podcast_id = self.insert_podcast(podcast_data)
            
            # Fetch all episodes
            episodes = self.fetch_podcast_episodes(apple_podcast_id)
            logger.info(f"Found {len(episodes)} episodes")
            
            processed = 0
            failed = 0
            
            # Process each episode
            for episode in episodes:
                try:
                    self.insert_episode(episode, db_podcast_id)
                    processed += 1
                    # Small delay to be nice to the API
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error processing episode: {str(e)}")
                    failed += 1
            
            self.conn.commit()
            logger.info(f"Successfully processed {processed} episodes")
            logger.info(f"Failed to process {failed} episodes")
            
            return {
                'podcast_id': db_podcast_id,
                'processed_episodes': processed,
                'failed_episodes': failed
            }
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error processing podcast: {str(e)}")
            raise e
        finally:
            self.cursor.close()
            self.conn.close()

# Example usage:
if __name__ == "__main__":
    # Replace with your database connection string
    db_connection = "postgresql://localhost/podcast_db"
    
    # Extract podcast ID from Apple Podcasts URL
    # e.g., from https://podcasts.apple.com/us/podcast/id1200361736
    # podcast_id would be "1200361736"
    podcast_id = "1200361736"
    
    scraper = ApplePodcastScraper(db_connection)
    try:
        results = scraper.process_podcast(podcast_id)
        print(f"Successfully processed {results['processed_episodes']} episodes")
        print(f"Failed to process {results['failed_episodes']} episodes")
    except Exception as e:
        print(f"Error occurred: {str(e)}")