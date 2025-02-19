import requests
import psycopg2
from datetime import datetime
import logging
import json
import time
import feedparser
import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PodcastScraper:
    def __init__(self, db_connection_string):
        self.conn = psycopg2.connect(db_connection_string)
        self.cursor = self.conn.cursor()
        self.base_url = "https://itunes.apple.com"

    def fetch_itunes_data(self, podcast_id: str) -> Optional[Dict]:
        """Fetch podcast and episode data from iTunes API"""
        # Get podcast metadata
        url = f"{self.base_url}/lookup"
        params = {
            'id': podcast_id,
            'entity': 'podcast',
            'country': 'US'
        }
        
        logger.info(f"Fetching podcast data from iTunes API for ID: {podcast_id}")
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch podcast data: {response.status_code}")
            
        podcast_data = response.json()
        #logger.info(f"iTunes API Response:\n{json.dumps(podcast_data, indent=4)}")

        if podcast_data.get('resultCount', 0) == 0:
            raise Exception("No podcast found with this ID")
            
        # Get episodes
        params['entity'] = 'podcastEpisode'
        params['limit'] = 50 # EF temp changed to 50, normally 10
        
        #logger.info("Fetching episode data from iTunes API")
        response = requests.get(url, params=params)
        episodes_data = response.json()
        
        return {
            'podcast': podcast_data['results'][0],
            'episodes': episodes_data['results'][1:] if episodes_data.get('resultCount', 0) > 1 else []
        }

    def parse_rss_feed(self, feed_url: str) -> Dict:
        """Parse RSS feed for detailed episode and credit information"""
        logger.info(f"PARSING RSS EPISODE FEED: {feed_url}")
        
        feed = feedparser.parse(feed_url)
        feed_data = {
            'episodes': []
        }

        # Process each episode
        for entry in feed.entries:
            episode = {
                'title': entry.title,
                'description': entry.description if hasattr(entry, 'description') else '',
                'published_date': datetime(*entry.published_parsed[:6]).date() if hasattr(entry, 'published_parsed') else None,
                'duration': entry.get('itunes_duration', ''),
                'episode_number': entry.get('itunes_episode', None),
                'season_number': entry.get('itunes_season', None),
                'author': entry.get('author', ''),
                'itunes_author': entry.get('itunes_author', ''),
                'link': entry.get('link', '')
            }
            feed_data['episodes'].append(episode)

        return feed_data


    def get_or_create_channel(self, channel_name: str) -> int:
        """Get channel ID or create new channel"""
        if not channel_name:
            return None
            
        query = """
        INSERT INTO channels (name)
        VALUES (%s)
        ON CONFLICT (name) DO UPDATE 
        SET name = EXCLUDED.name
        RETURNING channel_id
        """
        self.cursor.execute(query, (channel_name,))
        return self.cursor.fetchone()[0]

    def get_or_create_genre(self, genre_name: str, apple_genre_id: str = None) -> int:
        """Get genre ID or create new genre"""
        if not genre_name:
            return None
            
        query = """
        INSERT INTO genres (name, apple_genre_id)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE 
        SET apple_genre_id = COALESCE(EXCLUDED.apple_genre_id, genres.apple_genre_id)
        RETURNING genre_id
        """
        self.cursor.execute(query, (genre_name, apple_genre_id))
        return self.cursor.fetchone()[0]

    def link_podcast_genres(self, podcast_id: int, genre_data: Dict):
        """Link podcast to its genres"""
        # Clear existing genre relationships
        self.cursor.execute("""
            DELETE FROM podcast_genres
            WHERE podcast_id = %s
        """, (podcast_id,))
        
        # Add primary genre
        primary_genre_name = genre_data.get('primaryGenreName')
        if primary_genre_name:
            primary_genre_id = self.get_or_create_genre(
                primary_genre_name,
                next((gid for gn, gid in zip(genre_data.get('genres', []), 
                                           genre_data.get('genreIds', [])) 
                     if gn == primary_genre_name), None)
            )
            
            self.cursor.execute("""
                INSERT INTO podcast_genres (podcast_id, genre_id, is_primary)
                VALUES (%s, %s, true)
            """, (podcast_id, primary_genre_id))
    
        # Add additional genres
        for genre_name, genre_id in zip(genre_data.get('genres', []), 
                                      genre_data.get('genreIds', [])):
            if genre_name != primary_genre_name and genre_name != 'Podcasts':
                genre_db_id = self.get_or_create_genre(genre_name, genre_id)
                self.cursor.execute("""
                    INSERT INTO podcast_genres (podcast_id, genre_id, is_primary)
                    VALUES (%s, %s, false)
                    ON CONFLICT (podcast_id, genre_id) DO NOTHING
                """, (podcast_id, genre_db_id))

    def insert_podcast(self, podcast_data: Dict) -> int:
        """Insert podcast data into database"""
        try:
            # First, get or create channel
            channel_name = podcast_data.get('artistName')
            channel_id = self.get_or_create_channel(channel_name)
            
            # Then insert podcast
            query = """
            INSERT INTO podcasts (
                title, description, cover_art_url, website_url, 
                language, rss_feed_url, apple_podcast_id, channel_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (title) DO UPDATE 
            SET 
                description = EXCLUDED.description,
                cover_art_url = EXCLUDED.cover_art_url,
                website_url = EXCLUDED.website_url,
                language = EXCLUDED.language,
                rss_feed_url = EXCLUDED.rss_feed_url,
                apple_podcast_id = EXCLUDED.apple_podcast_id,
                channel_id = EXCLUDED.channel_id
            RETURNING podcast_id
            """
            
            values = (
                podcast_data.get('collectionName'),
                podcast_data.get('description', ''),
                podcast_data.get('artworkUrl600'),
                podcast_data.get('collectionViewUrl'),
                podcast_data.get('languageCode', 'en'),
                podcast_data.get('feedUrl'),
                podcast_data.get('trackId'),
                channel_id
            )
            
            self.cursor.execute(query, values)
            podcast_id = self.cursor.fetchone()[0]
            
            # Link genres
            self.link_podcast_genres(podcast_id, podcast_data)
            
            logger.info(f"Inserted podcast with ID: {podcast_id}")
            return podcast_id
            
        except Exception as e:
            logger.error(f"Error inserting podcast: {str(e)}")
            raise

    def insert_episode(self, episode_data: Dict, podcast_id: int, rss_data: Optional[Dict] = None) -> int:
        """Insert episode into database"""
        query = """
        INSERT INTO episodes (
            podcast_id, title, description, audio_url, 
            duration_seconds, published_date, episode_number,
            season_number, apple_episode_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (podcast_id, title) DO UPDATE 
        SET 
            description = EXCLUDED.description,
            audio_url = EXCLUDED.audio_url,
            duration_seconds = EXCLUDED.duration_seconds,
            published_date = EXCLUDED.published_date,
            episode_number = EXCLUDED.episode_number,
            season_number = EXCLUDED.season_number,
            apple_episode_id = EXCLUDED.apple_episode_id
        RETURNING episode_id
        """
        
        # Convert duration string to seconds if from RSS
        duration = rss_data.get('duration', '') if rss_data else episode_data.get('trackTimeMillis', 0)
        if isinstance(duration, str):
            try:
                parts = duration.split(':')
                if len(parts) == 3:
                    duration = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                elif len(parts) == 2:
                    duration = int(parts[0]) * 60 + int(parts[1])
                else:
                    duration = int(duration)
            except (ValueError, IndexError):
                duration = None
        else:
            duration = duration // 1000 if duration else None
        
        values = (
            podcast_id,
            episode_data.get('trackName') or episode_data.get('title'),
            episode_data.get('description', ''),
            episode_data.get('episodeUrl') or episode_data.get('link'),
            duration,
            episode_data.get('published_date') or datetime.strptime(
                episode_data.get('releaseDate', ''), 
                '%Y-%m-%dT%H:%M:%SZ'
            ).date() if episode_data.get('releaseDate') else None,
            episode_data.get('episode_number'),
            episode_data.get('season_number'),
            episode_data.get('trackId')
        )
        
        self.cursor.execute(query, values)
        episode_id = self.cursor.fetchone()[0]
        return episode_id

    def process_podcast(self, apple_podcast_id: str):
        """Main method to process a podcast"""
        try:
            # Fetch data from iTunes API
            itunes_data = self.fetch_itunes_data(apple_podcast_id)
            
            # Get RSS feed URL
            feed_url = itunes_data['podcast'].get('feedUrl')
            if not feed_url:
                print ("Could not find RSS feed URL")
                raise ValueError("Could not find RSS feed URL")
            
            # Parse RSS feed
            rss_data = self.parse_rss_feed(feed_url)
            
            # Insert podcast
            podcast_id = self.insert_podcast(itunes_data['podcast'])

            # Process episodes
            processed = 0
            failed = 0
            
            for itunes_episode in itunes_data['episodes']:
                try:
                    # Find matching RSS episode
                    #print(itunes_data['episodes'])
                    #logger.info(f"Episode INFO:\n{json.dumps(itunes_episode, indent=5)}")

                    rss_episode = next(
                        (e for e in rss_data['episodes'] 
                         if e['title'] == itunes_episode['trackName']),
                        None
                    )
                    
                    # Insert episode
                    episode_id = self.insert_episode(itunes_episode, podcast_id, rss_episode)
          
                    processed += 1
                    logger.info(f"Processed episode: {itunes_episode['trackName']}")
                    
                except Exception as e:
                    logger.error(f"Error processing episode {itunes_episode.get('trackName', 'Unknown')}: {str(e)}")
                    failed += 1
            
            self.conn.commit()
            
            # Print statistics
            self.cursor.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM episodes WHERE podcast_id = %s) as episode_count,
                    (SELECT COUNT(DISTINCT host_id) FROM episode_host eh 
                     JOIN episodes e ON eh.episode_id = e.episode_id 
                     WHERE e.podcast_id = %s) as person_count,
                    (SELECT COUNT(*) FROM episode_host eh 
                     JOIN episodes e ON eh.episode_id = e.episode_id 
                     WHERE e.podcast_id = %s AND eh.is_guest = true) as guest_appearances,
                    (SELECT COUNT(*) FROM episode_host eh 
                     JOIN episodes e ON eh.episode_id = e.episode_id 
                     WHERE e.podcast_id = %s AND eh.is_guest = false) as host_appearances
            """, [podcast_id] * 4)
            
            stats = self.cursor.fetchone()
            print() # new line for prettier printing
            logger.info(f"Stats for Podcast: {itunes_data['podcast'].get('trackName')}")
            logger.info(f"Episodes processed: {stats[0]}")
            logger.info(f"Unique people found: {stats[1]}")
            logger.info(f"Guest appearances: {stats[2]}")
            logger.info(f"Host appearances: {stats[3]}")
            
            return {
                'podcast_id': podcast_id,
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

# Example usage
if __name__ == "__main__":
    db_connection = "postgresql://localhost/podcast_db"
    podcast_id = "1200361736"  # Replace with your podcast ID
    
    scraper = PodcastScraper(db_connection)
    try:
        stats = scraper.process_podcast(podcast_id)
        print(f"\nProcessed {stats['processed_episodes']} episodes")
        print(f"Failed to process {stats['failed_episodes']} episodes")
    except Exception as e:
        print(f"Error: {str(e)}")