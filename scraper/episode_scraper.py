import requests
from bs4 import BeautifulSoup
import psycopg2
import time
import logging
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EpisodeHostScraper:
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }

    def get_episodes_to_process(self) -> List[Dict]:
        """Get episodes that have apple_episode_id but no host info"""
        conn = psycopg2.connect(self.db_connection_string)
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT 
                    e.episode_id,
                    e.apple_episode_id,
                    p.apple_podcast_id,
                    e.title
                FROM episodes e
                JOIN podcasts p ON e.podcast_id = p.podcast_id
                LEFT JOIN episode_host eh ON e.episode_id = eh.episode_id
                WHERE e.apple_episode_id IS NOT NULL
                AND eh.episode_id IS NULL
                AND p.apple_podcast_id IS NOT NULL
            """)
            
            episodes = [
                {
                    'episode_id': row[0],
                    'apple_episode_id': row[1],
                    'apple_podcast_id': row[2],
                    'title': row[3]
                }
                for row in cur.fetchall()
            ]
            
            return episodes
            
        finally:
            cur.close()
            conn.close()

    def scrape_episode_page(self, apple_podcast_id: str, apple_episode_id: str) -> Optional[Dict]:
        """Scrape host and guest information from episode page"""
        url = f"https://podcasts.apple.com/us/podcast/id{apple_podcast_id}?i={apple_episode_id}"
        
        try:
            #logger.info(f"Scraping episode page: {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            people = []
            
            # Look for the credits section
            credits_section = soup.find('ul', class_='shelf-grid__list')
            if credits_section:

                for person_div in credits_section.find_all('li'): #, recursive=False):

                    name_div = person_div.find('h3', class_='title')
                    
                    if name_div:
                        role_div = person_div.find('p', class_='subtitle')
                        print("NAME", name_div.text, ", ", role_div.text)
                        jpeg_source = person_div.find('source', {'type': 'image/jpeg'})
                        if jpeg_source:
                            # take the first jpeg (hopefully this doesnt super break in the future
                            image_url = jpeg_source.get('srcset').split(',')[0].split(' ')[0]

                        person = {
                            'name': name_div.text,
                            'role': role_div.text if role_div else 'Unknown',
                            'image_url': image_url if jpeg_source else None
                        }
                        people.append(person)
            
            return {'people': people} if people else None
            
        except Exception as e:
            logger.error(f"Error scraping episode page: {str(e)}")
            return None

    def save_person_info(self, person: Dict, episode_id: int) -> None:
        """Save person information to database"""
        conn = psycopg2.connect(self.db_connection_string)
        cur = conn.cursor()
        
        try:
            # Insert or update host
            name_parts = person['name'].split(' ', 1)
            first_name = name_parts[0]
            last_name = name_parts[1] if len(name_parts) > 1 else ''
            
            cur.execute("""
                INSERT INTO hosts (first_name, last_name, profile_image_url)
                VALUES (%s, %s, %s)
                ON CONFLICT (first_name, last_name) 
                DO UPDATE SET profile_image_url = EXCLUDED.profile_image_url
                RETURNING host_id
            """, (first_name, last_name, person.get('image_url')))
            
            host_id = cur.fetchone()[0]
            
            # Link to episode
            is_guest = 'guest' in person['role'].lower()

            cur.execute("""
                INSERT INTO episode_host (episode_id, host_id, is_guest, role)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (episode_id, host_id) DO NOTHING
            """, (episode_id, host_id, is_guest, person.get('role')))
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving person info: {str(e)}")
            raise
        finally:
            cur.close()
            conn.close()

    def process_episodes(self, batch_size: int = 10) -> Dict:
        """Process a batch of episodes"""
        episodes = self.get_episodes_to_process()[:batch_size]
        
        results = {
            'processed': 0,
            'failed': 0,
            'people_found': 0,
            'details': []
        }
        
        for episode in episodes:
            try:
                #logger.info(f"Processing episode: {episode['title']}")
                
                data = self.scrape_episode_page(
                    episode['apple_podcast_id'],
                    episode['apple_episode_id']
                )
                
                if data and data['people']:
                    for person in data['people']:
                        self.save_person_info(person, episode['episode_id'])
                    
                    results['processed'] += 1
                    results['people_found'] += len(data['people'])
                    results['details'].append({
                        'episode_id': episode['episode_id'],
                        'title': episode['title'],
                        'people_found': len(data['people'])
                    })
                else:
                    logger.warning(f"No people found for episode: {episode['title']}")
                    results['failed'] += 1
                
                # Be nice to Apple's servers
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error processing episode {episode['title']}: {str(e)}")
                results['failed'] += 1
                continue
        
        return results

# Example usage
if __name__ == "__main__":
    db_connection = "postgresql://localhost/podcast_db"
    scraper = EpisodeHostScraper(db_connection)
    
    try:
        results = scraper.process_episodes(batch_size=10)
        print(json.dumps(results, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")