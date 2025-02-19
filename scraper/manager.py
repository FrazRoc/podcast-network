import json
import psycopg2
from datetime import datetime
from typing import List, Dict
import logging
import time
from scraper import PodcastScraper  # Your existing scraper
from podchaser_client import PodchaserClient
from episode_scraper import EpisodeHostScraper

#remember to activate virtual env 
# source venv/bin/activate


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PodcastManager:
    def __init__(self, db_connection_string):
        self.db_connection_string = db_connection_string

        
    def _get_connection(self):
        """Get a fresh database connection"""
        return psycopg2.connect(self.db_connection_string)

    def add_podcasts(self, apple_podcast_ids: List[str]):
        """Add new podcasts to track"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            for podcast_id in apple_podcast_ids:
                query = """
                INSERT INTO podcast_tracking 
                (apple_podcast_id, status, created_at)
                VALUES (%s, 'pending', NOW())
                ON CONFLICT (apple_podcast_id) DO NOTHING
                """
                cur.execute(query, (podcast_id,))
            
            conn.commit()
            logger.info(f"Added {len(apple_podcast_ids)} podcasts to pending tracking")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error adding podcasts: {str(e)}")
            raise
        finally:
            cur.close()
            conn.close()

    def get_podcasts_to_update(self, min_interval_hours: int = 0) -> List[str]: # temp set to 1 hour
        """Get list of podcasts that need updating"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            query = """
            SELECT apple_podcast_id 
            FROM podcast_tracking
            WHERE (last_scraped_at IS NULL OR 
                  last_scraped_at < NOW() - INTERVAL '%s hours')
            AND status != 'in_progress'
            ORDER BY last_scraped_at ASC NULLS FIRST
            """
            cur.execute(query, (min_interval_hours,))
            results = cur.fetchall()
            return [r[0] for r in results]
            
        finally:
            cur.close()
            conn.close()

    def update_podcast_status(self, apple_podcast_id: str, status: str, 
                            error_message: str = None, total_episodes: int = None,
                            latest_episode_date: datetime = None):
        """Update podcast tracking status"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            query = """
            UPDATE podcast_tracking 
            SET status = %s,
                last_scraped_at = NOW(),
                scrape_count = scrape_count + 1,
                error_message = %s,
                total_episodes = COALESCE(%s, total_episodes),
                latest_episode_date = COALESCE(%s, latest_episode_date)
            WHERE apple_podcast_id = %s
            """
            cur.execute(query, (status, error_message, total_episodes, 
                              latest_episode_date, apple_podcast_id))
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating podcast status: {str(e)}")
            raise
        finally:
            cur.close()
            conn.close()

    def process_all_pending(self, max_podcasts: int = None):
        """Process all pending podcasts"""
        podcasts_to_update = self.get_podcasts_to_update()
        if max_podcasts:
            podcasts_to_update = podcasts_to_update[:max_podcasts]
            
        logger.info(f"Found {len(podcasts_to_update)} podcasts to update")
        
        for podcast_id in podcasts_to_update:
            try:
                # Mark as in progress
                self.update_podcast_status(podcast_id, 'in_progress')
                
                # Create new scraper for each podcast
                scraper = PodcastScraper(self.db_connection_string)
                results = scraper.process_podcast(podcast_id)
                
                # Update status with results
                self.update_podcast_status(
                    podcast_id,
                    'success',
                    total_episodes=results['processed_episodes'],
                    latest_episode_date=datetime.now()  # You might want to get this from the actual episodes
                )
                
                logger.info(f"Successfully processed podcast {podcast_id}")
                
                # Sleep briefly between podcasts
                time.sleep(2)
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error processing podcast {podcast_id}: {error_msg}")
                self.update_podcast_status(podcast_id, 'failed', error_message=error_msg)

    def get_status_summary(self) -> Dict:
        """Get summary of podcast tracking status"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        try:
            queries = {
                'total': "SELECT COUNT(*) FROM podcast_tracking",
                'pending': "SELECT COUNT(*) FROM podcast_tracking WHERE status = 'pending'",
                'in_progress': "SELECT COUNT(*) FROM podcast_tracking WHERE status = 'in_progress'",
                'success': "SELECT COUNT(*) FROM podcast_tracking WHERE status = 'success'",
                'failed': "SELECT COUNT(*) FROM podcast_tracking WHERE status = 'failed'",
                'total_episodes': "SELECT COALESCE(SUM(total_episodes), 0) FROM podcast_tracking",
            }
            
            results = {}
            for key, query in queries.items():
                cur.execute(query)
                results[key] = cur.fetchone()[0]
            
            return results
            
        finally:
            cur.close()
            conn.close()

if __name__ == "__main__":
    # Example usage
    db_connection = "postgresql://localhost/podcast_db"
    podchaser_client_id = "9dfc83b0-5c27-4620-9f4f-59e285c3a371"
    podchaser_api_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5ZGZjODNiMC01YzI3LTQ2MjAtOWY0Zi01OWUyODVjM2EzNzEiLCJqdGkiOiI0NDJlYzJjNzFlMWUzMzA4NjFlYTM3NWQzMzc1M2IzZDljM2JlYmM2MTNmMjFkMGZhZmRhNGQxNzFlYmNiMzZiZGVhN2U4YmEyYmEwNDE2OSIsImlhdCI6MTczNzA3ODY2OC43MTEwNjgsIm5iZiI6MTczNzA3ODY2OC43MTEwNzEsImV4cCI6MTc2ODYxNDY2OC42OTgwMSwic3ViIjoiIiwic2NvcGVzIjpbIioiXX0.frj6WSFDTE8cuscjXu89gXaIZGWDWXI2ooGd2wVgR7fglltdb2Ch0LF7iFnU3M8TZ8ey5Ld0hJad3FPx_a6vIqfr_ywrl6r-1i4dIAnliyLaYgaHFgJAGuuthUCVdZmOXBveQDHqzvrvgJUHnhvxAoNJMIHTI1nwCkR5QGxqlSO4YEuoGzQMT0y2GuV8KHmFiaBUdNR2DXLSiM57TPJS7BJsf6T3n94DkzKMQjKThJyruM-GfooN5ltJolaEBKig6p4lQxrg_EP1sTE7N2T1-p763p_7TbDbl0pOjbO4Qv-fUJkxHbmUrJonhzwO4inRBYl05KvsDLA3QZ62nKX58KCG78xXz62S073C1t5f7otZm1sBpdfWW961afS_4aOjcCl_BLqG2WScveEgr45JKrT1GuNtd0ZDEgAcrwyO0CyKuNuPTdxc88Lb1IcRaNGuxTNchxxn4Gw17s-18YlWW3uQVw5fXFjF1IDKHjZpC7Sp5S1ITHp5d5uLOppPvuOQDq8lnS6HEMp7pJnw8OibU2ZWn7FDGw8mgDzMjvsYl9lNr-Lr2-safrANygA6-pMeuGE7H1Sj23NuPm-w2GVa1x9ZZLVM0zPD0cVZThhvRUfjhYk4vUbvcOUZfjt67ugJYVfZwoaTREEXuCXth3iobX-Zx1opZ712baBlEFBcd1c"
    manager = PodcastManager(db_connection)
    client = PodchaserClient(client_id=podchaser_client_id, api_key=podchaser_api_key, db_connection_string=db_connection)

    
    # Add some podcasts to track
    podcast_ids = [ 
        #"360084272"#, # "Joe Rogan"
        "1521578868", # "Smartless"
        "1745204141", # Where Everybody knows your Name
        "1438054347", # Conan Needs a Friend
        "1345682353", # Armchair Expert with Dax Shepard
        "394775318", # 99% Invisible - Roman Mars host
        "1242537529", # What Roman Mars can Learn about Con Law - Roman is not listed as host on Apple
        "1119389968", # Revisisionist History
        "1002937870", # Dear Hank and John  - Roman Mars a guest in 2023 - https://www.podchaser.com/podcasts/dear-hank-john-46112/episodes/368-cowboys-through-and-throug-169954847
        "350359306", # Stuff to Blow your Mind - Roman Mars a guest in 2023 https://www.podchaser.com/podcasts/stuff-to-blow-your-mind-11871/episodes/from-the-vault-the-99-invisibl-195049079
        "265799883", # The Bugle - Jon Oliver sometimes guest
        "1586197367", # Late Show with Stephen Colbert 
        "1200361736", # The Daily
        "1469394914", #  The Journal
        "152016440", # WSJ Whats News
        "152024419", # WSJ your money briefing
        "74844126", # WSJ Tech news briefing
        "971901464", # WSJ Opinion
        "1234320525", # WSJ Future of Everything
        "1202441485", # WSJ Minute Briefing
        "290783428", # Planet Money
        "1320118593", # Planet Money - The Indicator
        "1578892272", # Planet Money - Summer School
        "278981407", # Stuff you Should KNow
        "329875043", # WTF with Marc Maron - RSS feed should have guest info
        "1710609544", # What Now with Trevor Noah
        "98746009", # Real Time with Bill Maher
        "425179503", # Anderson Cooper 360
        "1643163707", # All there Is with Anderson Cooper
        "1192761536", # Pod Save America 
        "1200016351", # Pod Save the World
        "1469168641", # Strict Scrutiny
        "1610392666", # Offline
        "1593203014", # Latitude Media - The Green Blueprint
        "1623272960", # Latitude Media - The Latitude
        "663379413", # The Energy Gang
        "1593204897", # Catalyst
        #"296762605", # Climate One - no host credits
        #"1534829787", # A Matter of degrees - no host credits
        "1645614328", # Fast Politics with Molly Jong-Fast
        "214089682", # Fresh Air
        "1028908750", # Hidden Brain
        "1459666764", # Jordan Klepper Fingers the Pulse
        "1334878780" # The Daily Show - sometimes Jon Stewart
    ]

    #podcast_ids = ["1192761536"]  # Trevor
    
    try:
        # Add podcasts
        manager.add_podcasts(podcast_ids)
        
        # Process pending podcasts
        manager.process_all_pending(max_podcasts=100)  # Process up to 2 podcasts
        
        # Get status summary
        status = manager.get_status_summary()
        print("\nPodcast Tracking Summary:")
        print(json.dumps(status, indent=2))


        #print("enriching host 1")
        # Podchaser: Enrich specific host
        #result = client.enrich_host_data(host_id=1)
        #print(json.dumps(result, indent=2))

        # print("enriching all hosts")
        # Podchaser: Enrich all hosts
        # results = client.enrich_all_hosts(batch_size=5)
        # print(json.dumps(results, indent=2))

        # Podchaser:  Look up Podchaser IDs for up to 10 podcasts
        #print("Looking up all Podcasts without a Podchaser ID")
        #results = client.find_podcast_podchaser_ids(batch_size=10)
        #print(json.dumps(results, indent=2))

        # Podchaser: Look up Podchaser IDs for up to 10 episodes
        #print("Looking up all Episodes without a Podchaser ID")
        #results = client.find_episode_podchaser_ids(batch_size=1)
        #print(json.dumps(results, indent=2))

        # Podchaser: Sync credits for up to 10 episodes
        #results = client.sync_episode_credits(batch_size=1)
        #print(json.dumps(results, indent=2))

        #print("searching for guests of episode 1")
        # Podchaser: Search for guests
        #guests = client.search_guests_by_episode(episode_id=1)
        #print(json.dumps(guests, indent=2))

        #podchaser_id = 10829 # Joe Rogan Experience
        #podcast = client.get_podcast_by_id(podchaser_id)
        #if podcast:
        #    print(f"Found podcast: {podcast['title']}")
        #    print(json.dumps(podcast, indent=2))
        #else:
        #    print("Podcast not found")

        scraper = EpisodeHostScraper("postgresql://localhost/podcast_db")
        results = scraper.process_episodes(batch_size=100)
        print(json.dumps(results, indent=2))
        
    except Exception as e:
        print(f"Error: {str(e)}")