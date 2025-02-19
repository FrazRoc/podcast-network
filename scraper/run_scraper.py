from scraper import PodcastScraper
import logging
import re
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_podcast_id(url: str) -> str:
    """Extract podcast ID from Apple Podcasts URL"""
    podcast_match = re.search(r'id(\d+)', url)
    if podcast_match:
        return podcast_match.group(1)
    raise ValueError("Could not extract podcast ID from URL")

def main():
    # Replace with your database connection string
    db_connection = "postgresql://localhost/podcast_db"
    
    # The podcast URL you want to scrape
    podcast_url = "https://podcasts.apple.com/us/podcast/id1200361736"
    
    try:
        # Extract podcast ID from URL
        podcast_id = extract_podcast_id(podcast_url)
        logger.info(f"Extracted podcast ID: {podcast_id}")
        
        # Initialize and run scraper
        scraper = PodcastScraper(db_connection)
        results = scraper.process_podcast(podcast_id)
        
        print("\nResults:")
        print(f"Successfully processed {results['processed_episodes']} episodes")
        print(f"Failed to process {results['failed_episodes']} episodes")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    main()