import requests
import psycopg2
from datetime import datetime, date
import logging
import json
import time
import feedparser
import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

# A feed must account for this much of what we already store before its
# episodes can be trusted to dedupe by title.
MIN_TITLE_MATCH_RATIO = 0.8
MIN_EPISODES_FOR_MATCH_CHECK = 20

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Episode markers some feeds wrap around the title that iTunes doesn't carry:
# "966: Title", "[Episode #282] - Title", "Ep 12: Title", "Title, Ep #136".
# Used only to decide whether two titles denote the same episode; the stored
# title is never rewritten.
_TITLE_PREFIX_RE = re.compile(
    r'^\s*(?:\[?\s*(?:ep(?:isode)?\.?\s*)?#?\d+\s*\]?)\s*[:.–—-]\s*', re.IGNORECASE
)
_TITLE_SUFFIX_RE = re.compile(
    r'\s*[,–—-]?\s*(?:\[[^\]]*\]|\((?:ep(?:isode)?\.?\s*)?#?\d+\)|'
    r'(?:ep(?:isode)?\.?\s*)#?\d+)\s*$', re.IGNORECASE
)


def _flatten_title(title: str) -> str:
    t = title.replace('’', "'").replace('‘', "'")
    t = t.replace('“', '"').replace('”', '"')
    return t.replace('–', '-').replace('—', '-')


def normalize_episode_title(title: str, strip_numbering: bool = True) -> str:
    """Key an episode by its title, ignoring per-feed numbering decoration.

    strip_numbering=False keeps the numbering, for shows where dropping it
    would merge genuinely different episodes (see pick_title_key).
    """
    if not title:
        return ''
    t = _flatten_title(title)
    if strip_numbering:
        t = _TITLE_PREFIX_RE.sub('', t)
        t = _TITLE_SUFFIX_RE.sub('', t)
    return re.sub(r'[^a-z0-9]+', '', t.lower())


def pick_title_key(feed_titles):
    """Choose how to key this show's episodes, then return that key function.

    Stripping "Ep 12:" off the front lets a numbered feed line up with iTunes'
    unnumbered titles, but it also merges "Ep 12: Weekly Roundup" with "Ep 13:
    Weekly Roundup" — two real episodes that differ only by number. If that
    would happen here, keep the numbering for this show and accept that its
    titles simply won't match iTunes'; the match-rate gate then skips it
    rather than inserting duplicates.
    """
    titles = [t for t in feed_titles if t]
    stripped = {normalize_episode_title(t) for t in titles}
    if len(stripped) < len(set(titles)):
        return lambda t: normalize_episode_title(t, strip_numbering=False)
    return normalize_episode_title


class PodcastScraper:
    def __init__(self, db_connection_string, episode_limit: int = 50):
        self.conn = psycopg2.connect(db_connection_string)
        self.cursor = self.conn.cursor()
        self.base_url = "https://itunes.apple.com"
        self.episode_limit = episode_limit

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
        params['limit'] = self.episode_limit
        
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

        # Extract show-level description from RSS channel
        show_description = (
            getattr(feed.feed, 'description', '') or
            getattr(feed.feed, 'subtitle', '') or
            getattr(feed.feed, 'summary', '') or
            ''
        )

        feed_data = {
            'description': show_description,
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

    def insert_podcast(self, podcast_data: Dict, rss_description: str = '') -> int:
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
                podcast_data.get('description', '') or rss_description,
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
            description = COALESCE(NULLIF(EXCLUDED.description, ''), episodes.description),
            audio_url = COALESCE(EXCLUDED.audio_url, episodes.audio_url),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, episodes.duration_seconds),
            published_date = COALESCE(EXCLUDED.published_date, episodes.published_date),
            episode_number = COALESCE(EXCLUDED.episode_number, episodes.episode_number),
            season_number = COALESCE(EXCLUDED.season_number, episodes.season_number),
            -- Never clear an Apple episode id with a NULL. An RSS-sourced row
            -- has no trackId, and this column is what gates the Apple credits
            -- scraper (WHERE apple_episode_id IS NOT NULL) — overwriting it
            -- would silently switch off our best source of host/guest labels.
            apple_episode_id = COALESCE(EXCLUDED.apple_episode_id, episodes.apple_episode_id)
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
        
        # Written out rather than as a conditional expression: the previous
        # one-liner parsed as "(published_date or releaseDate) if releaseDate
        # else None", so an RSS episode — which has published_date and no
        # releaseDate — always came out None, silently dropping the date.
        published_date = episode_data.get('published_date')
        if not published_date and episode_data.get('releaseDate'):
            try:
                published_date = datetime.strptime(
                    episode_data['releaseDate'], '%Y-%m-%dT%H:%M:%SZ'
                ).date()
            except ValueError:
                published_date = None

        values = (
            podcast_id,
            episode_data.get('trackName') or episode_data.get('title'),
            episode_data.get('description', ''),
            episode_data.get('episodeUrl') or episode_data.get('link'),
            duration,
            published_date,
            episode_data.get('episode_number'),
            episode_data.get('season_number'),
            episode_data.get('trackId')
        )
        
        self.cursor.execute(query, values)
        episode_id = self.cursor.fetchone()[0]
        return episode_id

    def backfill_from_rss(self, apple_podcast_id: str, since: Optional[date] = None,
                          dry_run: bool = False, refresh_existing: bool = False,
                          force: bool = False) -> Dict:
        """Add episodes the RSS feed has but iTunes never returned.

        iTunes' Lookup API caps at 200 episodes per show, so process_podcast()
        can only ever see the most recent 200 — everything older is invisible
        to it. The RSS feed usually carries the full back catalogue (Climate
        One serves 914 where iTunes gives 200), so this fills the gap.

        Episodes added here have no apple_episode_id, since that only comes
        from iTunes. The Apple credits scraper skips them by design; they get
        credits from description parsing only.
        """
        result = {'title': None, 'feed_total': 0, 'considered': 0, 'inserted': 0,
                  'refreshed': 0, 'existing': 0, 'skipped_old': 0, 'failed': 0,
                  'stored': 0, 'matched': 0, 'skipped_show': False}
        try:
            return self._backfill_from_rss(apple_podcast_id, since, dry_run,
                                           refresh_existing, force, result)
        except Exception:
            self.conn.rollback()
            raise
        finally:
            # Same single-use lifecycle as process_podcast: the caller builds
            # one scraper per show.
            self.cursor.close()
            self.conn.close()

    def _backfill_from_rss(self, apple_podcast_id: str, since, dry_run: bool,
                           refresh_existing: bool, force: bool, result: Dict) -> Dict:
        itunes_data = self.fetch_itunes_data(apple_podcast_id)
        result['title'] = itunes_data['podcast'].get('trackName')

        feed_url = itunes_data['podcast'].get('feedUrl')
        if not feed_url:
            raise ValueError("Could not find RSS feed URL")

        rss_data = self.parse_rss_feed(feed_url)
        result['feed_total'] = len(rss_data['episodes'])
        if not rss_data['episodes']:
            logger.warning(f"Feed returned no episodes: {feed_url}")
            return result

        podcast_id = self.insert_podcast(
            itunes_data['podcast'], rss_description=rss_data.get('description', '')
        )

        # Titles already stored for this show — the same key the episodes table
        # uniquely constrains on, so this predicts exactly what would conflict.
        self.cursor.execute(
            "SELECT title FROM episodes WHERE podcast_id = %s", (podcast_id,)
        )
        stored = [r[0] for r in self.cursor.fetchall()]

        # Decide the keying scheme from the feed, then use it on both sides.
        key_of = pick_title_key([e.get('title') or '' for e in rss_data['episodes']])
        # Keep the stored spelling for each key. The episodes table is unique on
        # the exact title, so an upsert only lands on the right row if it uses
        # the title already there — iTunes titles carry stray characters the
        # feed lacks (one ends in a U+202F narrow no-break space), and inserting
        # the feed's spelling would quietly create a second row for the episode.
        existing_by_key = {}
        for t in stored:
            k = key_of(t)
            if k:
                existing_by_key.setdefault(k, t)
        existing_titles = set(existing_by_key)

        # Safety gate. Dedupe rests entirely on titles lining up, and some
        # feeds title episodes quite differently from iTunes — SunCast's feed
        # numbers every episode, so only 10 of our 210 matched and a run would
        # have inserted 831 duplicates of episodes we already had. If the feed
        # cannot account for most of what we hold, we cannot tell new episodes
        # from ones we already have, so refuse rather than guess.
        feed_keys = {key_of(e.get('title') or '') for e in rss_data['episodes']}
        matched = len(existing_titles & feed_keys)
        result['stored'] = len(stored)
        result['matched'] = matched
        if not force and len(stored) >= MIN_EPISODES_FOR_MATCH_CHECK and \
                matched < len(stored) * MIN_TITLE_MATCH_RATIO:
            result['skipped_show'] = True
            logger.warning(
                f"  Skipping: feed titles match only {matched}/{len(stored)} stored "
                f"episodes, so new ones can't be told from duplicates. "
                f"Use force=True to override."
            )
            return result

        for rss_episode in rss_data['episodes']:
            title = (rss_episode.get('title') or '').strip()
            if not title:
                continue

            published = rss_episode.get('published_date')
            if since and published and published < since:
                result['skipped_old'] += 1
                continue

            key = key_of(title)
            if key in existing_titles and not refresh_existing:
                result['existing'] += 1
                continue

            result['considered'] += 1
            if dry_run:
                logger.info(f"  [dry-run] would add: {published} — {title[:80]}")
                continue

            try:
                # Passed as both arguments on purpose: the first supplies the
                # RSS-shaped fields, the second makes insert_episode take its
                # RSS branch for duration ("HH:MM:SS" rather than millis).
                was_present = key in existing_titles
                if was_present:
                    # Refresh mode: address the row we already have.
                    rss_episode = dict(rss_episode, title=existing_by_key[key])
                self.insert_episode(rss_episode, podcast_id, rss_episode)
                existing_titles.add(key)   # feeds can repeat a title
                existing_by_key.setdefault(key, rss_episode.get('title'))
                if was_present:
                    result['refreshed'] += 1
                else:
                    result['inserted'] += 1
            except Exception as e:
                logger.error(f"  Failed to add '{title[:60]}': {e}")
                result['failed'] += 1

        if dry_run:
            self.conn.rollback()
        else:
            self.conn.commit()
        return result

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
            podcast_id = self.insert_podcast(itunes_data['podcast'], rss_description=rss_data.get('description', ''))

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