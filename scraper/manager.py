import json
import psycopg2
from datetime import datetime
from typing import List, Dict, Optional
import logging
import time
from scraper import PodcastScraper
from podchaser_client import PodchaserClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PodcastManager:
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string

    def _get_connection(self):
        return psycopg2.connect(self.db_connection_string)

    # ------------------------------------------------------------------
    # SEEDING
    # ------------------------------------------------------------------

    def add_podcasts(self, podcasts: List[Dict]):
        """
        Add podcasts to the tracking table.

        Each dict may contain:
            apple_podcast_id  (str | None) -- primary key, required when known
            podchaser_id      (str | None) -- optional enrichment
            podcast_title     (str)        -- human-readable label for logging
        
        At least one of apple_podcast_id or podchaser_id must be provided.
        Rows with neither are skipped with a warning.
        """
        conn = self._get_connection()
        cur = conn.cursor()
        added = skipped = 0

        try:
            for p in podcasts:
                apple_id   = p.get("apple_podcast_id")
                podchaser  = p.get("podchaser_id")
                title      = p.get("podcast_title", "")

                if not apple_id and not podchaser:
                    logger.warning(f"Skipping '{title}' — no apple_podcast_id or podchaser_id")
                    skipped += 1
                    continue

                cur.execute(
                    """
                    INSERT INTO podcast_tracking
                        (apple_podcast_id, podchaser_id, podcast_title, status, created_at)
                    VALUES (%s, %s, %s, 'pending', NOW())
                    ON CONFLICT (apple_podcast_id) DO UPDATE
                        SET podchaser_id   = COALESCE(EXCLUDED.podchaser_id, podcast_tracking.podchaser_id),
                            podcast_title  = COALESCE(EXCLUDED.podcast_title, podcast_tracking.podcast_title)
                    """,
                    (apple_id, podchaser, title),
                )
                added += 1

            conn.commit()
            logger.info(f"Upserted {added} podcasts ({skipped} skipped — missing both IDs)")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error adding podcasts: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # QUERYING
    # ------------------------------------------------------------------

    def get_podcasts_to_update(self, min_interval_hours: int = 0, new_only: bool = False) -> List[Dict]:
        """
        Return tracking rows that need scraping, ordered by last_scraped_at ASC.
        Each row is a dict with apple_podcast_id, podchaser_id, podcast_title.
        If new_only=True, only return podcasts with no episodes in the DB yet.
        """
        conn = self._get_connection()
        cur = conn.cursor()
        try:
            new_only_filter = """
                AND NOT EXISTS (
                    SELECT 1 FROM podcasts p
                    JOIN episodes e ON e.podcast_id = p.podcast_id
                    WHERE p.apple_podcast_id = podcast_tracking.apple_podcast_id
                )""" if new_only else ""
            cur.execute(
                f"""
                SELECT apple_podcast_id, podchaser_id, podcast_title
                FROM podcast_tracking
                WHERE (last_scraped_at IS NULL OR
                       last_scraped_at < NOW() - INTERVAL '%s hours')
                  AND status != 'in_progress'
                  {new_only_filter}
                ORDER BY last_scraped_at ASC NULLS FIRST
                """,
                (min_interval_hours,),
            )
            rows = cur.fetchall()
            return [
                {"apple_podcast_id": r[0], "podchaser_id": r[1], "podcast_title": r[2]}
                for r in rows
            ]
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # STATUS UPDATES
    # ------------------------------------------------------------------

    def update_podcast_status(
        self,
        apple_podcast_id: Optional[str],
        status: str,
        error_message: str = None,
        total_episodes: int = None,
        latest_episode_date: datetime = None,
        podchaser_id: str = None,         # fill in if discovered during scrape
    ):
        conn = self._get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE podcast_tracking
                SET status              = %s,
                    last_scraped_at     = NOW(),
                    scrape_count        = scrape_count + 1,
                    error_message       = %s,
                    total_episodes      = COALESCE(%s, total_episodes),
                    latest_episode_date = COALESCE(%s, latest_episode_date),
                    podchaser_id        = COALESCE(%s, podchaser_id)
                WHERE apple_podcast_id = %s
                """,
                (status, error_message, total_episodes, latest_episode_date,
                 podchaser_id, apple_podcast_id),
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating status for {apple_podcast_id}: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # PROCESSING
    # ------------------------------------------------------------------

    def process_all_pending(self, max_podcasts: int = None, new_only: bool = False):
        """
        Main scrape loop.
        Uses apple_podcast_id as the primary driver; falls back to podchaser_id
        where apple_podcast_id is NULL (those rows will only be enriched via
        the Podchaser client path, not the Apple RSS path).
        """
        podcasts = self.get_podcasts_to_update(new_only=new_only)
        if max_podcasts:
            podcasts = podcasts[:max_podcasts]

        logger.info(f"Found {len(podcasts)} podcasts to update")

        for p in podcasts:
            apple_id  = p["apple_podcast_id"]
            podchaser = p["podchaser_id"]
            title     = p["podcast_title"]

            try:
                self.update_podcast_status(apple_id, "in_progress")

                if apple_id:
                    # Primary path: Apple Podcast ID → RSS scrape
                    scraper = PodcastScraper(self.db_connection_string)
                    results = scraper.process_podcast(apple_id)
                    self.update_podcast_status(
                        apple_id,
                        "success",
                        total_episodes=results.get("processed_episodes"),
                        latest_episode_date=datetime.now(),
                    )
                    logger.info(f"✅ Apple scrape complete: {title} ({apple_id})")
                elif podchaser:
                    # Fallback path: Podchaser-only shows (no Apple ID yet)
                    # These will be handled by podchaser_client enrichment pass
                    logger.info(f"⏭  Skipping RSS scrape for '{title}' — no Apple ID; "
                                f"will be handled by Podchaser enrichment pass")
                    self.update_podcast_status(apple_id, "pending")  # leave as pending
                else:
                    logger.warning(f"⚠️  No IDs at all for '{title}', skipping")
                    self.update_podcast_status(apple_id, "failed",
                                               error_message="No apple_podcast_id or podchaser_id")

                time.sleep(2)

            except Exception as e:
                logger.error(f"Error processing '{title}': {e}")
                self.update_podcast_status(apple_id, "failed", error_message=str(e))

    # ------------------------------------------------------------------
    # REPORTING
    # ------------------------------------------------------------------

    def get_status_summary(self) -> Dict:
        conn = self._get_connection()
        cur = conn.cursor()
        try:
            queries = {
                "total":          "SELECT COUNT(*) FROM podcast_tracking",
                "pending":        "SELECT COUNT(*) FROM podcast_tracking WHERE status = 'pending'",
                "in_progress":    "SELECT COUNT(*) FROM podcast_tracking WHERE status = 'in_progress'",
                "success":        "SELECT COUNT(*) FROM podcast_tracking WHERE status = 'success'",
                "failed":         "SELECT COUNT(*) FROM podcast_tracking WHERE status = 'failed'",
                "has_apple_id":   "SELECT COUNT(*) FROM podcast_tracking WHERE apple_podcast_id IS NOT NULL",
                "has_podchaser":  "SELECT COUNT(*) FROM podcast_tracking WHERE podchaser_id IS NOT NULL",
                "total_episodes": "SELECT COALESCE(SUM(total_episodes), 0) FROM podcast_tracking",
            }
            results = {}
            for key, query in queries.items():
                cur.execute(query)
                results[key] = cur.fetchone()[0]
            return results
        finally:
            cur.close()
            conn.close()


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Podcast episode scraper and tracking manager',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
commands:
  scrape      Scrape episodes for all pending podcasts (RSS via Apple API)
  add         Add one or more podcasts by Apple ID
  status      Print tracking summary
  reset       Reset failed podcasts back to pending so they retry

examples:
  python3 manager.py scrape
  python3 manager.py scrape --podcast "Volts"
  python3 manager.py scrape --max 5
  python3 manager.py add --apple-id 1805010154 --title "NET-0"
  python3 manager.py add --apple-id 1321759767 --podchaser-id 595385 --title "Reversing Climate Change"
  python3 manager.py status
  python3 manager.py reset
        """
    )

    parser.add_argument('command', choices=['scrape', 'add', 'status', 'reset'],
                        help='What to do')
    parser.add_argument('--db', type=str, default='postgresql://localhost/podcast_db',
                        help='Database connection string')
    parser.add_argument('--new-only', action='store_true', default=False,
                        help='Only scrape podcasts with no episodes yet')
    parser.add_argument('--max', type=int, default=None,
                        help='Max number of podcasts to scrape (default: all pending)')
    parser.add_argument('--podcast', type=str, default=None,
                        help='Only scrape this podcast title')
    parser.add_argument('--apple-id', type=str, default=None,
                        help='Apple Podcast ID for add command')
    parser.add_argument('--podchaser-id', type=str, default=None,
                        help='Podchaser ID for add command (optional)')
    parser.add_argument('--title', type=str, default=None,
                        help='Podcast title for add command')

    args = parser.parse_args()
    manager = PodcastManager(args.db)

    if args.command == 'status':
        status = manager.get_status_summary()
        print("\nPodcast Tracking Summary:")
        print(json.dumps(status, indent=2, default=str))

    elif args.command == 'add':
        if not args.apple_id:
            parser.error("--apple-id is required for the add command")

        # Auto-fetch title from iTunes API if not provided
        title = args.title
        if not title:
            import requests
            resp = requests.get(
                "https://itunes.apple.com/lookup",
                params={"id": args.apple_id, "entity": "podcast", "country": "US"},
                timeout=10
            )
            data = resp.json()
            if data.get("resultCount", 0) > 0:
                title = data["results"][0].get("collectionName", args.apple_id)
            else:
                title = args.apple_id

        manager.add_podcasts([{
            "apple_podcast_id": args.apple_id,
            "podchaser_id": args.podchaser_id,
            "podcast_title": title,
        }])
        print(f"Added: {title} ({args.apple_id})")

    elif args.command == 'reset':
        conn = manager._get_connection()
        cur = conn.cursor()
        cur.execute("UPDATE podcast_tracking SET status = 'pending' WHERE status = 'failed'")
        count = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        print(f"Reset {count} failed podcast(s) to pending")

    elif args.command == 'scrape':
        status = manager.get_status_summary()
        print("\nPodcast Tracking Summary:")
        print(json.dumps(status, indent=2, default=str))
        print()

        if args.podcast:
            # Filter to one specific show
            conn = manager._get_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE podcast_tracking SET status = 'pending' WHERE podcast_title = %s AND status = 'success'",
                (args.podcast,)
            )
            conn.commit()
            cur.close()
            conn.close()

        manager.process_all_pending(max_podcasts=args.max, new_only=args.new_only)
