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

    def get_podcasts_to_update(self, min_interval_hours: int = 0) -> List[Dict]:
        """
        Return tracking rows that need scraping, ordered by last_scraped_at ASC.
        Each row is a dict with apple_podcast_id, podchaser_id, podcast_title.
        """
        conn = self._get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT apple_podcast_id, podchaser_id, podcast_title
                FROM podcast_tracking
                WHERE (last_scraped_at IS NULL OR
                       last_scraped_at < NOW() - INTERVAL '%s hours')
                  AND status != 'in_progress'
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

    def process_all_pending(self, max_podcasts: int = None):
        """
        Main scrape loop.
        Uses apple_podcast_id as the primary driver; falls back to podchaser_id
        where apple_podcast_id is NULL (those rows will only be enriched via
        the Podchaser client path, not the Apple RSS path).
        """
        podcasts = self.get_podcasts_to_update()
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
    DB = "postgresql://localhost/podcast_db"

    PODCHASER_CLIENT_ID = "9dfc83b0-5c27-4620-9f4f-59e285c3a371"
    PODCHASER_API_KEY   = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5ZGZjODNiMC01YzI3LTQ2MjAtOWY0Zi01OWUyODVjM2EzNzEiLCJqdGkiOiI0NDJlYzJjNzFlMWUzMzA4NjFlYTM3NWQzMzc1M2IzZDljM2JlYmM2MTNmMjFkMGZhZmRhNGQxNzFlYmNiMzZiZGVhN2U4YmEyYmEwNDE2OSIsImlhdCI6MTczNzA3ODY2OC43MTEwNjgsIm5iZiI6MTczNzA3ODY2OC43MTEwNzEsImV4cCI6MTc2ODYxNDY2OC42OTgwMSwic3ViIjoiIiwic2NvcGVzIjpbIioiXX0.frj6WSFDTE8cuscjXu89gXaIZGWDWXI2ooGd2wVgR7fglltdb2Ch0LF7iFnU3M8TZ8ey5Ld0hJad3FPx_a6vIqfr_ywrl6r-1i4dIAnliyLaYgaHFgJAGuuthUCVdZmOXBveQDHqzvrvgJUHnhvxAoNJMIHTI1nwCkR5QGxqlSO4YEuoGzQMT0y2GuV8KHmFiaBUdNR2DXLSiM57TPJS7BJsf6T3n94DkzKMQjKThJyruM-GfooN5ltJolaEBKig6p4lQxrg_EP1sTE7N2T1-p763p_7TbDbl0pOjbO4Qv-fUJkxHbmUrJonhzwO4inRBYl05KvsDLA3QZ62nKX58KCG78xXz62S073C1t5f7otZm1sBpdfWW961afS_4aOjcCl_BLqG2WScveEgr45JKrT1GuNtd0ZDEgAcrwyO0CyKuNuPTdxc88Lb1IcRaNGuxTNchxxn4Gw17s-18YlWW3uQVw5fXFjF1IDKHjZpC7Sp5S1ITHp5d5uLOppPvuOQDq8lnS6HEMp7pJnw8OibU2ZWn7FDGw8mgDzMjvsYl9lNr-Lr2-safrANygA6-pMeuGE7H1Sj23NuPm-w2GVa1x9ZZLVM0zPD0cVZThhvRUfjhYk4vUbvcOUZfjt67ugJYVfZwoaTREEXuCXth3iobX-Zx1opZ712baBlEFBcd1c"

    manager = PodcastManager(DB)
    client  = PodchaserClient(
        client_id=PODCHASER_CLIENT_ID,
        api_key=PODCHASER_API_KEY,
        db_connection_string=DB,
    )

    # ----------------------------------------------------------------
    # Clean energy podcast seed list
    # apple_podcast_id is primary; podchaser_id is optional enrichment.
    # Fill in apple_podcast_id values as you look them up.
    # ----------------------------------------------------------------
    CLEAN_ENERGY_PODCASTS = [
        {"apple_podcast_id": None,          "podchaser_id": "851883",  "podcast_title": "Inevitable (formerly My Climate Journey)"},
        {"apple_podcast_id": "1593204897",  "podchaser_id": "2153206", "podcast_title": "Catalyst with Shayle Kann"},
        {"apple_podcast_id": None,          "podchaser_id": "1359342", "podcast_title": "Cleaning Up"},
        {"apple_podcast_id": "663379413",   "podchaser_id": "109600",  "podcast_title": "The Energy Gang"},
        {"apple_podcast_id": None,          "podchaser_id": "3737431", "podcast_title": "Volts"},
        {"apple_podcast_id": None,          "podchaser_id": "877656",  "podcast_title": "Switched On"},
        {"apple_podcast_id": None,          "podchaser_id": "378051",  "podcast_title": "The Interchange"},
        {"apple_podcast_id": None,          "podchaser_id": "541293",  "podcast_title": "The Interchange: Recharged"},
        {"apple_podcast_id": None,          "podchaser_id": "854652",  "podcast_title": "Watt It Takes"},
        {"apple_podcast_id": None,          "podchaser_id": "656398",  "podcast_title": "Political Climate"},
        {"apple_podcast_id": None,          "podchaser_id": "2153207", "podcast_title": "The Carbon Copy"},
        {"apple_podcast_id": None,          "podchaser_id": "4826131", "podcast_title": "Zero: The Climate Race"},
        {"apple_podcast_id": None,          "podchaser_id": "742294",  "podcast_title": "Redefining Energy"},
        {"apple_podcast_id": None,          "podchaser_id": "1979929", "podcast_title": "Climate Tech Cocktails"},
        {"apple_podcast_id": None,          "podchaser_id": "934211",  "podcast_title": "Climate Rising"},
        {"apple_podcast_id": None,          "podchaser_id": "197171",  "podcast_title": "The Energy Transition Show"},
        {"apple_podcast_id": None,          "podchaser_id": "1501741", "podcast_title": "A Matter of Degrees"},
        {"apple_podcast_id": None,          "podchaser_id": "873859",  "podcast_title": "Outrage + Optimism"},
        {"apple_podcast_id": None,          "podchaser_id": "1544490", "podcast_title": "Leaders in Cleantech"},
        {"apple_podcast_id": None,          "podchaser_id": "5769839", "podcast_title": "Supercool"},
        {"apple_podcast_id": None,          "podchaser_id": "1531238", "podcast_title": "Climate Question"},
        {"apple_podcast_id": None,          "podchaser_id": "805995",  "podcast_title": "Energy Unplugged"},
        {"apple_podcast_id": None,          "podchaser_id": "5985497", "podcast_title": "Open Circuit"},
        {"apple_podcast_id": None,          "podchaser_id": "4288186", "podcast_title": "Factor This!"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "Critical Capital"},
        {"apple_podcast_id": "1081481629",  "podchaser_id": "40484",   "podcast_title": "Columbia Energy Exchange"},
        {"apple_podcast_id": None,          "podchaser_id": "463390",  "podcast_title": "CleanTech Talk"},
        {"apple_podcast_id": None,          "podchaser_id": "1934775", "podcast_title": "The Big Switch"},
        {"apple_podcast_id": None,          "podchaser_id": "1944634", "podcast_title": "How We Survive"},
        {"apple_podcast_id": None,          "podchaser_id": "748053",  "podcast_title": "Drilled"},
        {"apple_podcast_id": "1593203014",  "podchaser_id": "4007204", "podcast_title": "The Green Blueprint"},
        {"apple_podcast_id": None,          "podchaser_id": "1257328", "podcast_title": "Build Repeat."},
        {"apple_podcast_id": None,          "podchaser_id": "4752417", "podcast_title": "Hardware to Save a Planet"},
        {"apple_podcast_id": None,          "podchaser_id": "5370888", "podcast_title": "With Great Power"},
        {"apple_podcast_id": None,          "podchaser_id": "2020250", "podcast_title": "Energy Transition Solutions"},
        {"apple_podcast_id": None,          "podchaser_id": "4147481", "podcast_title": "The Great Simplification"},
        {"apple_podcast_id": None,          "podchaser_id": "2199181", "podcast_title": "Smart Energy Voices"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "DER Task Force Podcast"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "The Carbon Removal Show"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "ClimateBiz"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "Nuclear Barbarians"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "Energy Central Power Perspectives"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "Decarbonizing Commerce"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "The Clean Energy Show"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "Climate Capital Podcast"},
        {"apple_podcast_id": "1561411048",  "podchaser_id": "2176818", "podcast_title": "Climate CEOs"},
        {"apple_podcast_id": None,          "podchaser_id": "604598",  "podcast_title": "Titans of Nuclear"},
        {"apple_podcast_id": "296762605",   "podchaser_id": "14444",   "podcast_title": "Climate One"},
        {"apple_podcast_id": "1541394865",  "podchaser_id": "1569388", "podcast_title": "Where the Internet Lives"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "Climate Insiders"},
        {"apple_podcast_id": None,          "podchaser_id": None,      "podcast_title": "The Tech 4 Climate Podcast"},
    ]

    try:
        # Podcasts are seeded via podcast-schema.sql.
        # Only uncomment add_podcasts() if adding new shows not in the schema.
        # manager.add_podcasts(CLEAN_ENERGY_PODCASTS)

        status = manager.get_status_summary()
        print("\nPodcast Tracking Summary:")
        print(json.dumps(status, indent=2, default=str))

        # Run the full scrape (Apple RSS -> episodes table)
        #manager.process_all_pending()

        # Uncomment to run Podchaser enrichment on hosts:
        # client.enrich_all_hosts(batch_size=5)

        # Uncomment to look up missing Podchaser IDs for podcasts:
        client.find_podcast_podchaser_ids(batch_size=10)

        # Uncomment to sync episode credits via Podchaser:
        # client.sync_episode_credits(batch_size=10)

    except Exception as e:
        print(f"Error: {e}")
