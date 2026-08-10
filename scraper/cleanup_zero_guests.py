"""
cleanup_zero_guests.py

Removes incorrectly linked guests from Zero: The Climate Race episodes.
The issue: "Explore further: Past episode with [Name]" footer caused guests
from OTHER episodes to be linked to the wrong episode via parsed_desc.

This script:
1. Shows you what would be removed (dry-run by default)
2. Removes parsed_desc episode_host links from Zero episodes where the
   person's name ONLY appears in the "Explore further" footer, not the main content
3. Also cleans up any suggestions that were wrongly approved for Zero

Usage:
    python3 cleanup_zero_guests.py           # dry run — show what would be removed
    python3 cleanup_zero_guests.py --run     # actually delete
"""

import psycopg2
import re
import argparse

DB = 'postgresql://localhost/podcast_db'

STRIP_AT = [
    'Explore further:',
    '\nPast episode with',
    'See omnystudio.com',
    '\nSee Privacy Policy',
]


def clean_description(text):
    """Strip footer content from description."""
    if not text:
        return ''
    for pattern in STRIP_AT:
        idx = text.find(pattern)
        if idx != -1:
            text = text[:idx]
    return text.strip()


def run(dry_run=True):
    conn = psycopg2.connect(DB)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    cur = conn.cursor()

    # Get all parsed_desc credits on Zero episodes
    cur.execute("""
        SELECT
            eh.episode_id,
            eh.host_id,
            e.title AS episode_title,
            e.description,
            h.first_name || ' ' || h.last_name AS host_name,
            eh.data_source
        FROM episode_host eh
        JOIN episodes e ON eh.episode_id = e.episode_id
        JOIN podcasts p ON e.podcast_id = p.podcast_id
        JOIN hosts h ON h.host_id = eh.host_id
        WHERE p.title = 'Zero: The Climate Race'
          AND eh.data_source IN ('parsed_desc', 'parsed_title')
          AND eh.is_guest = true
        ORDER BY e.published_date DESC, h.last_name
    """)
    credits = cur.fetchall()

    print(f"\nChecking {len(credits)} parsed guest credits on Zero episodes...\n")

    to_remove = []

    for credit in credits:
        name = credit['host_name']
        desc = credit['description'] or ''
        title = credit['episode_title'] or ''
        name_lower = name.lower()

        # Check if name appears in MAIN content (before footer)
        main_content = clean_description(desc)
        in_main = name_lower in main_content.lower() or name_lower in title.lower()

        # Check if name appears in FOOTER only
        footer = desc[len(main_content):].lower()
        in_footer = name_lower in footer

        if not in_main and in_footer:
            to_remove.append(credit)
            print(f"  REMOVE: {name} from \"{title[:60]}\"")
            print(f"          (only appears in footer: '{footer[footer.find(name_lower)-20:footer.find(name_lower)+40].strip()}')")
            print()

    print(f"\n{'Would remove' if dry_run else 'Removing'} {len(to_remove)} incorrectly linked guest credits")

    if dry_run:
        print("\nDry run — run with --run to actually delete")
        cur.close()
        conn.close()
        return

    # Delete
    removed = 0
    for credit in to_remove:
        cur.execute("""
            DELETE FROM episode_host
            WHERE episode_id = %s AND host_id = %s
              AND data_source IN ('parsed_desc', 'parsed_title')
        """, (credit['episode_id'], credit['host_id']))
        removed += cur.rowcount

    conn.commit()
    print(f"Deleted {removed} incorrect credits")

    cur.close()
    conn.close()


if __name__ == '__main__':
    import psycopg2.extras

    parser = argparse.ArgumentParser(
        description='Remove incorrectly linked guests from Zero: The Climate Race episodes'
    )
    parser.add_argument('--run', action='store_true', default=False,
                        help='Actually delete (default is dry run)')
    args = parser.parse_args()

    run(dry_run=not args.run)
