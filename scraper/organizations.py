"""
organizations.py

Keeps organizations / organization_aliases in step with the company names in
host_affiliations (see migrate_add_organizations.sql).

    python3 organizations.py sync --dry-run   # counts only, no writes
    python3 organizations.py sync             # then rebuilds the merge-suggestion queue
    python3 organizations.py suggestions      # rebuild the queue only

sync does two things, both idempotent:
  1. Stamps host_affiliations.company_key (normalize_org_name(company)) on
     rows that lack it.
  2. Creates one organisation, plus an 'auto' alias, for every key that no
     alias covers yet. Its name is the most common spelling of that key.

It never merges: which spellings are the same organisation beyond the
normalised key is decided by a person in Company Admin, from the queue that
sync rebuilds afterwards (company_merge_suggestions — see
backend/org_suggestions.py).
"""

import argparse
import logging
import os
import sys
from collections import Counter, defaultdict

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'backend'))
from org_names import normalize_org_name, _LEGAL_SUFFIXES  # noqa: E402
from org_suggestions import refresh_suggestions  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB = os.getenv('DATABASE_URL', 'postgresql://localhost/podcast_db')


def _is_bare(spelling: str) -> bool:
    """No leading "The" and no legal suffix — the name as a person would say it."""
    words = spelling.split()
    return (not words or words[0].casefold() != 'the') and \
        (len(words) < 2 or words[-1].casefold().rstrip('.') not in _LEGAL_SUFFIXES)


def pick_display_name(spellings: list) -> str:
    """Most common spelling. Ties go to the bare form ("Carbon Engineering"
    over "Carbon Engineering Ltd.", "World Resources Institute" over "the
    World Resources Institute"), then to the one with the most capitals (the
    brand's own styling: "BloombergNEF" over "bloombergnef"), then the
    shortest."""
    counts = Counter(spellings)
    return max(counts, key=lambda s: (counts[s], _is_bare(s), sum(c.isupper() for c in s), -len(s)))


def plan_sync(cur) -> dict:
    """What sync would do, without doing it."""
    cur.execute("SELECT affiliation_id, company FROM host_affiliations "
                "WHERE company IS NOT NULL AND company_key IS NULL")
    to_stamp = [(aid, normalize_org_name(company)) for aid, company in cur.fetchall()]

    cur.execute("SELECT company, company_key FROM host_affiliations WHERE company IS NOT NULL")
    spellings = defaultdict(list)
    for company, key in cur.fetchall():
        spellings[key or normalize_org_name(company)].append(company)

    cur.execute("SELECT normalized_name FROM organization_aliases")
    known = {r[0] for r in cur.fetchall()}
    new_orgs = sorted(
        ((key, pick_display_name(names)) for key, names in spellings.items()
         if key and key not in known),
        key=lambda kn: kn[0],
    )
    return {'to_stamp': [(aid, key) for aid, key in to_stamp if key], 'new_orgs': new_orgs}


def apply_sync(cur, plan: dict) -> None:
    if plan['to_stamp']:
        execute_values(cur, """
            UPDATE host_affiliations ha SET company_key = d.key
            FROM (VALUES %s) AS d(affiliation_id, key)
            WHERE ha.affiliation_id = d.affiliation_id
        """, plan['to_stamp'])
    for key, name in plan['new_orgs']:
        cur.execute("INSERT INTO organizations (name) VALUES (%s) RETURNING org_id", (name,))
        org_id = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO organization_aliases (org_id, alias_name, normalized_name, source)
            VALUES (%s, %s, %s, 'auto')
        """, (org_id, name, key))


def sync(dry_run: bool = False) -> dict:
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    plan = plan_sync(cur)
    logger.info(f"Roles to stamp with a company key: {len(plan['to_stamp'])}")
    logger.info(f"New organisations to create:       {len(plan['new_orgs'])}")
    for key, name in plan['new_orgs'][:15]:
        logger.info(f"    {name}  [{key}]")
    if dry_run:
        conn.rollback()
    else:
        apply_sync(cur, plan)
        conn.commit()
        logger.info("Done. Rebuilding merge suggestions…")
        logger.info(f"Merge suggestions: {refresh_suggestions(conn)}")
    cur.close()
    conn.close()
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('sync')
    p.add_argument('--dry-run', action='store_true')
    sub.add_parser('suggestions', help='Rebuild the merge-suggestion queue only')
    args = parser.parse_args()
    if args.command == 'sync':
        sync(args.dry_run)
    elif args.command == 'suggestions':
        conn = psycopg2.connect(DB)
        logger.info(f"Merge suggestions: {refresh_suggestions(conn)}")
        conn.close()


if __name__ == '__main__':
    main()
