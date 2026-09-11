"""Record the timestamp of the most recent successful scrape pipeline run.

Usage: python3 mark_scrape_complete.py --db postgresql://...
"""
import argparse
import psycopg2

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', type=str, required=True, help='Database connection string')
    args = parser.parse_args()

    conn = psycopg2.connect(args.db)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO scrape_status (id, last_run_at)
        VALUES (1, NOW())
        ON CONFLICT (id) DO UPDATE SET last_run_at = NOW();
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Marked scrape complete.")
