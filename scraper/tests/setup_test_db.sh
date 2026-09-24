#!/usr/bin/env bash
# Creates the disposable Postgres database the DB-backed tests use.
# Safe to re-run: drops and recreates from scratch.
#
# Usage: scraper/tests/setup_test_db.sh
set -euo pipefail

cd "$(dirname "$0")/.."   # scraper/

DB_NAME="${SCANNER_TEST_DB_NAME:-podcast_scanner_test}"
PSQL_HOST="${PGHOST:-localhost}"

dropdb --if-exists -h "$PSQL_HOST" "$DB_NAME"
createdb -h "$PSQL_HOST" "$DB_NAME"

psql -h "$PSQL_HOST" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f podcast-schema.sql > /dev/null

for f in migrate_add_data_source.sql migrate_add_host_aliases.sql \
         migrate_add_image_suggestions.sql migrate_add_scan_descriptions.sql \
         migrate_add_scrape_status.sql migrate_add_suggestions.sql \
         migrate_add_credit_suppressions.sql migrate_add_no_guest_confirmed.sql \
         migrate_add_host_affiliations.sql migrate_add_host_role_pins.sql; do
    psql -h "$PSQL_HOST" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$f" > /dev/null
done

# hosts.bluesky_handle exists in production (backend/main.py reads it) but
# was added without a migration file, so neither podcast-schema.sql nor any
# migrate_add_*.sql creates it. Without it, anything that runs the admin
# People list query fails here.
psql -h "$PSQL_HOST" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
    -c "ALTER TABLE hosts ADD COLUMN IF NOT EXISTS bluesky_handle TEXT" > /dev/null

echo "Test database '$DB_NAME' ready."
