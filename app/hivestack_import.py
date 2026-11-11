"""Load captured Hivestack records into an existing AdBridge database.

Startup seeding only runs on an empty database, so use this to switch a
database that is already seeded, or to refresh it from a new capture.

    python -m app.hivestack_import                  # upsert the capture into the hs_ tables
    python -m app.hivestack_import --replace        # empty the hs_ v2 tables, reseed in sample mode
    python -m app.hivestack_import --dir PATH       # capture directory (default: config.yml sample_dir)

Uses the same database settings as the app (DB_BACKEND, SQLITE_PATH, POSTGRES_*).
--replace keeps the RTB tables (publishers, screens, deals, bid requests,
impressions, events) and deletes everything else under /hivestack, including
records created through POST.
"""

import argparse
import os
import sys
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m app.hivestack_import", description=__doc__.split("\n")[0])
    parser.add_argument("--dir", help="capture directory (default: hivestack.sample_dir in config.yml)")
    parser.add_argument("--replace", action="store_true",
                        help="empty the Hivestack v2 tables first, then load the capture and synthesize the rest")
    args = parser.parse_args(argv)

    if args.dir:
        os.environ["HIVESTACK_SAMPLE_DIR"] = args.dir
    os.environ["HIVESTACK_SEED"] = "sample"

    from app import database
    from app.db_backend import get_connection, init_db_schema
    from app.hivestack_model import V2_TABLES

    directory = database._hs_sample_dir()
    if not directory:
        sys.exit(f"No capture files found in {Path(database._hs_settings()['sample_dir']).resolve()}")

    conn = get_connection()
    try:
        database._migrate_hivestack_v2(conn)
        init_db_schema(conn)
        cur = conn.cursor()
        if args.replace:
            for table in V2_TABLES:
                cur.execute(f"DELETE FROM {table}")
            database._seed_hivestack_v2(cur)  # empty tables -> loads the capture, then synthesizes the rest
        else:
            counts = database._load_hivestack_capture(cur, directory)
            print(f"Hivestack: upserted {counts} from {directory}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
