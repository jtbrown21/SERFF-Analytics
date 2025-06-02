#!/usr/bin/env python3
"""Initialize the DuckDB database used by SERFF Analytics."""
import argparse
import os
from serff_analytics.db import DatabaseManager
from serff_analytics.config import Config


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the local DuckDB database")
    parser.add_argument(
        "--db-path", default=Config.DB_PATH, help="Location of the database file"
    )
    args = parser.parse_args()

    if os.path.exists(args.db_path):
        print(f"Database already exists at {args.db_path}")
        return

    DatabaseManager(args.db_path)
    print(f"Database created at {args.db_path}")


if __name__ == "__main__":
    main()
