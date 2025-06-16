"""Monthly newsletter workflow CLI.

Run ``python -m src.monthly_workflow <command> [--dry-run]`` from the project
root to orchestrate the monthly newsletter process.

Commands
========
``generate``
    Build newsletter reports for all states with filing activity and log them to
    Airtable.

``send``
    Send all "Approved" reports to subscribers via Postmark.

Passing ``--dry-run`` performs a full simulation without writing files or
sending emails.

The environment variables listed in ``docs/README.md`` must be configured
before running any commands.
"""

import sys
import logging
from dotenv import load_dotenv

from src.generate_reports import generate_all_reports
from src.send_reports import send_approved_reports
from src.shared.utils import check_required_env_vars

load_dotenv()
logging.basicConfig(level=logging.INFO)

if not check_required_env_vars():
    logging.error("Required environment variables not set. Aborting.")
    sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m src.monthly_workflow {generate|send} [--dry-run]")
        return

    command = sys.argv[1]
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("🔍 DRY RUN MODE - No changes will be made\n")

    if command == "generate":
        generate_all_reports(dry_run)
    elif command == "send":
        send_approved_reports(dry_run)
    else:
        print(f"Unknown command: {command}")
        print("Use 'generate' or 'send'")


if __name__ == "__main__":
    main()
