"""Command line entry for monthly newsletter workflow."""

import sys
import logging
from dotenv import load_dotenv

from src.generate_reports import generate_all_reports
from src.send_reports import send_approved_reports

load_dotenv()
logging.basicConfig(level=logging.INFO)


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m src.monthly_workflow [generate|send] [--dry-run]")
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
