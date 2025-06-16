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
import argparse
from dotenv import load_dotenv

from src.generate_reports import generate_all_reports
from src.send_reports import send_approved_reports
from src.shared.utils import check_required_env_vars

load_dotenv()
logging.basicConfig(level=logging.INFO)

if not check_required_env_vars():
    logging.error("Required environment variables not set. Aborting.")
    sys.exit(1)


def is_test_mode(args: argparse.Namespace) -> bool:
    """Return True if the CLI is running in test mode."""
    return getattr(args, "test", False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Monthly newsletter workflow")
    subparsers = parser.add_subparsers(dest="command", required=True)

    gen = subparsers.add_parser("generate", help="Generate monthly reports")
    gen.add_argument("--dry-run", action="store_true")
    gen.add_argument("-t", "--test", action="store_true", help="Enable test mode")
    gen.add_argument("-i", "--test-item", help="Process a single item in test mode")

    send = subparsers.add_parser("send", help="Send approved reports")
    send.add_argument("--dry-run", action="store_true")
    send.add_argument("-t", "--test", action="store_true", help="Enable test mode")
    send.add_argument("-i", "--test-item", help="Process a single item in test mode")

    args = parser.parse_args()

    if args.dry_run:
        print("🔍 DRY RUN MODE - No changes will be made\n")

    if args.command == "generate":
        generate_all_reports(
            dry_run=args.dry_run,
            test_mode=is_test_mode(args),
            test_item=args.test_item,
        )
    elif args.command == "send":
        send_approved_reports(
            dry_run=args.dry_run,
            test_mode=is_test_mode(args),
            test_item=args.test_item,
        )


if __name__ == "__main__":
    main()
