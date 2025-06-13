#!/usr/bin/env python3
"""CLI interface for Airtable sync operations"""

import click
import duckdb
from datetime import datetime
from tabulate import tabulate
import time

from serff_analytics.ingest.airtable_sync import AirtableSync


@click.group()
def cli():
    """Airtable sync management CLI"""
    pass


@cli.command()
@click.option('--full', is_flag=True, help='Force a full sync')
def sync(full):
    """Run Airtable sync"""
    try:
        syncer = AirtableSync()

        since_date = datetime(1900, 1, 1) if full else None

        click.echo("🔄 Starting sync...")
        click.echo(f"   Mode: {'FULL' if full else 'INCREMENTAL'}")

        start_time = time.time()
        result = syncer.sync_data(since=since_date)
        duration = time.time() - start_time

        click.echo(f"\n✅ Sync completed in {duration:.1f} seconds!")
        click.echo(f"   Processed: {result['records_processed']} records")
        click.echo(f"   Total in DB: {result['total_records']}")

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)


@cli.command()
def status():
    """Show current sync status"""
    try:
        conn = duckdb.connect('serff_analytics/data/insurance_filings.db', read_only=True)

        last_sync = conn.execute(
            """
            SELECT started_at, completed_at, sync_mode, status, records_processed
            FROM sync_history 
            ORDER BY started_at DESC 
            LIMIT 1
            """
        ).fetchone()

        if not last_sync:
            click.echo("No sync history found.")
            return

        stats = conn.execute(
            """
            SELECT COUNT(*) as total,
                   COUNT(Airtable_Last_Modified) as with_timestamps
            FROM filings
            """
        ).fetchone()

        click.echo("\n📊 SYNC STATUS")
        click.echo("=" * 40)
        click.echo(f"Last sync: {last_sync[3]} ({last_sync[2]})")
        click.echo(f"Started: {last_sync[0]}")
        click.echo(f"Records in DB: {stats[0]:,}")
        click.echo(f"With timestamps: {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)")

        conn.close()

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)


@cli.command()
def history():
    """Show sync history"""
    try:
        conn = duckdb.connect('serff_analytics/data/insurance_filings.db', read_only=True)

        results = conn.execute(
            """
            SELECT sync_id, started_at, sync_mode, status, records_processed
            FROM sync_history 
            ORDER BY started_at DESC 
            LIMIT 10
            """
        ).fetchall()

        if not results:
            click.echo("No sync history found.")
            return

        click.echo("\n📜 RECENT SYNCS")
        headers = ['ID', 'Started', 'Mode', 'Status', 'Records']

        formatted = []
        for r in results:
            formatted.append([
                r[0],
                r[1].strftime('%Y-%m-%d %H:%M'),
                r[2],
                "✅" if r[3] == 'completed' else "❌",
                r[4] or 0
            ])

        click.echo(tabulate(formatted, headers=headers, tablefmt='simple'))
        conn.close()

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)


@cli.command()
def test():
    """Test sync system health"""
    click.echo("🧪 Testing sync system...\n")

    passed = 0
    total = 3

    try:
        conn = duckdb.connect('serff_analytics/data/insurance_filings.db', read_only=True)

        schema = conn.execute("PRAGMA table_info(filings)").fetchall()
        if any(col[1] == 'Airtable_Last_Modified' for col in schema):
            click.echo("✅ Database schema correct")
            passed += 1
        else:
            click.echo("❌ Missing Airtable_Last_Modified column")

        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        if any(t[0] == 'sync_history' for t in tables):
            click.echo("✅ Sync history table exists")
            passed += 1
        else:
            click.echo("❌ Missing sync_history table")

        last = conn.execute(
            """
            SELECT completed_at FROM sync_history 
            WHERE status = 'completed' 
            ORDER BY completed_at DESC LIMIT 1
            """
        ).fetchone()

        if last:
            click.echo("✅ Sync has run successfully")
            passed += 1
        else:
            click.echo("❌ No successful syncs found")

        conn.close()

        click.echo(f"\nResult: {passed}/{total} tests passed")

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)


if __name__ == '__main__':
    cli()

