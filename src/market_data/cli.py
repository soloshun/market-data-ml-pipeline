"""Workshop commands. Run market-data --help for options."""

import argparse
import sys
from datetime import date

import psycopg
from dotenv import load_dotenv
from prefect.schedules import Cron

from market_data import config, storage
from market_data.flows.historical_ingestion import ingest_historical_prices
from market_data.ingest import TransientFetchError


def make_parser() -> argparse.ArgumentParser:
    """Define the workshop commands and their arguments.

    Returns:
        An argument parser for the market-data entry point.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in [
        ("run", "Ingest a bounded window and print a feature preview"),
        ("backfill", "Re-ingest an explicit historical window"),
        ("catch-up", "Use each symbol's latest stored date and a 7-day overlap"),
        ("serve", "Register a local schedule and keep its execution process running"),
        ("counts", "Check database row counts and distinct keys"),
    ]:
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--source", choices=["fixture", "yfinance"])
        command.add_argument(
            "--symbol", action="append", required=name == "backfill", help="Repeat for each ticker."
        )
        if name in {"run", "backfill"}:
            command.add_argument(
                "--start-date", type=date.fromisoformat, required=name == "backfill"
            )
            command.add_argument("--end-date", type=date.fromisoformat, required=name == "backfill")
        if name == "catch-up":
            command.add_argument("--end-date", type=date.fromisoformat)
        if name in {"run", "backfill", "serve"}:
            write = command.add_mutually_exclusive_group()
            write.add_argument("--skip-db", action="store_true", help="Preview without PostgreSQL.")
            write.add_argument("--require-db", action="store_true", help="Fail without PostgreSQL.")
        if name == "serve":
            command.add_argument("--incremental", action="store_true", help="Catch up daily.")
        if name == "run":
            command.add_argument("--demo-failure", choices=["transient", "invalid"])
    return parser


def main() -> int:
    """Load local settings, parse arguments, and run the selected command.

    Returns:
        Zero on success or one for a handled runtime error. The parser handles usage errors.
    """
    load_dotenv(".env")
    args = make_parser().parse_args()
    try:
        return run_command(args)
    except (ValueError, TransientFetchError, psycopg.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def run_command(args: argparse.Namespace) -> int:
    """Dispatch a parsed command to the flow, deployment, or database query.

    Args:
        args: Command and arguments produced by make_parser.

    Returns:
        Zero after the command completes successfully.
    """
    source = args.source or config.data_source()
    symbols = config.resolve_symbols(source, args.symbol)
    if args.command == "counts":
        url = config.database_url()
        if not url:
            raise ValueError("Set DATABASE_URL before checking database counts.")
        print("table             rows  distinct_keys")
        for symbol in symbols:
            print(symbol)
            for table, rows, keys in storage.row_counts(url, symbol):
                print(f"{table:17} {rows:4}  {keys:13}")
        return 0

    if args.command == "serve":
        if args.incremental and args.skip_db:
            raise ValueError("Incremental schedules need a database.")
        if (args.incremental or args.require_db) and not config.database_url():
            raise ValueError("Set DATABASE_URL before serving this deployment.")
        ingest_historical_prices.serve(
            name="workshop-daily",
            schedules=[Cron("0 6 * * *", timezone="Africa/Accra")],
            parameters={
                "source": source,
                "symbols": symbols,
                "skip_db": args.skip_db,
                "require_db": args.require_db,
                "incremental": args.incremental,
            },
            limit=1,
            global_limit=1,
        )
        return 0

    prices, features, loaded = ingest_historical_prices(
        source=source,
        symbols=symbols,
        start_date=getattr(args, "start_date", None),
        end_date=args.end_date,
        incremental=args.command == "catch-up",
        skip_db=getattr(args, "skip_db", False),
        require_db=getattr(args, "require_db", False),
        demo_failure=getattr(args, "demo_failure", None),
    )
    print(f"Prepared {prices.height} Silver rows and {features.height} Gold preview rows.")
    if loaded is None:
        print("PostgreSQL skipped.")
    else:
        print(f"PostgreSQL committed {loaded[0]} Silver and {loaded[1]} Gold upserts.")
    for symbol in symbols:
        print(f"Gold preview for {symbol}, calculated within the fetched window:")
        print(features.filter(features["symbol"] == symbol).tail(5))
    return 0
