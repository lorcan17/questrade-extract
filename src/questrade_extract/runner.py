"""Main extract entry point.

Refreshes the Questrade token, fetches all accounts → balances + positions,
and writes a daily snapshot to SQLite.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date

from questrade_extract.auth import AuthError, refresh
from questrade_extract.client import QuestradeClient
from questrade_extract.db import connect, upsert_balance, upsert_position
from questrade_extract.telemetry import setup_meter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _db_path() -> str:
    # NixOS systemd sets STATE_DIRECTORY; fall back to local ./db for dev
    state_dir = os.environ.get("STATE_DIRECTORY")
    if state_dir:
        return f"{state_dir}/questrade.db"
    return os.environ.get("QUESTRADE_DB_PATH", "./db/questrade.db")


def _token_file() -> str | None:
    return os.environ.get("QUESTRADE_TOKEN_FILE")


def run() -> None:
    provider, meter = setup_meter("questrade-extract")
    run_duration = meter.create_histogram("questrade_extract.run.duration_seconds", unit="s")
    run_status = meter.create_gauge("questrade_extract.run.exit_status")
    rows_counter = meter.create_counter("questrade_extract.rows_written")

    t0 = time.perf_counter()
    status = 0
    try:
        logger.info("questrade-extract starting")

        try:
            access_token, api_server = refresh(_token_file())
        except AuthError as e:
            print(str(e), file=sys.stderr)
            status = 1
            sys.exit(1)

        client = QuestradeClient(access_token, api_server)
        conn = connect(_db_path())
        today = date.today()

        accounts = client.get_accounts()
        logger.info("Found %d account(s)", len(accounts))

        for account in accounts:
            logger.info("Processing account %s (%s)", account.number, account.type)

            balances = client.get_balances(account.number)
            for b in balances:
                upsert_balance(conn, b, today)
                rows_counter.add(1, {"table": "balances"})
                logger.info(
                    "  Balance [%s] %s: equity=%.2f cash=%.2f pnl=%.2f",
                    account.number, b.currency, b.total_equity, b.cash, b.open_pnl,
                )

            positions = client.get_positions(account.number)
            for p in positions:
                upsert_position(conn, p, today)
                rows_counter.add(1, {"table": "positions"})
                logger.info(
                    "  Position %s: qty=%.0f price=%.2f mkt_val=%.2f pnl=%.2f",
                    p.symbol, p.quantity, p.current_price, p.current_market_value, p.open_pnl,
                )

            if not positions:
                logger.info("  No positions in account %s", account.number)

        conn.commit()
        conn.close()
        logger.info("Extract complete — %d account(s) written to %s", len(accounts), _db_path())

    except Exception:
        status = 1
        raise
    finally:
        run_duration.record(time.perf_counter() - t0)
        run_status.set(status)
        try:
            provider.force_flush(timeout_millis=10_000)
            provider.shutdown()
        except Exception:
            logger.warning("OTel flush/shutdown failed", exc_info=True)


if __name__ == "__main__":
    run()
