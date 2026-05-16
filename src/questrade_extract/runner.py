"""Main extract entry point.

Refreshes the Questrade token, fetches all accounts → balances + positions,
and writes a daily snapshot to SQLite.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date

from questrade_extract.auth import AuthError, refresh
from questrade_extract.client import QuestradeClient
from questrade_extract.db import connect, upsert_balance, upsert_position

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    db_path: str
    duration_s: float
    balances_written: int = 0
    positions_written: int = 0
    accounts: int = 0
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None


def _db_path() -> str:
    state_dir = os.environ.get("STATE_DIRECTORY")
    if state_dir:
        return f"{state_dir}/questrade.db"
    return os.environ.get("QUESTRADE_DB_PATH", "./db/questrade.db")


def _token_file() -> str | None:
    return os.environ.get("QUESTRADE_TOKEN_FILE")


def run() -> RunResult:
    """Extract all accounts and write to SQLite. Returns RunResult; never calls sys.exit."""
    db_path = _db_path()
    t0 = time.perf_counter()
    result = RunResult(db_path=db_path, duration_s=0.0)

    try:
        logger.info("questrade-extract starting")

        try:
            access_token, api_server = refresh(_token_file())
        except AuthError as e:
            result.error = str(e)
            result.duration_s = time.perf_counter() - t0
            logger.error("Auth failed: %s", e)
            return result

        client = QuestradeClient(access_token, api_server)
        conn = connect(db_path)
        today = date.today()

        accounts = client.get_accounts()
        result.accounts = len(accounts)
        logger.info("Found %d account(s)", len(accounts))

        for account in accounts:
            logger.info("Processing account %s (%s)", account.number, account.type)

            balances = client.get_balances(account.number)
            for b in balances:
                upsert_balance(conn, b, today)
                result.balances_written += 1
                logger.info(
                    "  Balance [%s] %s: equity=%.2f cash=%.2f pnl=%.2f",
                    account.number, b.currency, b.total_equity, b.cash, b.open_pnl,
                )

            positions = client.get_positions(account.number)
            for p in positions:
                upsert_position(conn, p, today)
                result.positions_written += 1
                logger.info(
                    "  Position %s: qty=%.0f price=%.2f mkt_val=%.2f pnl=%.2f",
                    p.symbol, p.quantity, p.current_price, p.current_market_value, p.open_pnl,
                )

            if not positions:
                logger.info("  No positions in account %s", account.number)

        conn.commit()
        conn.close()
        logger.info("Extract complete — %d account(s) written to %s", len(accounts), db_path)

    except Exception as e:
        result.error = str(e)
        logger.exception("Unexpected error during extract")

    finally:
        result.duration_s = time.perf_counter() - t0

    return result


def main() -> None:
    result = run()
    if not result.success:
        print(result.error, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
