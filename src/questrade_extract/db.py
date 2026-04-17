"""SQLite storage — schema creation and upsert helpers."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

from questrade_extract.client import Balance, Position


def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _apply_schema(conn)
    return conn


def _apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS questrade_balances (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            account_number   TEXT    NOT NULL,
            currency         TEXT    NOT NULL,
            snapshot_date    TEXT    NOT NULL,
            cash             REAL    NOT NULL,
            market_value     REAL    NOT NULL,
            total_equity     REAL    NOT NULL,
            book_cost        REAL    NOT NULL,
            open_pnl         REAL    NOT NULL,
            fetched_at       TEXT    NOT NULL,
            UNIQUE(account_number, currency, snapshot_date)
        );

        CREATE TABLE IF NOT EXISTS questrade_positions (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            account_number       TEXT    NOT NULL,
            snapshot_date        TEXT    NOT NULL,
            symbol               TEXT    NOT NULL,
            symbol_id            INTEGER,
            description          TEXT,
            currency             TEXT,
            quantity             REAL    NOT NULL,
            current_price        REAL    NOT NULL,
            average_entry_price  REAL    NOT NULL,
            current_market_value REAL    NOT NULL,
            book_cost            REAL    NOT NULL,
            open_pnl             REAL    NOT NULL,
            fetched_at           TEXT    NOT NULL,
            UNIQUE(account_number, symbol, snapshot_date)
        );
    """)
    conn.commit()


def upsert_balance(conn: sqlite3.Connection, b: Balance, snapshot_date: date) -> None:
    conn.execute("""
        INSERT INTO questrade_balances
            (account_number, currency, snapshot_date, cash, market_value,
             total_equity, book_cost, open_pnl, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(account_number, currency, snapshot_date) DO UPDATE SET
            cash=excluded.cash, market_value=excluded.market_value,
            total_equity=excluded.total_equity, book_cost=excluded.book_cost,
            open_pnl=excluded.open_pnl, fetched_at=excluded.fetched_at
    """, (
        b.account_number, b.currency, snapshot_date.isoformat(),
        b.cash, b.market_value, b.total_equity, b.book_cost, b.open_pnl,
        datetime.now(timezone.utc).isoformat(),
    ))


def upsert_position(conn: sqlite3.Connection, p: Position, snapshot_date: date) -> None:
    conn.execute("""
        INSERT INTO questrade_positions
            (account_number, snapshot_date, symbol, symbol_id, description,
             currency, quantity, current_price, average_entry_price,
             current_market_value, book_cost, open_pnl, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(account_number, symbol, snapshot_date) DO UPDATE SET
            quantity=excluded.quantity, current_price=excluded.current_price,
            average_entry_price=excluded.average_entry_price,
            current_market_value=excluded.current_market_value,
            book_cost=excluded.book_cost, open_pnl=excluded.open_pnl,
            fetched_at=excluded.fetched_at
    """, (
        p.account_number, snapshot_date.isoformat(), p.symbol, p.symbol_id,
        p.description, p.currency, p.quantity, p.current_price,
        p.average_entry_price, p.current_market_value, p.book_cost, p.open_pnl,
        datetime.now(timezone.utc).isoformat(),
    ))
