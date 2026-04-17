"""Questrade REST API client — accounts, balances, positions."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)


@dataclass
class Account:
    number: str
    type: str
    status: str
    is_primary: bool


@dataclass
class Balance:
    account_number: str
    currency: str
    cash: float
    market_value: float
    total_equity: float
    book_cost: float
    open_pnl: float


@dataclass
class Position:
    account_number: str
    symbol: str
    symbol_id: int
    description: str
    currency: str
    quantity: float
    current_price: float
    average_entry_price: float
    current_market_value: float
    book_cost: float
    open_pnl: float


class QuestradeClient:
    def __init__(self, access_token: str, api_server: str) -> None:
        self._base = f"{api_server.rstrip('/')}/v1"
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {access_token}"

    def _get(self, path: str) -> dict:
        resp = self._session.get(f"{self._base}{path}", timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_accounts(self) -> list[Account]:
        data = self._get("/accounts")
        return [
            Account(
                number=a["number"],
                type=a["type"],
                status=a["status"],
                is_primary=a.get("isPrimary", False),
            )
            for a in data.get("accounts", [])
        ]

    def get_balances(self, account_number: str) -> list[Balance]:
        data = self._get(f"/accounts/{account_number}/balances")
        balances = []
        for b in data.get("combinedBalances", []):
            balances.append(Balance(
                account_number=account_number,
                currency=b["currency"],
                cash=b.get("cash", 0.0),
                market_value=b.get("marketValue", 0.0),
                total_equity=b.get("totalEquity", 0.0),
                book_cost=b.get("bookCost", 0.0),
                open_pnl=b.get("openPnl", 0.0),
            ))
        return balances

    def get_positions(self, account_number: str) -> list[Position]:
        data = self._get(f"/accounts/{account_number}/positions")
        return [
            Position(
                account_number=account_number,
                symbol=p["symbol"],
                symbol_id=p["symbolId"],
                description=p.get("description", ""),
                currency=p.get("currency", ""),
                quantity=p.get("openQuantity", 0.0),
                current_price=p.get("currentPrice", 0.0),
                average_entry_price=p.get("averageEntryPrice", 0.0),
                current_market_value=p.get("currentMarketValue", 0.0),
                book_cost=p.get("bookCost", 0.0),
                open_pnl=p.get("openPnl", 0.0),
            )
            for p in data.get("positions", [])
        ]
