#!/usr/bin/env python3
"""
Falcon resource classes for the Investments REST API.
"""
import logging

import falcon

import model

log = logging.getLogger(__name__)


class HealthResource:

    def __init__(self, conn):
        self.conn = conn

    async def on_get(self, req, resp):
        """GET /health"""
        if self.conn.is_connected():
            resp.media = {'status': 'healthy', 'database': 'connected'}
        else:
            resp.media = {'status': 'unhealthy', 'database': 'disconnected'}
            resp.status = falcon.HTTP_503


class SetupResource:
    """Admin — create keyspace and all 6 tables (DDL only, no data)."""

    def __init__(self, conn):
        self.conn = conn

    async def on_post(self, req, resp):
        """POST /setup"""
        try:
            session = self.conn.session
            model.create_keyspace(session, self.conn.keyspace, self.conn.replication_factor)
            session.set_keyspace(self.conn.keyspace)
            model.create_schema(session)
            resp.media = {
                'status': 'success',
                'message': 'Keyspace and schema created',
                'tables': [name for name, _ in model.ALL_TABLES],
            }
            resp.status = falcon.HTTP_201
        except Exception as e:
            log.exception("Setup failed")
            resp.media = {'status': 'error', 'message': str(e)}
            resp.status = falcon.HTTP_500


class SeedResource:
    """Admin — populate demo data through real business operations."""

    def __init__(self, conn):
        self.conn = conn

    async def on_post(self, req, resp):
        """POST /seed  body: { "accounts": 5, "trades_per_account": 10 }"""
        try:
            body = await req.get_media() or {}
            session = self.conn.session
            session.set_keyspace(self.conn.keyspace)
            result = model.seed_data(
                session,
                num_accounts=int(body.get('accounts', 5)),
                trades_per_account=int(body.get('trades_per_account', 10)),
            )
            resp.media = {'status': 'success', **result}
            resp.status = falcon.HTTP_201
        except Exception as e:
            log.exception("Seed failed")
            resp.media = {'status': 'error', 'message': str(e)}
            resp.status = falcon.HTTP_500


class AccountsResource:

    def __init__(self, conn):
        self.conn = conn

    async def on_get(self, req, resp):
        """
        GET /accounts?username=X

        Reads from accounts_by_user. Username is the partition key — this is
        always the entry point because the user is known from their session.
        """
        username = req.get_param('username')
        if not username:
            resp.media = {'error': 'username parameter is required'}
            resp.status = falcon.HTTP_400
            return
        try:
            session = self.conn.session
            session.set_keyspace(self.conn.keyspace)
            accounts = model.get_accounts_by_user(session, username)
            resp.media = {'username': username, 'accounts': accounts, 'count': len(accounts)}
        except Exception as e:
            log.exception(f"get accounts: {username}")
            resp.media = {'error': str(e)}
            resp.status = falcon.HTTP_500

    async def on_post(self, req, resp):
        """
        POST /accounts — open a new investment account.

        Body: { "username": "alice", "name": "Alice's Portfolio", "initial_balance": 10000 }

        Single write to accounts_by_user.
        """
        try:
            body = await req.get_media()
            username = (body.get('username') or '').strip()
            name = (body.get('name') or '').strip()
            balance = float(body.get('initial_balance', 10000.0))
            if not username or not name:
                resp.media = {'error': 'username and name are required'}
                resp.status = falcon.HTTP_400
                return
            session = self.conn.session
            session.set_keyspace(self.conn.keyspace)
            result = model.open_account(session, username, name, balance)
            resp.media = result
            resp.status = falcon.HTTP_201
        except Exception as e:
            log.exception("open account")
            resp.media = {'error': str(e)}
            resp.status = falcon.HTTP_500


class PortfolioResource:

    def __init__(self, conn):
        self.conn = conn

    async def on_get(self, req, resp, account_id):
        """GET /accounts/{account_id}/portfolio — current holdings"""
        try:
            session = self.conn.session
            session.set_keyspace(self.conn.keyspace)
            positions = model.get_portfolio(session, account_id)
            resp.media = {'account_id': account_id, 'positions': positions, 'count': len(positions)}
        except Exception as e:
            log.exception(f"get portfolio: {account_id}")
            resp.media = {'error': str(e)}
            resp.status = falcon.HTTP_500


class TradesResource:

    def __init__(self, conn):
        self.conn = conn

    async def on_post(self, req, resp, account_id):
        """
        POST /accounts/{account_id}/trades — execute a buy or sell order.

        Body: { "username": "alice", "type": "buy", "symbol": "AAPL", "shares": 10, "price": 180.50 }

        username is required because accounts_by_user uses it as partition key
        for the cash balance update. In a real app this comes from the auth token.

        One trade writes to SIX tables — the response lists every write explicitly.
        """
        try:
            body = await req.get_media()
            username = (body.get('username') or '').strip()
            trade_type = (body.get('type') or '').strip()
            symbol = (body.get('symbol') or '').strip().upper()
            shares = int(body.get('shares', 0))
            price = float(body.get('price', 0))

            if not username or not trade_type or not symbol or shares <= 0 or price <= 0:
                resp.media = {'error': 'username, type, symbol, shares (>0) and price (>0) are required'}
                resp.status = falcon.HTTP_400
                return

            session = self.conn.session
            session.set_keyspace(self.conn.keyspace)
            result = model.execute_trade(session, username, account_id, trade_type, symbol, shares, price)
            resp.media = result
            resp.status = falcon.HTTP_201
        except ValueError as e:
            resp.media = {'error': str(e)}
            resp.status = falcon.HTTP_400
        except Exception as e:
            log.exception(f"execute trade: {account_id}")
            resp.media = {'error': str(e)}
            resp.status = falcon.HTTP_500

    async def on_get(self, req, resp, account_id):
        """
        GET /accounts/{account_id}/trades — trade history with optional filters.

        Query params: start, end, type (buy|sell), symbol, limit

        The response includes query_routing showing which of the 4 trade tables
        was selected based on the active filters.
        """
        start = req.get_param('start')
        end = req.get_param('end')
        limit = req.get_param_as_int('limit') or 100
        trade_type = req.get_param('type')
        symbol = req.get_param('symbol')

        try:
            session = self.conn.session
            session.set_keyspace(self.conn.keyspace)
            trades, table_used, reason = model.get_trade_history(
                session, account_id,
                start_date=start, end_date=end,
                limit=limit, trade_type=trade_type, symbol=symbol,
            )
            resp.media = {
                'account_id': account_id,
                'trades': trades,
                'count': len(trades),
                'query_routing': {'table_used': table_used, 'reason': reason},
            }
        except Exception as e:
            log.exception(f"get trades: {account_id}")
            resp.media = {'error': str(e)}
            resp.status = falcon.HTTP_500
