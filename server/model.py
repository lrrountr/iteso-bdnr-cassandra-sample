#!/usr/bin/env python3
"""
Database model layer for the Investments portfolio application.

Schema design — 6 tables, all access patterns start from a known username:

  accounts_by_user      — list accounts for a user (entry point)
  positions_by_account  — current holdings per account
  trades_by_a_d         — trade history, filter by date range
  trades_by_a_td        — trade history, filter by type
  trades_by_a_std       — trade history, filter by symbol + type
  trades_by_a_sd        — trade history, filter by symbol

Typical query flow:
  username → accounts_by_user → account_ids → positions_by_account / trades_*
"""
import csv
import datetime
import logging
import os
import random
import time
import uuid

import time_uuid
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType

log = logging.getLogger(__name__)

DATA_DIR = os.getenv('DATA_DIR', os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data'))


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_users():
    with open(os.path.join(DATA_DIR, 'users.csv'), 'r') as f:
        return [(r['username'], r['name']) for r in csv.DictReader(f)]


def load_instruments():
    with open(os.path.join(DATA_DIR, 'instruments.csv'), 'r') as f:
        return [r['symbol'] for r in csv.DictReader(f)]


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class CassandraConnection:

    def __init__(self, hosts=None, port=9042, keyspace='investments', replication_factor=1):
        self.hosts = hosts or ['localhost']
        self.port = port
        self.keyspace = keyspace
        self.replication_factor = replication_factor
        self.cluster = None
        self.session = None

    def connect(self, retries=5, delay=5):
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                log.info(f"Connecting to Cassandra (attempt {attempt}/{retries})")
                self.cluster = Cluster(self.hosts, port=self.port)
                self.session = self.cluster.connect()
                return True
            except Exception as e:
                last_exc = e
                log.warning(f"Connection failed ({attempt}/{retries}): {e}")
                if attempt < retries:
                    time.sleep(delay)
        raise last_exc

    def close(self):
        for obj in (self.session, self.cluster):
            if obj:
                try:
                    obj.shutdown()
                except Exception:
                    pass

    def is_connected(self):
        return self.session is not None and not self.session.is_shutdown


# ---------------------------------------------------------------------------
# DDL — 6 tables
# ---------------------------------------------------------------------------

CREATE_KEYSPACE = """
    CREATE KEYSPACE IF NOT EXISTS {}
    WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

# Entry point: username is always known (from auth/session in a real app).
# Returns account_ids which are then used to query positions and trades.
CREATE_ACCOUNTS_BY_USER = """
    CREATE TABLE IF NOT EXISTS accounts_by_user (
        username     TEXT,
        account_id   TEXT,
        name         TEXT STATIC,
        cash_balance DOUBLE,
        PRIMARY KEY ((username), account_id)
    )
"""

# Positions keyed by account_id — retrieved after resolving account_ids from accounts_by_user.
CREATE_POSITIONS = """
    CREATE TABLE IF NOT EXISTS positions_by_account (
        account_id TEXT,
        symbol     TEXT,
        quantity   INT,
        PRIMARY KEY ((account_id), symbol)
    )
"""

# Four trade tables — same data, different clustering keys.
# Each supports a specific query pattern; every trade write goes to all four.

CREATE_TRADES_BY_DATE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_d (
        account_id TEXT,
        trade_id   TIMEUUID,
        type       TEXT,
        symbol     TEXT,
        shares     INT,
        price      DOUBLE,
        amount     DOUBLE,
        PRIMARY KEY ((account_id), trade_id)
    ) WITH CLUSTERING ORDER BY (trade_id DESC)
"""

CREATE_TRADES_BY_TYPE_DATE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_td (
        account_id TEXT,
        trade_id   TIMEUUID,
        type       TEXT,
        symbol     TEXT,
        shares     INT,
        price      DOUBLE,
        amount     DOUBLE,
        PRIMARY KEY ((account_id), type, trade_id)
    ) WITH CLUSTERING ORDER BY (type ASC, trade_id DESC)
"""

CREATE_TRADES_BY_SYMBOL_TYPE_DATE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_std (
        account_id TEXT,
        trade_id   TIMEUUID,
        type       TEXT,
        symbol     TEXT,
        shares     INT,
        price      DOUBLE,
        amount     DOUBLE,
        PRIMARY KEY ((account_id), symbol, type, trade_id)
    ) WITH CLUSTERING ORDER BY (symbol ASC, type ASC, trade_id DESC)
"""

CREATE_TRADES_BY_SYMBOL_DATE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_sd (
        account_id TEXT,
        trade_id   TIMEUUID,
        type       TEXT,
        symbol     TEXT,
        shares     INT,
        price      DOUBLE,
        amount     DOUBLE,
        PRIMARY KEY ((account_id), symbol, trade_id)
    ) WITH CLUSTERING ORDER BY (symbol ASC, trade_id DESC)
"""

ALL_TABLES = [
    ('accounts_by_user',     CREATE_ACCOUNTS_BY_USER),
    ('positions_by_account', CREATE_POSITIONS),
    ('trades_by_a_d',        CREATE_TRADES_BY_DATE),
    ('trades_by_a_td',       CREATE_TRADES_BY_TYPE_DATE),
    ('trades_by_a_std',      CREATE_TRADES_BY_SYMBOL_TYPE_DATE),
    ('trades_by_a_sd',       CREATE_TRADES_BY_SYMBOL_DATE),
]

# Maps active filter set → (table, reason)
TRADE_TABLE_ROUTING = {
    frozenset(['symbol', 'type']): ('trades_by_a_std', 'optimized for symbol + type filters'),
    frozenset(['symbol']):         ('trades_by_a_sd',  'optimized for symbol filter'),
    frozenset(['type']):           ('trades_by_a_td',  'optimized for type filter'),
    frozenset():                   ('trades_by_a_d',   'optimized for date range / all trades'),
}


def _exec(session, cql, retries=3, delay=3):
    for attempt in range(1, retries + 1):
        try:
            session.execute(cql, timeout=30)
            time.sleep(0.2)
            return
        except Exception as e:
            log.warning(f"CQL failed ({attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay * attempt)
            else:
                raise


def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace '{keyspace}' RF={replication_factor}")
    _exec(session, CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info(f"Creating {len(ALL_TABLES)} tables")
    for _, ddl in ALL_TABLES:
        _exec(session, ddl)


# ---------------------------------------------------------------------------
# Business operations
# ---------------------------------------------------------------------------

def open_account(session, username, name, initial_balance=10000.0):
    """
    Open a new investment account.

    Single write to accounts_by_user — username is the partition key,
    so the account is immediately queryable by that user.
    """
    account_id = str(uuid.uuid4())
    balance = float(initial_balance)

    session.execute(
        session.prepare(
            "INSERT INTO accounts_by_user (username, account_id, name, cash_balance) VALUES (?, ?, ?, ?)"
        ),
        (username, account_id, name, balance)
    )

    log.info(f"Account opened: {account_id} for '{username}'")
    return {
        'account_id': account_id,
        'username': username,
        'name': name,
        'cash_balance': balance,
        'writes': [
            {'table': 'accounts_by_user', 'description': 'account stored under username partition'},
        ],
    }


def execute_trade(session, username, account_id, trade_type, symbol, shares, price):
    """
    Execute a buy or sell order.

    username is required because accounts_by_user uses it as partition key —
    in a real app this comes from the authenticated session, not the request body.

    One trade writes to SIX tables:
      1-4. Four trade tables  — same data, different clustering keys
                                each table serves a different query pattern
        5. positions_by_account — running share count per symbol
        6. accounts_by_user     — updated cash balance

    Writes 1-4 use an UNLOGGED BATCH (efficient multi-row write to the same
    logical operation). Writes 5-6 are separate — Cassandra has no cross-
    partition ACID transactions, which is an intentional scalability trade-off.
    """
    trade_type = trade_type.lower()
    if trade_type not in ('buy', 'sell'):
        raise ValueError("trade_type must be 'buy' or 'sell'")

    amount = round(float(shares) * float(price), 2)
    trade_id = time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(datetime.datetime.now()))

    # Read current account cash balance
    acc = session.execute(
        session.prepare(
            "SELECT cash_balance FROM accounts_by_user WHERE username = ? AND account_id = ?"
        ),
        (username, account_id)
    ).one()
    if not acc:
        raise ValueError(f"Account {account_id} not found for user '{username}'")
    old_balance = float(acc.cash_balance or 0)

    # Read current position for this symbol
    pos = session.execute(
        session.prepare(
            "SELECT quantity FROM positions_by_account WHERE account_id = ? AND symbol = ?"
        ),
        (account_id, symbol)
    ).one()
    old_quantity = pos.quantity if pos else 0

    # Compute new state
    if trade_type == 'buy':
        new_balance = old_balance - amount
        new_quantity = old_quantity + shares
    else:
        new_balance = old_balance + amount
        new_quantity = old_quantity - shares

    # Writes 1-4: all four trade tables in a single unlogged batch
    trade_row = (account_id, trade_id, trade_type, symbol, shares, float(price), amount)
    trade_stmts = [
        session.prepare("INSERT INTO trades_by_a_d   (account_id,trade_id,type,symbol,shares,price,amount) VALUES (?,?,?,?,?,?,?)"),
        session.prepare("INSERT INTO trades_by_a_td  (account_id,trade_id,type,symbol,shares,price,amount) VALUES (?,?,?,?,?,?,?)"),
        session.prepare("INSERT INTO trades_by_a_std (account_id,trade_id,type,symbol,shares,price,amount) VALUES (?,?,?,?,?,?,?)"),
        session.prepare("INSERT INTO trades_by_a_sd  (account_id,trade_id,type,symbol,shares,price,amount) VALUES (?,?,?,?,?,?,?)"),
    ]
    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
    for stmt in trade_stmts:
        batch.add(stmt, trade_row)
    session.execute(batch)

    # Write 5: update position (upsert)
    session.execute(
        session.prepare(
            "INSERT INTO positions_by_account (account_id, symbol, quantity) VALUES (?, ?, ?)"
        ),
        (account_id, symbol, new_quantity)
    )

    # Write 6: update cash balance — username is the partition key, so it is required here
    session.execute(
        session.prepare(
            "UPDATE accounts_by_user SET cash_balance = ? WHERE username = ? AND account_id = ?"
        ),
        (new_balance, username, account_id)
    )

    log.info(f"Trade: {trade_type} {shares}x {symbol} @ ${price:.2f} — account {account_id}")
    return {
        'trade_id': str(trade_id),
        'account_id': account_id,
        'username': username,
        'type': trade_type,
        'symbol': symbol,
        'shares': shares,
        'price': float(price),
        'amount': amount,
        'cash_balance_after': new_balance,
        'writes': [
            {'table': 'trades_by_a_d',       'description': 'all trades — date range queries'},
            {'table': 'trades_by_a_td',       'description': 'trades filtered by type'},
            {'table': 'trades_by_a_std',      'description': 'trades filtered by symbol + type'},
            {'table': 'trades_by_a_sd',       'description': 'trades filtered by symbol'},
            {'table': 'positions_by_account', 'description': f'{symbol}: {old_quantity} → {new_quantity} shares'},
            {'table': 'accounts_by_user',     'description': f'cash: ${old_balance:,.2f} → ${new_balance:,.2f}'},
        ],
    }


def seed_data(session, num_accounts=5, trades_per_account=10):
    """Populate demo data using real business operations."""
    users = load_users()
    instruments = load_instruments()
    created = []

    for _ in range(num_accounts):
        username, name = random.choice(users)
        balance = round(random.uniform(5000, 100000), 2)
        result = open_account(session, username, name, balance)
        created.append((username, result['account_id']))

    total_trades = 0
    for username, account_id in created:
        for _ in range(trades_per_account):
            try:
                execute_trade(
                    session, username, account_id,
                    trade_type=random.choice(['buy', 'sell']),
                    symbol=random.choice(instruments),
                    shares=random.randint(1, 500),
                    price=round(random.uniform(1.0, 1000.0), 2),
                )
                total_trades += 1
            except Exception as e:
                log.warning(f"Seed trade skipped: {e}")

    return {
        'accounts_created': len(created),
        'trades_created': total_trades,
        'sample_accounts': [{'username': u, 'account_id': a} for u, a in created[:5]],
    }


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------

def get_accounts_by_user(session, username):
    rows = session.execute(
        session.prepare(
            "SELECT username, account_id, name, cash_balance FROM accounts_by_user WHERE username = ?"
        ),
        (username,)
    )
    return [
        {
            'account_id': r.account_id,
            'username': r.username,
            'name': r.name or '',
            'cash_balance': float(r.cash_balance or 0),
        }
        for r in rows
    ]


def get_portfolio(session, account_id):
    """Return current holdings for an account (positions only — balance lives in accounts_by_user)."""
    rows = session.execute(
        session.prepare(
            "SELECT symbol, quantity FROM positions_by_account WHERE account_id = ?"
        ),
        (account_id,)
    )
    return [{'symbol': r.symbol, 'quantity': r.quantity} for r in rows]


def get_trade_history(session, account_id, start_date=None, end_date=None,
                      limit=100, trade_type=None, symbol=None):
    """
    Query trade history using the table that best matches the active filters.
    Returns (trades, table_name, reason).
    """
    active = frozenset(f for f, v in [('symbol', symbol), ('type', trade_type)] if v)
    table, reason = TRADE_TABLE_ROUTING.get(active, TRADE_TABLE_ROUTING[frozenset()])

    cql = f"SELECT trade_id, type, symbol, shares, price, amount FROM {table} WHERE account_id = ?"
    params = [account_id]

    if symbol and 'symbol' in active:
        cql += ' AND symbol = ?'
        params.append(symbol)
    if trade_type and 'type' in active:
        cql += ' AND type = ?'
        params.append(trade_type)

    def to_timeuuid(s):
        for fmt in ('%Y-%m-%d', '%Y-%m-%dT%H:%M:%S'):
            try:
                return time_uuid.TimeUUID.with_timestamp(
                    time_uuid.mkutime(datetime.datetime.strptime(s, fmt))
                )
            except Exception:
                pass
        return None

    if start_date:
        tu = to_timeuuid(start_date)
        if tu:
            cql += ' AND trade_id >= ?'
            params.append(tu)
    if end_date:
        tu = to_timeuuid(end_date)
        if tu:
            cql += ' AND trade_id <= ?'
            params.append(tu)

    rows = session.execute(session.prepare(cql), params)
    trades = []
    for row in rows:
        if len(trades) >= limit:
            break
        ts = None
        try:
            ts = datetime.datetime.fromtimestamp(
                (row.trade_id.time - 0x01b21dd213814000) / 1e7
            ).isoformat()
        except Exception:
            pass
        trades.append({
            'trade_id': str(row.trade_id),
            'datetime': ts,
            'type': row.type,
            'symbol': row.symbol,
            'shares': row.shares,
            'price': float(row.price),
            'amount': float(row.amount),
        })

    return trades, table, reason
