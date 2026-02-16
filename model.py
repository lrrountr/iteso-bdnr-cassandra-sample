#!/usr/bin/env python3
import datetime
import logging
import random
import uuid
import time
import sys

import time_uuid
from cassandra.query import BatchStatement
from tabulate import tabulate

from fixtures import DEMO_USERS, DEMO_INSTRUMENTS, DEMO_CONFIG

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS accounts_by_user (
        username TEXT,
        account_number TEXT,
        cash_balance DOUBLE,
        name TEXT STATIC,
        PRIMARY KEY ((username),account_number)
    )
"""

CREATE_POSITIONS_BY_ACCOUNT_TABLE = """
    CREATE TABLE IF NOT EXISTS positions_by_account (
        account TEXT,
        symbol TEXT,
        quantity INT,
        PRIMARY KEY ((account),symbol)
    )
"""

CREATE_TRADES_BY_ACCOUNT_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_d (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares INT,
        price DOUBLE,
        amount DOUBLE,
        PRIMARY KEY ((account), trade_id)
    ) WITH CLUSTERING ORDER BY (trade_id DESC)
"""

CREATE_TRADES_BY_ACCOUNT_TYPE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_td (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares INT,
        price DOUBLE,
        amount DOUBLE,
        PRIMARY KEY ((account), type, trade_id)
    ) WITH CLUSTERING ORDER BY (type ASC, trade_id DESC)
"""

CREATE_TRADES_BY_ACCOUNT_SYMBOL_TYPE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_std (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares INT,
        price DOUBLE,
        amount DOUBLE,
        PRIMARY KEY ((account), symbol, type, trade_id)
    ) WITH CLUSTERING ORDER BY (symbol ASC, type ASC, trade_id DESC)
"""

CREATE_TRADES_BY_ACCOUNT_SYMBOL_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_sd (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares INT,
        price DOUBLE,
        amount DOUBLE,
        PRIMARY KEY ((account), symbol, trade_id)
    ) WITH CLUSTERING ORDER BY (symbol ASC, trade_id DESC)
"""

SELECT_USER_ACCOUNTS = """
    SELECT username, account_number, name, cash_balance
    FROM accounts_by_user
    WHERE username = ?
"""

SELECT_POSITIONS_BY_ACCOUNT = """
    SELECT symbol, quantity
    FROM positions_by_account
    WHERE account = ?
"""

SELECT_TRADES_BY_ACCOUNT = """
    SELECT trade_id, type, symbol, shares, price, amount
    FROM {table}
    WHERE account = ?
"""

# mapping of logical table keys to physical table names and which extra filters they support
TRADE_TABLES = {
    'by_date': {'table': 'trades_by_a_d', 'supports': []},
    'by_type': {'table': 'trades_by_a_td', 'supports': ['type']},
    'by_symbol_type': {'table': 'trades_by_a_std', 'supports': ['symbol', 'type']},
    'by_symbol': {'table': 'trades_by_a_sd', 'supports': ['symbol']},
}

def execute_batch(session, stmt, data):
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        chunk = data[i : i+batch_size]
        for item in chunk:
            batch.add(stmt, item)
        log.info(f"Executing batch: statements={len(chunk)}")
        # retry individual batch execution to provide clearer user feedback on failures
        last_exc = None
        for attempt in range(1, 4):
            try:
                session.execute(batch, timeout=30)
                break
            except Exception as e:
                last_exc = e
                log.warning(f"Batch execution failed (attempt {attempt}/3): {e}")
                if attempt < 3:
                    time.sleep(2 * attempt)
                else:
                    # final failure: log and print a concise error for the user
                    log.error(f"Batch execution failed after retries: {e}")
                    print(f"ERROR: failed to execute a batch of {len(chunk)} statements: {e}", file=sys.stderr)
                    raise
    log.info(f"Finished executing batches for statement; total_statements={len(data)}")


def bulk_insert(session):
    acc_stmt = session.prepare("INSERT INTO accounts_by_user (username, account_number, cash_balance, name) VALUES (?, ?, ?, ?)")
    pos_stmt = session.prepare("INSERT INTO positions_by_account(account, symbol, quantity) VALUES (?, ?, ?)")
    # prepare statements for all trade tables to ensure consistent inserts
    tad_stmt = session.prepare("INSERT INTO trades_by_a_d (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    tat_stmt = session.prepare("INSERT INTO trades_by_a_td (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    tast_stmt = session.prepare("INSERT INTO trades_by_a_std (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    tasd_stmt = session.prepare("INSERT INTO trades_by_a_sd (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    accounts = []

    # Load configuration from fixtures
    accounts_num = DEMO_CONFIG['accounts_count']
    positions_by_account = DEMO_CONFIG['positions_per_account']
    trades_by_account = DEMO_CONFIG['trades_per_account']
   
    # Generate accounts by user
    log.info(f"Populating demo: accounts={accounts_num}, positions={positions_by_account}, trades={trades_by_account}")
    data = []
    for i in range(accounts_num):
        user = random.choice(DEMO_USERS)
        account_number = str(uuid.uuid4())
        accounts.append(account_number)
        cash_balance = random.uniform(0.1, 100000.0)
        data.append((user[0], account_number, cash_balance, user[1]))
    execute_batch(session, acc_stmt, data)
    log.info(f"Inserted {len(data)} accounts")
    
   
    # Generate positions by account
    # Using a set to track seen (account, symbol) pairs for uniqueness (more efficient than string keys)
    seen_positions = set()
    data = []
    max_attempts = 10000  # Prevent infinite loops on very large datasets
    attempts = 0
    while len(data) < positions_by_account and attempts < max_attempts:
        acc = random.choice(accounts)
        sym = random.choice(DEMO_INSTRUMENTS)
        pos_key = (acc, sym)
        if pos_key not in seen_positions:
            seen_positions.add(pos_key)
            quantity = random.randint(1, 500)
            data.append((acc, sym, quantity))
        attempts += 1
    execute_batch(session, pos_stmt, data)
    log.info(f"Inserted {len(data)} positions")

    # Generate trades by account
    tad_data = []
    tat_data = []
    tast_data = []
    tasd_data = []
    for i in range(trades_by_account):
        trade_id = random_date(datetime.datetime(2020, 1, 1), datetime.datetime.now())
        acc = random.choice(accounts)
        sym = random.choice(DEMO_INSTRUMENTS)
        trade_type = random.choice(['buy', 'sell'])
        shares = random.randint(1, 5000)
        price = random.uniform(0.1, 100000.0)
        amount = shares * price
        # append same data tuple to each trade-table batch so all trade tables stay consistent
        tup = (acc, trade_id, trade_type, sym, shares, price, amount)
        tad_data.append(tup)
        tat_data.append(tup)
        tast_data.append(tup)
        tasd_data.append(tup)
    # execute batches for each trades table (ensure identical data is written to each table)
    log.info(f"Inserting {len(tad_data)} trades into all trade tables")
    execute_batch(session, tad_stmt, tad_data)
    execute_batch(session, tat_stmt, tat_data)
    execute_batch(session, tast_stmt, tast_data)
    execute_batch(session, tasd_stmt, tasd_data)
    log.info("Finished inserting trades into all trade tables")

    # Return a summary so callers can present a user-friendly output
    return {
        'accounts': accounts,
        'accounts_count': accounts_num,
        'positions_count': positions_by_account,
        'trades_count': trades_by_account,
    }


def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    rand_date = start_date + datetime.timedelta(days=random_number_of_days)
    return time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(rand_date))


def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    execute_with_retries(session, CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    execute_with_retries(session, CREATE_USERS_TABLE)
    execute_with_retries(session, CREATE_POSITIONS_BY_ACCOUNT_TABLE)
    execute_with_retries(session, CREATE_TRADES_BY_ACCOUNT_DATE_TABLE)
    execute_with_retries(session, CREATE_TRADES_BY_ACCOUNT_TYPE_TABLE)
    execute_with_retries(session, CREATE_TRADES_BY_ACCOUNT_SYMBOL_TYPE_TABLE)
    execute_with_retries(session, CREATE_TRADES_BY_ACCOUNT_SYMBOL_TABLE)


def execute_with_retries(session, cql, retries=3, timeout=30, delay=5):
    """Execute a CQL statement with retries and a longer timeout for schema changes.

    Uses session.execute(cql, timeout=timeout) and retries on timeout/connection errors.
    """
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            log.info(f"Executing CQL (attempt {attempt}/{retries}): {cql.splitlines()[0]!r}")
            session.execute(cql, timeout=timeout)
            # small pause to give the cluster time to refresh schema
            time.sleep(0.2)
            return
        except Exception as e:
            last_exc = e
            log.warning(f"CQL execution failed (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay * attempt)
            else:
                log.error(f"Giving up executing CQL after {retries} attempts")
                raise


def get_user_accounts(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])

    rows_list = []
    for row in rows:
        name = row.name if hasattr(row, 'name') and row.name is not None else ''
        balance = float(row.cash_balance) if hasattr(row, 'cash_balance') and row.cash_balance is not None else 0.0
        balance_str = f"${balance:,.2f}"
        rows_list.append([row.account_number, name, balance_str])

    if not rows_list:
        print(f"No accounts found for user {username}.")
        return

    headers = ['Account', 'Name', 'Cash Balance']
    print(tabulate(rows_list, headers=headers, tablefmt='github'))


def get_positions_by_account(session, account):
    """Print positions for a given account in a human readable table."""
    log.info(f"Retrieving positions for account {account}")
    stmt = session.prepare(SELECT_POSITIONS_BY_ACCOUNT)
    rows = session.execute(stmt, [account])

    print(f"Positions for account {account}:")
    rows_list = []
    for row in rows:
        rows_list.append([row.symbol, f"{row.quantity:,}"])

    if not rows_list:
        print('No positions found for this account.')
        return

    headers = ['Symbol', 'Quantity']
    print(tabulate(rows_list, headers=headers, tablefmt='github'))


def parse_date_string(s):
    if not s:
        return None
    for fmt in ('%Y-%m-%d', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.datetime.strptime(s, fmt)
        except Exception:
            continue
    try:
        # try fromisoformat as a last resort
        return datetime.datetime.fromisoformat(s)
    except Exception:
        return None


def get_trades_by_account(session, account, start_date=None, end_date=None, limit=100, table_key='by_date', trade_type=None, symbol=None):
    """Print trades for an account from the selected trade table.

    table_key: one of TRADE_TABLES keys ('by_date','by_type','by_symbol','by_symbol_type')
    trade_type, symbol: optional filters used depending on table schema
    """
    log.info(f"Retrieving trades for account {account} from {start_date} to {end_date} on {table_key}")

    if table_key not in TRADE_TABLES:
        raise ValueError('unknown trade table key')

    start_dt = parse_date_string(start_date)
    end_dt = parse_date_string(end_date)

    table_name = TRADE_TABLES[table_key]['table']
    supports = TRADE_TABLES[table_key]['supports']

    # build base CQL using the parameterized SELECT template
    cql = SELECT_TRADES_BY_ACCOUNT.format(table=table_name)
    params = [account]

    # include optional filters when provided and supported by the table
    if 'symbol' in supports and symbol:
        cql += ' AND symbol = ?'
        params.append(symbol)
    if 'type' in supports and trade_type:
        cql += ' AND type = ?'
        params.append(trade_type)

    # translate start/end dates to timeuuid bounds when present
    if start_dt and end_dt:
        start_tu = time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(start_dt))
        end_tu = time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(end_dt))
        cql += ' AND trade_id >= ? AND trade_id <= ?'
        params.extend([start_tu, end_tu])
    elif start_dt:
        start_tu = time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(start_dt))
        cql += ' AND trade_id >= ?'
        params.append(start_tu)
    elif end_dt:
        end_tu = time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(end_dt))
        cql += ' AND trade_id <= ?'
        params.append(end_tu)

    # Apply limit client-side for consistency with prepared statement pattern
    stmt = session.prepare(cql)
    rows = session.execute(stmt, params)

    rows_list = []
    row_count = 0
    for row in rows:
        if row_count >= limit:
            break
        
        trade_id = row.trade_id
        trade_ts = None
        try:
            if hasattr(trade_id, 'time'):
                ts = (trade_id.time - 0x01b21dd213814000) / 1e7
                trade_ts = datetime.datetime.fromtimestamp(ts)
        except Exception:
            trade_ts = None

        when = trade_ts.isoformat(sep=' ') if trade_ts else str(trade_id)
        shares_str = f"{row.shares:,}"
        price_str = f"${float(row.price):,.2f}"
        amount_str = f"${float(row.amount):,.2f}"
        rows_list.append([when, row.type, row.symbol, shares_str, price_str, amount_str])
        row_count += 1

    if not rows_list:
        print('No trades found for this account with the given filters.')
        return

    headers = ['Datetime/ID', 'Type', 'Symbol', 'Shares', 'Price', 'Amount']
    print(tabulate(rows_list, headers=headers, tablefmt='github'))