#!/usr/bin/env python3
"""
Investments CLI — portfolio management client.

The user is always identified first (username is the entry point, like a logged-in session).
The typical flow mirrors how a real app works:

  1. Know the user                 →  username (given, like an auth token)
  2. List their accounts           →  accounts --username alice
  3. Open a new account            →  open-account --username alice ...
  4. Trade                         →  buy / sell  (username required for balance update)
  5. Check holdings                →  portfolio --account <id>
  6. Review history                →  history --account <id> [filters]

Admin (run once):
  setup   — create keyspace + 6 tables
  seed    — populate demo data
"""
import argparse
import os
import sys

import requests
from tabulate import tabulate

API_URL = os.getenv('API_URL', 'http://localhost:5000')


def _err(msg):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def _get(path, **params):
    try:
        return requests.get(f"{API_URL}{path}", params=params or None, timeout=30)
    except requests.exceptions.ConnectionError:
        _err(f"Cannot connect to API at {API_URL}")


def _post(path, body=None, timeout=60):
    try:
        return requests.post(f"{API_URL}{path}", json=body, timeout=timeout)
    except requests.exceptions.ConnectionError:
        _err(f"Cannot connect to API at {API_URL}")


def _print_writes(writes):
    print(f"\nCassandra writes ({len(writes)}):")
    for i, w in enumerate(writes, 1):
        print(f"  [{i}/{len(writes)}] {w['table']:<28} ← {w['description']}")


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

def cmd_status():
    r = _get('/health')
    if r.ok:
        d = r.json()
        print(f"API:      {d.get('status')}")
        print(f"Database: {d.get('database')}")
    else:
        _err(f"HTTP {r.status_code}")


def cmd_setup():
    r = _post('/setup', timeout=120)
    if r.ok:
        d = r.json()
        print(f"OK: {d['message']}\n")
        print("Tables created:")
        for t in d.get('tables', []):
            print(f"  {t}")
    else:
        _err(f"Setup failed: {r.text}")


def cmd_seed(accounts, trades_per_account):
    print(f"Seeding {accounts} accounts × {trades_per_account} trades …")
    r = _post('/seed', {'accounts': accounts, 'trades_per_account': trades_per_account}, timeout=300)
    if r.ok:
        d = r.json()
        print(f"OK: {d['accounts_created']} accounts, {d['trades_created']} trades inserted\n")
        for entry in d.get('sample_accounts', []):
            print(f"  {entry['username']:<20} {entry['account_id']}")
    else:
        _err(f"Seed failed: {r.text}")


# ---------------------------------------------------------------------------
# Portfolio actions
# ---------------------------------------------------------------------------

def cmd_accounts(username):
    """
    Entry point: list all accounts for a user.
    Reads from accounts_by_user — username is always the starting point.
    """
    r = _get('/accounts', username=username)
    if not r.ok:
        _err(f"Failed: {r.text}")

    d = r.json()
    rows = d.get('accounts', [])
    if not rows:
        print(f"No accounts found for '{username}'")
        return

    table = [[a['account_id'], a['name'], f"${a['cash_balance']:,.2f}"] for a in rows]
    print(tabulate(table, headers=['Account ID', 'Name', 'Cash Balance'], tablefmt='github'))
    print(f"\n{len(rows)} account(s) for '{username}'")


def cmd_open_account(username, name, balance):
    """Open a new investment account — single write to accounts_by_user."""
    r = _post('/accounts', {'username': username, 'name': name, 'initial_balance': balance})
    if not r.ok:
        _err(f"Failed: {r.text}")

    d = r.json()
    print(f"Account opened!\n")
    print(f"  Account ID:  {d['account_id']}")
    print(f"  Owner:       {d['username']}")
    print(f"  Name:        {d['name']}")
    print(f"  Balance:     ${d['cash_balance']:,.2f}")
    _print_writes(d['writes'])


def cmd_buy(username, account_id, symbol, shares, price):
    """
    Place a buy order.
    username is required — the server needs it to update the cash balance
    in accounts_by_user (partition key is username).
    In a real app this comes from the authenticated session.
    """
    _execute_order(username, account_id, 'buy', symbol, shares, price)


def cmd_sell(username, account_id, symbol, shares, price):
    _execute_order(username, account_id, 'sell', symbol, shares, price)


def _execute_order(username, account_id, order_type, symbol, shares, price):
    amount = shares * price
    verb = "Buying" if order_type == 'buy' else "Selling"
    print(f"{verb} {shares}x {symbol} @ ${price:,.2f}  (total ${amount:,.2f}) …\n")

    r = _post(
        f'/accounts/{account_id}/trades',
        {'username': username, 'type': order_type, 'symbol': symbol, 'shares': shares, 'price': price},
    )
    if not r.ok:
        _err(f"Order failed: {r.text}")

    d = r.json()
    print(f"Order executed!\n")
    print(f"  Trade ID:      {d['trade_id']}")
    print(f"  {'Bought' if order_type == 'buy' else 'Sold'}:         {d['shares']:,}x {d['symbol']}")
    print(f"  Price:         ${d['price']:,.2f}")
    print(f"  Total:         ${d['amount']:,.2f}")
    print(f"  Cash balance:  ${d['cash_balance_after']:,.2f}")
    _print_writes(d['writes'])


def cmd_portfolio(account_id):
    """View current holdings — reads from positions_by_account."""
    r = _get(f'/accounts/{account_id}/portfolio')
    if not r.ok:
        _err(f"Failed: {r.text}")

    d = r.json()
    positions = d.get('positions', [])

    if not positions:
        print(f"No positions for account {account_id}")
        return

    table = [[p['symbol'], f"{p['quantity']:,}"] for p in positions]
    print(f"Holdings for account {account_id}:\n")
    print(tabulate(table, headers=['Symbol', 'Shares held'], tablefmt='github'))
    print(f"\n{len(positions)} position(s)")


def cmd_history(account_id, start=None, end=None, trade_type=None, symbol=None, limit=100):
    """
    View trade history. Different filters route to different Cassandra tables:

      no filter          →  trades_by_a_d    (all trades by date)
      --symbol AAPL      →  trades_by_a_sd   (trades by symbol)
      --type buy         →  trades_by_a_td   (trades by type)
      --symbol + --type  →  trades_by_a_std  (trades by symbol + type)
    """
    params = {'limit': limit}
    if start:       params['start'] = start
    if end:         params['end'] = end
    if trade_type:  params['type'] = trade_type
    if symbol:      params['symbol'] = symbol

    r = _get(f'/accounts/{account_id}/trades', **params)
    if not r.ok:
        _err(f"Failed: {r.text}")

    d = r.json()
    routing = d.get('query_routing', {})
    trades = d.get('trades', [])

    print(f"Cassandra table used:  {routing.get('table_used', '?')}")
    print(f"Reason:                {routing.get('reason', '?')}\n")

    if not trades:
        print("No trades found.")
        return

    table = [
        [t['datetime'], t['type'].upper(), t['symbol'],
         f"{t['shares']:,}", f"${t['price']:,.2f}", f"${t['amount']:,.2f}"]
        for t in trades
    ]
    print(tabulate(table, headers=['Date', 'Type', 'Symbol', 'Shares', 'Price', 'Total'], tablefmt='github'))
    print(f"\n{len(trades)} trade(s)  (limit: {limit})")


# ---------------------------------------------------------------------------
# CLI definition
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Investments — portfolio management CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Admin (run once):
  python cli.py setup
  python cli.py seed --accounts 5 --trades 10

Typical session (username is always the starting point):
  python cli.py accounts      --username alice
  python cli.py open-account  --username alice --name "Alice's Portfolio" --balance 50000
  python cli.py buy   --username alice --account <id> --symbol AAPL --shares 10 --price 180.50
  python cli.py sell  --username alice --account <id> --symbol AAPL --shares  5 --price 190.00
  python cli.py portfolio --account <id>

History (watch which Cassandra table is chosen per filter):
  python cli.py history --account <id>
  python cli.py history --account <id> --symbol AAPL
  python cli.py history --account <id> --type buy
  python cli.py history --account <id> --symbol AAPL --type buy
        """
    )
    sub = parser.add_subparsers(dest='command', required=True)

    sub.add_parser('status', help='Check API health')
    sub.add_parser('setup',  help='Create keyspace + 6 tables (run once)')

    p = sub.add_parser('seed', help='Insert demo data')
    p.add_argument('--accounts', type=int, default=5,  help='Number of accounts (default 5)')
    p.add_argument('--trades',   type=int, default=10, help='Trades per account (default 10)')

    p = sub.add_parser('accounts', help='List accounts for a user (entry point)')
    p.add_argument('--username', '-u', required=True)

    p = sub.add_parser('open-account', help='Open a new investment account')
    p.add_argument('--username', '-u', required=True)
    p.add_argument('--name',     '-n', required=True, help='Portfolio name')
    p.add_argument('--balance',  '-b', type=float, default=10000.0)

    p = sub.add_parser('buy', help='Place a buy order (6 Cassandra writes)')
    p.add_argument('--username', '-u', required=True, help='Account owner (needed for balance update)')
    p.add_argument('--account',  '-a', required=True, help='Account ID')
    p.add_argument('--symbol',   '-s', required=True)
    p.add_argument('--shares',         required=True, type=int)
    p.add_argument('--price',          required=True, type=float)

    p = sub.add_parser('sell', help='Place a sell order (6 Cassandra writes)')
    p.add_argument('--username', '-u', required=True, help='Account owner (needed for balance update)')
    p.add_argument('--account',  '-a', required=True, help='Account ID')
    p.add_argument('--symbol',   '-s', required=True)
    p.add_argument('--shares',         required=True, type=int)
    p.add_argument('--price',          required=True, type=float)

    p = sub.add_parser('portfolio', help='View current holdings')
    p.add_argument('--account', '-a', required=True)

    p = sub.add_parser('history', help='View trade history (routes to different tables by filter)')
    p.add_argument('--account', '-a', required=True)
    p.add_argument('--start',  help='From date YYYY-MM-DD')
    p.add_argument('--end',    help='To date YYYY-MM-DD')
    p.add_argument('--type',   dest='trade_type', choices=['buy', 'sell'])
    p.add_argument('--symbol')
    p.add_argument('--limit',  type=int, default=100)

    args = parser.parse_args()

    if   args.command == 'status':       cmd_status()
    elif args.command == 'setup':        cmd_setup()
    elif args.command == 'seed':         cmd_seed(args.accounts, args.trades)
    elif args.command == 'accounts':     cmd_accounts(args.username)
    elif args.command == 'open-account': cmd_open_account(args.username, args.name, args.balance)
    elif args.command == 'buy':          cmd_buy(args.username, args.account, args.symbol, args.shares, args.price)
    elif args.command == 'sell':         cmd_sell(args.username, args.account, args.symbol, args.shares, args.price)
    elif args.command == 'portfolio':    cmd_portfolio(args.account)
    elif args.command == 'history':      cmd_history(args.account, args.start, args.end,
                                                     args.trade_type, args.symbol, args.limit)


if __name__ == '__main__':
    main()
