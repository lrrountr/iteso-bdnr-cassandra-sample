#!/usr/bin/env python3
import argparse
import logging
import os
import random
import sys

from cassandra.cluster import Cluster

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def get_instrument_value(instrument):
    instr_mock_sum = sum(bytearray(instrument, encoding='utf-8'))
    return random.uniform(1.0, instr_mock_sum)


def main():
    parser = argparse.ArgumentParser(description='Investments demo (Cassandra Python driver)')
    parser.add_argument('--cluster-ips', default=CLUSTER_IPS, help='Comma-separated Cassandra contact points')
    parser.add_argument('--keyspace', default=KEYSPACE, help='Keyspace to use')
    parser.add_argument('--replication-factor', default=REPLICATION_FACTOR, help='Keyspace replication factor')
    parser.add_argument('--username', dest='global_username', help='Default username for commands')

    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    subparsers.add_parser('populate', help='Populate demo data')

    accounts_parser = subparsers.add_parser('accounts', help='Show accounts for a username')
    accounts_parser.add_argument('--username', '-u', help='Username to lookup (overrides global)')

    positions_parser = subparsers.add_parser('positions', help='Show positions for an account')
    positions_parser.add_argument('--account', '-a', required=True, help='Account identifier')

    trades_parser = subparsers.add_parser('trades', help='Show trade history for an account')
    trades_parser.add_argument('--account', '-a', required=True, help='Account identifier')
    trades_parser.add_argument('--start', help='Start date (YYYY-MM-DD or ISO)')
    trades_parser.add_argument('--end', help='End date (YYYY-MM-DD or ISO)')
    trades_parser.add_argument('--limit', type=int, default=100, help='Maximum number of trades to show')
    trades_parser.add_argument('--type', dest='trade_type', help='Trade type filter (buy/sell)')
    trades_parser.add_argument('--symbol', help='Instrument symbol filter')

    args = parser.parse_args()

    log.info(f"Running command: {args.command} args={args}")

    log.info("Connecting to Cluster")
    cluster = None
    session = None
    try:
        cluster = Cluster(args.cluster_ips.split(','))
        session = cluster.connect()

        model.create_keyspace(session, args.keyspace, args.replication_factor)
        session.set_keyspace(args.keyspace)

        model.create_schema(session)

        if args.command == 'populate':
            result = model.bulk_insert(session)

            accounts = result.get('accounts', [])
            print('\nPopulate summary:')
            print(f"- Accounts created: {result.get('accounts_count', len(accounts))}")
            print(f"- Positions created: {result.get('positions_count', 'unknown')}")
            print(f"- Trades created: {result.get('trades_count', 'unknown')}")
            show_n = 10
            print(f"- Sample account IDs (first {min(show_n, len(accounts))}):")
            for a in accounts[:show_n]:
                print('   ', a)
            log.info(f"Populate finished: accounts={len(accounts)}, positions={result.get('positions_count')}, trades={result.get('trades_count')}")
        elif args.command == 'accounts':
            username = args.username or args.global_username
            if not username:
                parser.error('accounts requires --username or provide --username globally')
            model.get_user_accounts(session, username)
        elif args.command == 'positions':
            log.info(f"Querying positions for account {args.account}")
            model.get_positions_by_account(session, args.account)
        elif args.command == 'trades':
            # dynamically pick the best trade table based on provided filters
            if args.symbol and args.trade_type:
                table_key = 'by_symbol_type'
            elif args.symbol:
                table_key = 'by_symbol'
            elif args.trade_type:
                table_key = 'by_type'
            else:
                table_key = 'by_date'

            log.info(f"Querying trades for account {args.account} selected_table={table_key} type={getattr(args,'trade_type',None)} symbol={getattr(args,'symbol',None)} limit={args.limit}")
            model.get_trades_by_account(
                session,
                args.account,
                start_date=args.start,
                end_date=args.end,
                limit=args.limit,
                table_key=table_key,
                trade_type=getattr(args, 'trade_type', None),
                symbol=getattr(args, 'symbol', None),
            )
    except Exception as e:
        log.exception('Unhandled error')
        print(f"ERROR: operation failed: {e}. See investments.log for details.", file=sys.stderr)
        sys.exit(1)
    finally:
        if session:
            try:
                session.shutdown()
            except Exception:
                pass
        if cluster:
            try:
                cluster.shutdown()
            except Exception:
                pass


if __name__ == '__main__':
    main()
