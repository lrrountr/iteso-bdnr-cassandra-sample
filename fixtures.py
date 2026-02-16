#!/usr/bin/env python3
"""
Sample data fixtures for the investments demo application.

This module contains:
- User and instrument data for demo population
- Configuration for demo data sizes
- Can be easily customized or swapped for different datasets
"""

# Demo users (username, full_name)
DEMO_USERS = [
    ('mike', 'Michael Jones'),
    ('stacy', 'Stacy Malibu'),
    ('john', 'John Doe'),
    ('marie', 'Marie Condo'),
    ('tom', 'Tomas Train')
]

# Demo trading instruments (stock symbols)
DEMO_INSTRUMENTS = [
    'ETSY', 'PINS', 'SE', 'SHOP', 'SQ', 'MELI', 'ISRG', 'DIS', 'BRK.A', 'AMZN',
    'VOO', 'VEA', 'VGT', 'VIG', 'MBB', 'QQQ', 'SPY', 'BSV', 'BND', 'MUB',
    'VSMPX', 'VFIAX', 'FXAIX', 'VTSAX', 'SPAXX', 'VMFXX', 'FDRXX', 'FGXX'
]

# Configuration for demo data sizes
# Adjust these to create smaller/larger datasets for testing
DEMO_CONFIG = {
    'accounts_count': 10,      # Number of accounts to create
    'positions_per_account': 100,  # Total positions across all accounts
    'trades_per_account': 1000,    # Total trades across all accounts
}
