#!/usr/bin/env python3
"""
Falcon ASGI application for the Investments REST API.
Connects to Cassandra and exposes REST endpoints.
"""
import logging
import os

import falcon.asgi

from model import CassandraConnection
from resources import (
    HealthResource,
    SetupResource,
    SeedResource,
    AccountsResource,
    PortfolioResource,
    TradesResource,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger(__name__)

# Read configuration from environment
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')
CASSANDRA_PORT = int(os.getenv('CASSANDRA_PORT', '9042'))
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
CASSANDRA_REPLICATION_FACTOR = int(os.getenv('CASSANDRA_REPLICATION_FACTOR', '1'))


class LoggingMiddleware:
    """Middleware for request/response logging."""

    async def process_request(self, req, resp):
        log.info(f"Request: {req.method} {req.uri}")

    async def process_response(self, req, resp, resource, req_succeeded):
        log.info(f"Response: {resp.status} for {req.method} {req.uri}")


class CORSMiddleware:
    """Middleware for CORS headers."""

    async def process_response(self, req, resp, resource, req_succeeded):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.set_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        resp.set_header('Access-Control-Allow-Headers', 'Content-Type')


def create_app():
    """Create and configure the Falcon application."""
    # Initialize Cassandra connection
    connection = CassandraConnection(
        hosts=[CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        keyspace=CASSANDRA_KEYSPACE,
        replication_factor=CASSANDRA_REPLICATION_FACTOR,
    )

    # Connect to Cassandra
    try:
        connection.connect()
        log.info(f"Connected to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}")
    except Exception as e:
        log.error(f"Failed to connect to Cassandra: {e}")
        raise

    # Create Falcon app with middleware
    app = falcon.asgi.App(middleware=[
        LoggingMiddleware(),
        CORSMiddleware(),
    ])

    # Instantiate resources
    health_resource    = HealthResource(connection)
    setup_resource     = SetupResource(connection)
    seed_resource      = SeedResource(connection)
    accounts_resource  = AccountsResource(connection)
    portfolio_resource = PortfolioResource(connection)
    trades_resource    = TradesResource(connection)

    # Register routes
    app.add_route('/health',                          health_resource)
    app.add_route('/setup',                           setup_resource)
    app.add_route('/seed',                            seed_resource)
    app.add_route('/accounts',                        accounts_resource)
    app.add_route('/accounts/{account_id}/portfolio', portfolio_resource)
    app.add_route('/accounts/{account_id}/trades',    trades_resource)

    log.info("Routes:")
    log.info("  GET  /health")
    log.info("  POST /setup                          — DDL only (6 tables)")
    log.info("  POST /seed                           — demo data via business logic")
    log.info("  GET  /accounts?username=X            — entry point, returns account_ids")
    log.info("  POST /accounts                       — open account (1 write)")
    log.info("  GET  /accounts/{id}/portfolio        — holdings from positions_by_account")
    log.info("  POST /accounts/{id}/trades           — execute trade (6 writes)")
    log.info("  GET  /accounts/{id}/trades           — history (routes across 4 trade tables)")

    return app


# Create the application instance
app = create_app()
