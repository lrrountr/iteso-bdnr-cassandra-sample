# ITESO BDNR - Cassandra Sample

A sample investment portfolio application demonstrating Cassandra data modeling patterns with a REST API architecture.

## Architecture

```
┌────────────┐       ┌────────────┐       ┌────────────┐
│   Client   │ HTTP  │   Server   │ CQL   │ Cassandra  │
│   (CLI)    │ ────► │ (REST API) │ ────► │ (Docker)   │
└────────────┘       └────────────┘       └────────────┘
     client/              server/           port 9042
```

## Project Structure

```
iteso-bdnr-cassandra-sample/
├── server/
│   ├── app.py          # Falcon application and routes
│   ├── resources.py    # REST endpoint handlers
│   └── model.py        # Cassandra schema and queries
├── client/
│   └── cli.py          # Command-line client
├── data/
│   ├── users.csv
│   └── instruments.csv
├── requirements.txt
└── README.md
```

## Data Model

Six tables, all access patterns start from a known username (like a logged-in session):

```
username → accounts_by_user → account_id → positions_by_account
                                         → trades_by_a_d   (all trades, by date)
                                         → trades_by_a_td  (filter by type)
                                         → trades_by_a_sd  (filter by symbol)
                                         → trades_by_a_std (filter by symbol + type)
```

| Table | Partition Key | Purpose |
|-------|--------------|---------|
| `accounts_by_user` | username | List accounts for a user |
| `positions_by_account` | account_id | Current holdings per account |
| `trades_by_a_d` | account_id | Trade history — date range queries |
| `trades_by_a_td` | account_id | Trade history — filter by type |
| `trades_by_a_sd` | account_id | Trade history — filter by symbol |
| `trades_by_a_std` | account_id | Trade history — filter by symbol + type |

The API automatically selects the right trade table based on your query filters.

## Setup

You will need **2 terminal windows**: one for the server, one for the CLI.

### Step 1: Start Cassandra

```bash
docker run --name cassandra -p 9042:9042 -d cassandra

# Wait ~60 seconds for Cassandra to initialize, then verify:
docker exec -it cassandra cqlsh -e "describe cluster"
```

### Step 2: Install Dependencies

```bash
python3 -m venv venv
source venv/bin/activate        # Linux/Mac
# .\venv\Scripts\Activate.ps1   # Windows

pip install -r requirements.txt
```

### Step 3: Start the API Server

```bash
cd server
uvicorn app:app --reload --port 5000
```

### Step 4: Create the Schema

```bash
cd client
source ../venv/bin/activate

python cli.py setup
```

### Step 5: (Optional) Load Demo Data

```bash
python cli.py seed --accounts 5 --trades 10
```

This inserts demo data by calling the real business endpoints — the same code path as actual trades.

## CLI Commands

### Admin

| Command | Description |
|---------|-------------|
| `status` | Check if API is running |
| `setup` | Create keyspace and all 6 tables (DDL only) |
| `seed --accounts N --trades N` | Insert demo data |

### Portfolio Actions

| Command | Description |
|---------|-------------|
| `accounts --username NAME` | List accounts for a user |
| `open-account --username NAME --name NAME --balance N` | Open a new account |
| `buy --username NAME --account ID --symbol SYM --shares N --price N` | Place a buy order |
| `sell --username NAME --account ID --symbol SYM --shares N --price N` | Place a sell order |
| `portfolio --account ID` | View current holdings |
| `history --account ID [filters]` | View trade history |

### Typical Session

```bash
# 1. List accounts for a user (username is always the starting point)
python cli.py accounts --username alice

# 2. Open a new account
python cli.py open-account --username alice --name "Alice's Portfolio" --balance 50000

# 3. Trade  (--username is required for the cash balance update)
python cli.py buy  --username alice --account <id> --symbol AAPL --shares 10 --price 180.50
python cli.py sell --username alice --account <id> --symbol AAPL --shares  5 --price 190.00

# 4. Check holdings
python cli.py portfolio --account <id>

# 5. View trade history — each filter routes to a different Cassandra table
python cli.py history --account <id>                           # → trades_by_a_d
python cli.py history --account <id> --symbol AAPL            # → trades_by_a_sd
python cli.py history --account <id> --type buy               # → trades_by_a_td
python cli.py history --account <id> --symbol AAPL --type buy # → trades_by_a_std
```

### Trade Output

Every `buy` or `sell` prints the 6 Cassandra writes it triggered:

```
Buying 10x AAPL @ $180.50  (total $1,805.00)

Order executed!

  Trade ID:      ...
  Bought:        10x AAPL
  Price:         $180.50
  Total:         $1,805.00
  Cash balance:  $48,195.00

Cassandra writes (6):
  [1/6] trades_by_a_d         ← all trades — date range queries
  [2/6] trades_by_a_td        ← trades filtered by type
  [3/6] trades_by_a_std       ← trades filtered by symbol + type
  [4/6] trades_by_a_sd        ← trades filtered by symbol
  [5/6] positions_by_account  ← AAPL: 0 → 10 shares
  [6/6] accounts_by_user      ← cash: $50,000.00 → $48,195.00
```

## REST API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/setup` | Create keyspace + 6 tables (DDL only) |
| POST | `/seed` | Insert demo data |
| GET | `/accounts?username=X` | List accounts for a user |
| POST | `/accounts` | Open a new account |
| GET | `/accounts/{id}/portfolio` | Current holdings |
| POST | `/accounts/{id}/trades` | Execute a trade (6 writes) |
| GET | `/accounts/{id}/trades` | Trade history (routes across 4 tables) |

### Example API Calls

```bash
curl http://localhost:5000/health
curl -X POST http://localhost:5000/setup
curl "http://localhost:5000/accounts?username=alice"

curl -X POST http://localhost:5000/accounts \
  -H "Content-Type: application/json" \
  -d '{"username": "alice", "name": "Alice Portfolio", "initial_balance": 50000}'

curl -X POST http://localhost:5000/accounts/<id>/trades \
  -H "Content-Type: application/json" \
  -d '{"username": "alice", "type": "buy", "symbol": "AAPL", "shares": 10, "price": 180.50}'

curl "http://localhost:5000/accounts/<id>/trades?symbol=AAPL&type=buy"
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `API_URL` | `http://localhost:5000` | API URL (client) |
| `CASSANDRA_HOST` | `localhost` | Cassandra host (server) |
| `CASSANDRA_PORT` | `9042` | Cassandra port (server) |
| `CASSANDRA_KEYSPACE` | `investments` | Keyspace name (server) |

## Troubleshooting

**"Cannot connect to API"** — make sure the server is running: `cd server && uvicorn app:app --reload --port 5000`

**"Failed to connect to Cassandra"** — wait ~60s after starting Docker, then check: `docker exec -it cassandra cqlsh -e "describe cluster"`

**"Account not found"** — run `setup` first, then `seed` or `open-account`
