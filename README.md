# iteso-bdnr-cassandra

A place to share cassandra app code

### Setup a python virtual env with python cassandra installed
```
# If pip is not present in your system
sudo apt update
sudo apt install python3-pip

# Install and activate virtual env (Linux/MacOS)
python3 -m pip install virtualenv
python3 -m venv ./venv
source ./venv/bin/activate

# Install and activate virtual env (Windows)
python3 -m pip install virtualenv
python3 -m venv ./venv
.\venv\Scripts\Activate.ps1

# Install project python requirements
pip install -r requirements.txt
```


### Launch cassandra container
```
# To start a new container
docker run --name node01 -p 9042:9042 -d cassandra

# If container already exists just start it
docker start node01

# Wait for Cassandra to be ready (10-30 seconds)
# The app will retry connections, so it's OK to start immediately
```

### Start a Cassandra cluster with 2 nodes
```
# Recipe to create a cassandra cluster using docker
docker run --name node01 -p 9042:9042 -d cassandra
docker run --name node02 -d --link node01:cassandra cassandra

# Wait for containers to be fully initialized, verify node status
docker exec -it node01 nodetool status

# Note: --link is deprecated; consider using Docker compose for production setups
```

## Project Structure

- **app.py**: Main CLI application with argparse-based subcommands (populate, accounts, positions, trades)
- **model.py**: Data layer with schema creation, prepared statements, batch operations, and query functions
- **fixtures.py**: Demo data (users, instruments, configuration) - easily customizable for different dataset sizes
- **requirements.txt**: Python dependencies with pinned versions for reproducibility

## Usage (CLI)

After starting Cassandra and installing Python deps (see above), use the CLI in `app.py`.

- Show help:
```bash
python3 app.py --help
```

- Populate demo data (creates keyspace, tables, and inserts rows):
```bash
python3 app.py populate
```
This will print a short summary and sample account IDs when finished.

- List accounts for a user (prints `account_number` values you can use with `positions`/`trades`):
```bash
python3 app.py accounts --username mike
```

- Show positions for an account:
```bash
python3 app.py positions --account <account-id>
```

- Show trades for an account (optional date range):
```bash
python3 app.py trades --account <account-id> --start 2020-01-01 --end 2020-12-31 --limit 50
```

Table selection:
- The CLI automatically selects the most appropriate internal trade table based on supplied filters:
	- If you pass both `--symbol` and `--type` it uses the symbol+type table (best for that filter).
	- If you pass only `--symbol` it uses the symbol table.
	- If you pass only `--type` it uses the type table.
	- If you pass neither it uses the default recent-by-date table.

Examples:
```bash
# Default (by date)
python3 app.py trades --account <account-id> --limit 5

# Filter by symbol -> uses symbol-backed table
python3 app.py trades --account <account-id> --symbol ETSY --limit 10

# Filter by type -> uses type-backed table
python3 app.py trades --account <account-id> --type buy --limit 10

# Filter by symbol and type -> uses symbol+type-backed table
python3 app.py trades --account <account-id> --symbol AMZN --type sell --limit 25
```

Notes:
- To point to a remote Cassandra cluster, use `--cluster-ips` and `--keyspace` flags.
- The `populate` command prints a short sample of created account IDs (first 10) — use those for quick testing.

## Customizing Demo Data

Edit `fixtures.py` to adjust the demo dataset:

```python
# fixtures.py - customize these for different test scenarios:
DEMO_USERS = [...]           # List of demo users
DEMO_INSTRUMENTS = [...]     # Stock symbols to use
DEMO_CONFIG = {
    'accounts_count': 10,           # Number of accounts
    'positions_per_account': 100,   # Total positions 
    'trades_per_account': 1000,     # Total trades
}
```

For example, to create a smaller dataset for quick testing:
```python
DEMO_CONFIG = {
    'accounts_count': 2,
    'positions_per_account': 10,
    'trades_per_account': 50,
}
```
