# Personal Finance Helper

A personal finance analytics dashboard that transforms ING Bank Netherlands CSV exports into actionable insights — running balances, category breakdowns, and spend-over-time charts.

## Architecture

```
personal-finance-helper/
├── pipeline.py       # ETL: extract → transform → load (CSV/Excel → SQLite/PG)
├── query_layer.py    # Named SQL queries + QueryExecutor → DataFrames
├── app.py            # Streamlit dashboard (3 tabs + sidebar filters)
├── explore_data.py   # Schema discovery script (run once on new data)
├── test_etl.py       # 35 ETL tests (pytest)
├── test_queries.py   # 39 query layer tests (pytest)
├── Dockerfile        # python:3.11-slim image
├── docker-compose.yml
├── .env.example
└── data/             # gitignored — put bank.csv here
```

## Standard Transaction Schema

| Column | Type | Description |
|---|---|---|
| `transaction_type` | str | Payment terminal, Transfer, iDEAL, etc. |
| `date` | date | Parsed from YYYYMMDD integer |
| `description` | str | Merchant / counterparty name |
| `amount` | float | Positive = credit, Negative = debit |
| `currency` | str | Always `EUR` for ING NL |
| `category` | str | Rule-based: Groceries, Transport, Dining, … |
| `account` | str | Account IBAN |
| `status` | str | Always `verified` |

## Quick Start

### Option A — Local (SQLite, no Docker)

```bash
pip install -r requirements.txt

# Run the app (SQLite auto-created at finance.db)
streamlit run app.py
```

Upload your ING Bank CSV in the **Data Management** tab. That's it.

### Option B — Docker (recommended for persistence)

```bash
cp .env.example .env          # review settings if needed
docker compose up --build
```

Open **http://localhost:8501**. The SQLite database is stored in a named Docker volume (`finance_db`) — data persists across restarts.

**Optional PostgreSQL:** uncomment the `db` service in `docker-compose.yml` and update `DATABASE_URL`.

### Run Tests

```bash
pytest test_etl.py test_queries.py -v
# 74 tests, all passing
```

### ETL Pipeline (CLI)

```bash
# Load a CSV directly without the UI
python pipeline.py --file data/bank.csv --db sqlite:///finance.db
```

## Data Privacy

`data/`, `*.csv`, `*.xlsx`, `*.xls`, `*.db` are all gitignored. Your bank data never leaves your machine.
