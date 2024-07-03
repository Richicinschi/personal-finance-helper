# Personal Finance Helper

A personal finance analytics dashboard that turns ING Bank Netherlands CSV exports into
actionable insights — running balances, category breakdowns, and spend-over-time charts.
All data stays on your machine.

---

## Table of Contents

1. [Features](#features)
2. [Prerequisites](#prerequisites)
3. [Quick Start — Local (SQLite)](#quick-start--local-sqlite)
4. [Quick Start — Docker](#quick-start--docker)
5. [First Use Walkthrough](#first-use-walkthrough)
6. [Configuration](#configuration)
7. [CLI — Batch Load Without the UI](#cli--batch-load-without-the-ui)
8. [Running Tests](#running-tests)
9. [Switching to PostgreSQL](#switching-to-postgresql)
10. [Architecture](#architecture)
11. [Standard Transaction Schema](#standard-transaction-schema)
12. [Data Privacy](#data-privacy)
13. [Troubleshooting](#troubleshooting)

---

## Features

- **Data Management tab** — upload your ING Bank CSV or Excel export; choose to replace or
  append data.
- **Analytics tab** — running balance line chart, spend-by-period stacked bar, and category
  breakdown horizontal bar. Sidebar controls: account filter, date range, granularity
  (daily / weekly / monthly).
- **Explorer tab** — group transactions by any column (description, counterparty, transaction
  type, code, debit/credit, date). Choose a metric, pick the top N, drill down into multiple
  values with a pie chart and total sum.
- **ETL pipeline** — ING-specific CSV parser (YYYYMMDD dates, comma-decimal amounts,
  Debit/Credit signing, 30+ auto-categorisation rules). Works with CSV and Excel exports.
- **Portable** — SQLite by default (zero config). Optional PostgreSQL via Docker.

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.11+ | 3.10 works but untested |
| pip | any recent | comes with Python |
| Git | any | to clone the repo |
| Docker + Docker Compose | any recent | **only needed for Option B** |

Check your Python version:

```bash
python --version   # or python3 --version on Mac/Linux
```

---

## Quick Start — Local (SQLite)

No Docker, no database server. SQLite is created automatically.

### 1. Clone the repository

```bash
git clone <repository-url>
cd personal-finance-helper
```

### 2. Create and activate a virtual environment

**Mac / Linux**
```bash
python -m venv .venv
source .venv/bin/activate
```

**Windows (PowerShell)**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

**Windows (Command Prompt)**
```cmd
python -m venv .venv
.venv\Scripts\activate.bat
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. (Optional) Create a local config

```bash
cp .env.example .env
```

The defaults in `.env.example` already point to SQLite (`sqlite:///finance.db`), so this
step is optional for local use. The `.env` file is gitignored — your settings stay private.

### 5. Run the app

```bash
streamlit run app.py
```

The browser opens automatically at `http://localhost:8501`.

---

## Quick Start — Docker

One command. SQLite data is stored in a named Docker volume so it survives restarts.

```bash
cp .env.example .env          # review if needed, defaults are fine
docker compose up --build
```

Open `http://localhost:8501`.

To stop:

```bash
docker compose down
```

To wipe the database volume too:

```bash
docker compose down -v
```

---

## First Use Walkthrough

### Step 1 — Export your ING Bank data

1. Log in to **Mijn ING** at ing.nl
2. Go to **Betaalrekening** → **Transacties**
3. Click **Exporteren** and choose **CSV**
4. Save the file somewhere handy (e.g. `~/Downloads/bank.csv`)

The export will have columns like:
`Date, Name / Description, Account, Counterparty, Code, Debit/credit, Amount (EUR), Transaction type, Notifications`

### Step 2 — Upload in the app

1. Open the app in your browser
2. Go to the **Data Management** tab
3. Under **Upload data file**, click **Browse files** and select your CSV
4. Choose **Replace all data** (default) or **Append to existing** if you are loading
   multiple files
5. Click **Run ETL pipeline**
6. The app shows the total row count and a preview of the processed transactions

### Step 3 — Explore your data

**Analytics tab**
- Use the sidebar to filter by account, date range, and granularity
- **Running Balance** shows your account balance over time
- **Spend by Period** shows inflows and outflows stacked by category
- **Category Breakdown** ranks your spending categories

**Explorer tab**
- Choose a **Group by** column (e.g. Name / Description to see per-merchant totals)
- Choose a **Metric** (total amount, transaction count, average)
- Adjust **Include** to show debits only, credits only, or both
- Use the **Top N** slider to focus on the biggest items
- Expand **Raw transactions for selected group**, pick one or more values to see a
  pie chart and total sum for just those entries

---

## Configuration

Copy `.env.example` to `.env` and edit as needed.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `sqlite:///finance.db` | SQLAlchemy connection URL |
| `STREAMLIT_SERVER_PORT` | `8501` | Override the default port |
| `STREAMLIT_SERVER_ADDRESS` | `0.0.0.0` | Bind address (change to `localhost` for local-only) |

The `.env` file is loaded automatically by `python-dotenv` on startup. It is gitignored
and will never be committed.

---

## CLI — Batch Load Without the UI

Load a CSV directly from the command line (useful for scripting or cron jobs):

```bash
# Replace existing data
python pipeline.py --file data/bank.csv --db sqlite:///finance.db

# Append to existing data (for multiple files)
python pipeline.py --file data/jan.csv --db sqlite:///finance.db --mode append
python pipeline.py --file data/feb.csv --db sqlite:///finance.db --mode append
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--file` | required | Path to the CSV or Excel file |
| `--db` | `sqlite:///finance.db` | SQLAlchemy database URL |
| `--mode` | `replace` | `replace` clears the tables first; `append` adds rows |

---

## Running Tests

```bash
# All tests (76 total)
pytest test_etl.py test_queries.py -v

# ETL pipeline tests only
pytest test_etl.py -v

# Query layer tests only
pytest test_queries.py -v
```

Tests use an in-memory SQLite database — no real data file or network needed.

---

## Switching to PostgreSQL

The app ships with SQLite for zero-config local use. For shared access or production use
PostgreSQL.

### With Docker Compose

1. Open `docker-compose.yml`
2. Uncomment the `db` service block
3. Uncomment `depends_on` in the `app` service
4. Uncomment the `DATABASE_URL` PostgreSQL line and comment out the SQLite line
5. Uncomment `postgres_data` in the `volumes` block
6. Run:

```bash
docker compose up --build
```

### Without Docker

1. Create a PostgreSQL database and user
2. Set `DATABASE_URL` in `.env`:

```
DATABASE_URL=postgresql://finance_user:finance_pass@localhost:5432/finance_db
```

3. Run the app normally — SQLAlchemy creates the tables on first load.

> **Note:** `psycopg2-binary` is already in `requirements.txt`.

---

## Architecture

```
personal-finance-helper/
├── pipeline.py       # ETL: extract -> transform -> load (CSV/Excel -> SQLite/PG)
├── query_layer.py    # Named SQL queries + QueryExecutor -> DataFrames
├── app.py            # Streamlit dashboard (4 tabs + sidebar filters)
├── explore_data.py   # Schema discovery script (run once on new data)
├── test_etl.py       # 37 ETL tests (pytest)
├── test_queries.py   # 39 query layer tests (pytest)
├── Dockerfile        # python:3.11-slim image
├── docker-compose.yml
├── .env.example
└── data/             # gitignored -- put bank.csv here
```

**Data flow:**

```
ING CSV export
     |
  extract()          -- reads CSV, normalises snake_case column names
     |
  transform()        -- parses dates, signs amounts, classifies categories
     |
  load()             -- writes raw_transactions + transactions to DB
     |
QueryExecutor        -- named SQL queries -> DataFrames
     |
  app.py             -- Streamlit charts and tables
```

---

## Standard Transaction Schema

The `transactions` table after the ETL pipeline:

| Column | Type | Description |
|--------|------|-------------|
| `transaction_type` | str | Payment terminal, Transfer, iDEAL, etc. |
| `date` | date | Parsed from YYYYMMDD integer |
| `description` | str | Merchant or counterparty name |
| `amount` | float | Positive = credit (money in), Negative = debit (money out) |
| `currency` | str | Always `EUR` for ING NL |
| `category` | str | Rule-based: Groceries, Transport, Dining, Subscriptions, … |
| `account` | str | Account IBAN |
| `status` | str | Always `verified` |

The raw ING columns are preserved in `raw_transactions` for reference.

---

## Data Privacy

- `data/`, `*.csv`, `*.xlsx`, `*.xls`, `*.db`, `*.sqlite` are all gitignored.
- Your bank data never leaves your machine.
- Docker volumes are local to your machine.
- The app has no telemetry, no external API calls, and no cloud connectivity.

---

## Troubleshooting

### "Module not found" on import

Make sure your virtual environment is activated and dependencies are installed:

```bash
source .venv/bin/activate   # Mac/Linux
pip install -r requirements.txt
```

### Port 8501 already in use

```bash
streamlit run app.py --server.port 8502
```

Or kill the process occupying the port:

```bash
# Mac/Linux
lsof -ti:8501 | xargs kill
# Windows
netstat -ano | findstr :8501   # find the PID
taskkill /PID <pid> /F
```

### "Unsupported file type" on upload

The app accepts `.csv`, `.xlsx`, and `.xls` files only. Make sure you exported from ING
Bank as CSV (not PDF).

### Amounts look wrong (e.g. 2,450 instead of 24.50)

ING Bank uses European number formatting: dot as thousands separator, comma as decimal.
The pipeline handles this automatically. If you are loading a non-ING CSV with standard
formatting you may need to pre-process the amount column.

### Data doubles on every upload

Use **Replace all data** (the default) in the upload UI. **Append to existing** is for
loading multiple different CSV files; uploading the same file twice in append mode will
duplicate the rows.

### Docker container exits immediately

Check the logs:

```bash
docker compose logs app
```

Common causes: port conflict, missing `.env`, or a Python import error in the source.

### Windows — pytest permission error on temp files

The test suite uses a `.test_fixtures/` directory in the project root instead of the
system temp folder, which avoids Windows permission issues. If you see permission errors,
make sure you are running pytest from the project root and that `.test_fixtures/` is
writable.
