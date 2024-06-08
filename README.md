# Personal Finance Helper

A personal finance analytics dashboard that transforms raw transaction data from any CSV-exporting expense tracking app into actionable financial insights.

## Architecture

```
personal-finance-helper/
├── etl/                  # Extract, Transform, Load pipeline
│   ├── extract.py        # CSV/Excel loader with schema detection
│   ├── transform.py      # Filter & standardize transactions
│   ├── load.py           # PostgreSQL loader
│   ├── pipeline.py       # Orchestrates ETL steps
│   └── db.py             # SQLAlchemy engine factory
├── queries/              # Named SQL query files
│   ├── executor.py       # Parses named queries, returns DataFrames
│   ├── running_balance.sql
│   ├── daily_spend.sql
│   ├── weekly_spend.sql
│   ├── monthly_spend.sql
│   └── account_pivot.sql
├── app/                  # Streamlit frontend
│   ├── main.py           # Entry point, tab layout
│   ├── state.py          # Session state management
│   ├── pages/
│   │   ├── landing.py
│   │   ├── data_management.py
│   │   └── analytics.py
│   └── components/
│       └── sidebar.py
├── notebooks/            # Exploratory analysis
├── tests/                # Pytest test suite
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

## Standard Transaction Schema

After transformation, all transactions conform to this schema:

| Column           | Type     | Description                        |
|------------------|----------|------------------------------------|
| transaction_type | str      | e.g. debit, credit, transfer       |
| date             | date     | Transaction date                   |
| description      | str      | Merchant / narrative               |
| amount           | float    | Positive = inflow, Negative = outflow |
| currency         | str      | ISO 4217 code (e.g. USD, EUR)      |
| category         | str      | Spending category                  |
| account          | str      | Account / payment method name      |
| status           | str      | verified, pending, etc.            |

## Quick Start

### With Docker (recommended)

```bash
cp .env.example .env
# edit .env with your DB credentials
docker compose up
```

App will be available at http://localhost:8501

### Local Development

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# edit .env
streamlit run app/main.py
```

### Run Tests

```bash
pytest tests/
```

## Data Privacy

**Never commit data files.** The `data/` directory and all `*.xlsx`, `*.xls`, `*.csv` files are gitignored. Your bank data stays local.
