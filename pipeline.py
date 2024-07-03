"""
ETL Pipeline — personal-finance-helper
=======================================
Single-module Extract → Transform → Load pipeline for ING Bank CSV exports.

Usage (CLI):
    python etl.py --file data/bank.csv --db sqlite:///finance.db

Usage (programmatic):
    from etl import run_pipeline
    result = run_pipeline("data/bank.csv", engine)
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import (
    Column,
    Date,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

# Standard output schema column order
STANDARD_COLUMNS = [
    "transaction_type",
    "date",
    "description",
    "amount",
    "currency",
    "category",
    "account",
    "status",
]

# Rule-based category mapping: substring in description (lowercased) -> category
CATEGORY_RULES: list[tuple[str, str]] = [
    # Groceries
    ("albert heijn", "Groceries"),
    ("jumbo", "Groceries"),
    ("lidl", "Groceries"),
    ("aldi", "Groceries"),
    ("dirk", "Groceries"),
    ("ah to go", "Groceries"),
    # Transport
    ("ns ", "Transport"),
    ("bck*ns", "Transport"),
    ("ns.nl", "Transport"),
    ("gvb", "Transport"),
    ("ret ", "Transport"),
    ("ov-chipkaart", "Transport"),
    ("uber", "Transport"),
    ("bolt.eu", "Transport"),
    ("parking", "Transport"),
    ("q-park", "Transport"),
    # Dining & Cafes
    ("mcd", "Dining"),
    ("mcdonalds", "Dining"),
    ("burger king", "Dining"),
    ("kfc", "Dining"),
    ("pizza", "Dining"),
    ("starbucks", "Dining"),
    ("cafe", "Dining"),
    ("restaurant", "Dining"),
    ("dominos", "Dining"),
    ("thuisbezorgd", "Dining"),
    ("uber eats", "Dining"),
    ("deliveroo", "Dining"),
    # Online / Subscriptions
    ("netflix", "Subscriptions"),
    ("spotify", "Subscriptions"),
    ("amazon", "Online Shopping"),
    ("bol.com", "Online Shopping"),
    ("zalando", "Online Shopping"),
    ("coolblue", "Online Shopping"),
    # Health
    ("apotheek", "Health"),
    ("pharmacy", "Health"),
    ("huisarts", "Health"),
    ("tandarts", "Health"),
    ("gym", "Health"),
    ("basic-fit", "Health"),
    # Revolut (transfers to/from Revolut)
    ("revolut", "Revolut Transfer"),
    # ATM / Cash
    ("cash machine", "Cash"),
    ("geldautomaat", "Cash"),
    ("atm", "Cash"),
    # Utilities / Bills
    ("vattenfall", "Utilities"),
    ("eneco", "Utilities"),
    ("ziggo", "Utilities"),
    ("kpn", "Utilities"),
    ("t-mobile", "Utilities"),
    ("vodafone", "Utilities"),
    # Income / Salary
    ("salaris", "Income"),
    ("salary", "Income"),
    ("loon", "Income"),
    ("werkgever", "Income"),
]


# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------

def make_engine(db_url: str) -> Engine:
    """Create a SQLAlchemy engine from a connection URL.

    Examples:
        make_engine("sqlite:///finance.db")
        make_engine("postgresql://user:pass@localhost:5432/finance")
    """
    return create_engine(db_url, future=True)


# ---------------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------------

def _snake(name: str) -> str:
    """Convert a column name to snake_case."""
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name


def extract(path: str | Path) -> pd.DataFrame:
    """Load a CSV or Excel file into a raw DataFrame.

    Returns the raw data with normalised snake_case column names and a
    'source_file' column recording provenance. No type coercion is applied.

    Args:
        path: Path to the CSV (.csv) or Excel (.xlsx / .xls) file.

    Returns:
        Raw DataFrame with snake_case columns.

    Raises:
        ValueError: If the file extension is not supported.
        FileNotFoundError: If the path does not exist.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    ext = path.suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file type '{ext}'. Supported: {SUPPORTED_EXTENSIONS}"
        )

    if ext == ".csv":
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding="latin-1")
    else:
        df = pd.read_excel(path, engine="openpyxl")

    # Normalise column names
    df.columns = [_snake(c) for c in df.columns]
    df["source_file"] = path.name

    return df


# ---------------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------------

def _parse_amount(series: pd.Series) -> pd.Series:
    """Convert ING-style comma-decimal strings '1.026,39' to float 1026.39."""
    return (
        series.astype(str)
        .str.replace(".", "", regex=False)   # thousands separator
        .str.replace(",", ".", regex=False)  # decimal separator
        .astype(float)
    )


def _sign_amount(df: pd.DataFrame, amount_col: str, dc_col: str) -> pd.Series:
    """Return signed amount: Credit = positive, Debit = negative."""
    amounts = _parse_amount(df[amount_col])
    return amounts.where(df[dc_col].str.strip().str.lower() == "credit", -amounts)


def _parse_date(series: pd.Series) -> pd.Series:
    """Convert YYYYMMDD integer or string to datetime.date."""
    return pd.to_datetime(series.astype(str), format="%Y%m%d").dt.date


def _classify(description: str) -> str:
    """Apply keyword rules to assign a spending category."""
    lower = description.lower()
    for keyword, category in CATEGORY_RULES:
        if keyword in lower:
            return category
    return "Other"


def transform(raw: pd.DataFrame) -> pd.DataFrame:
    """Standardise a raw extract DataFrame to the canonical schema.

    Performs:
    - Date parsing (YYYYMMDD int -> datetime.date)
    - Amount parsing (comma-decimal string -> signed float)
    - Column renaming to standard schema
    - Adds derived fields: currency, category, status
    - Drops rows missing critical fields

    Args:
        raw: DataFrame produced by extract().

    Returns:
        Processed DataFrame with columns: STANDARD_COLUMNS.

    Raises:
        KeyError: If expected source columns are missing.
    """
    df = raw.copy()

    # Detect column aliases (ING export uses specific names)
    col_map = _detect_column_map(df.columns.tolist())

    out = pd.DataFrame()
    out["transaction_type"] = df[col_map["transaction_type"]].str.strip()
    out["date"] = _parse_date(df[col_map["date"]])
    out["description"] = df[col_map["description"]].str.strip()
    out["amount"] = _sign_amount(df, col_map["amount_raw"], col_map["debit_credit"])
    out["currency"] = "EUR"
    out["category"] = out["description"].apply(_classify)
    out["account"] = df[col_map["account"]].str.strip()
    out["status"] = "verified"

    # Drop rows with null critical fields
    critical = ["date", "description", "amount", "account"]
    before = len(out)
    out = out.dropna(subset=critical).reset_index(drop=True)
    dropped = before - len(out)
    if dropped > 0:
        print(f"  [transform] Dropped {dropped} rows with null critical fields.")

    return out[STANDARD_COLUMNS]


def _detect_column_map(columns: list[str]) -> dict[str, str]:
    """Map logical field names to actual snake_case column names.

    Supports ING Bank export format and generic fallbacks.
    """
    col_set = set(columns)

    def find(candidates: list[str]) -> str:
        for c in candidates:
            if c in col_set:
                return c
        raise KeyError(
            f"Could not find any of {candidates} in columns: {columns}. "
            "Ensure the file is a valid ING Bank CSV export."
        )

    return {
        "date":             find(["date"]),
        "description":      find(["name_description", "description", "name"]),
        "account":          find(["account"]),
        "amount_raw":       find(["amount_eur", "amount", "bedrag_eur"]),
        "debit_credit":     find(["debit_credit", "debit_credit_indicator", "af_bij"]),
        "transaction_type": find(["transaction_type", "mutatiesoort"]),
    }


# ---------------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------------

def _ensure_tables(engine: Engine) -> None:
    """Create raw_transactions and transactions tables if they don't exist."""
    meta = MetaData()

    Table(
        "raw_transactions",
        meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("date", String(8)),
        Column("name_description", Text),
        Column("account", String(50)),
        Column("counterparty", String(50)),
        Column("code", String(10)),
        Column("debit_credit", String(10)),
        Column("amount_eur", String(20)),
        Column("transaction_type", String(50)),
        Column("notifications", Text),
        Column("source_file", String(255)),
    )

    Table(
        "transactions",
        meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("transaction_type", String(50)),
        Column("date", Date),
        Column("description", Text),
        Column("amount", Float),
        Column("currency", String(10)),
        Column("category", String(50)),
        Column("account", String(50)),
        Column("status", String(20)),
    )

    meta.create_all(engine)


def load(
    raw: pd.DataFrame,
    processed: pd.DataFrame,
    engine: Engine,
    mode: str = "replace",
) -> dict[str, int]:
    """Insert raw and processed DataFrames into the database.

    Args:
        raw: Raw DataFrame from extract().
        processed: Standardised DataFrame from transform().
        engine: SQLAlchemy engine.
        mode: ``"replace"`` (default) drops and recreates both tables before
            inserting — safe for single-file uploads and re-uploads.
            ``"append"`` adds rows to existing tables — use when loading
            multiple CSV files in sequence via the CLI.

    Returns:
        Dict with 'raw_rows' and 'processed_rows' counts inserted.
    """
    if mode not in ("replace", "append"):
        raise ValueError(f"mode must be 'replace' or 'append', got '{mode}'")

    if_exists = "replace" if mode == "replace" else "append"

    # _ensure_tables is only needed in append mode; replace recreates the tables.
    if mode == "append":
        _ensure_tables(engine)

    raw.to_sql("raw_transactions", engine, if_exists=if_exists, index=False)
    processed.to_sql("transactions", engine, if_exists=if_exists, index=False)

    return {
        "raw_rows": len(raw),
        "processed_rows": len(processed),
    }


# ---------------------------------------------------------------------------
# PIPELINE (orchestrator)
# ---------------------------------------------------------------------------

def run_pipeline(
    file_path: str | Path,
    engine: Engine,
    verbose: bool = True,
    mode: str = "replace",
) -> dict[str, int]:
    """Run the full ETL pipeline: extract -> transform -> load.

    Args:
        file_path: Path to the CSV or Excel data file.
        engine: SQLAlchemy engine connected to the target database.
        verbose: Print progress messages.
        mode: ``"replace"`` (default) clears existing data before loading.
            ``"append"`` adds to existing data — for multi-file batch loads.

    Returns:
        Dict with 'raw_rows' and 'processed_rows' counts.
    """
    path = Path(file_path)
    if verbose:
        print(f"[ETL] Starting pipeline for: {path.name}")

    if verbose:
        print("[ETL] Extracting...")
    raw = extract(path)
    if verbose:
        print(f"  Extracted {len(raw):,} rows, {len(raw.columns)} columns")

    if verbose:
        print("[ETL] Transforming...")
    processed = transform(raw)
    if verbose:
        print(f"  Processed {len(processed):,} rows")

    if verbose:
        print("[ETL] Loading into database...")
    counts = load(raw, processed, engine, mode=mode)
    if verbose:
        print(f"  Loaded: {counts['raw_rows']:,} raw, {counts['processed_rows']:,} processed")
        print("[ETL] Pipeline complete.")

    return counts


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the ETL pipeline for a bank CSV/Excel export."
    )
    parser.add_argument("--file", required=True, help="Path to the data file")
    parser.add_argument(
        "--db",
        default="sqlite:///finance.db",
        help="SQLAlchemy database URL (default: sqlite:///finance.db)",
    )
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="'replace' clears existing data (default); 'append' adds to it",
    )
    args = parser.parse_args()

    engine = make_engine(args.db)
    try:
        counts = run_pipeline(args.file, engine, verbose=True, mode=args.mode)
        print(f"\nDone. Raw rows: {counts['raw_rows']:,} | Processed: {counts['processed_rows']:,}")
    except (FileNotFoundError, ValueError, KeyError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
