"""
Data Exploration Script — personal-finance-helper
Mirrors notebooks/01_data_exploration.ipynb as a runnable Python script.

Usage:
    python explore_data.py
    python explore_data.py --data data/bank.csv

Outputs a full schema report and canonical field mapping to stdout.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_raw(path: Path) -> pd.DataFrame:
    """Load CSV (auto-detects comma vs semicolon delimiter)."""
    try:
        df = pd.read_csv(path, sep=",", encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=",", encoding="latin-1")
    return df


def parse_amount(series: pd.Series) -> pd.Series:
    """Convert ING-style amount strings like '1.026,39' → float 1026.39."""
    return (
        series.astype(str)
        .str.replace(".", "", regex=False)   # thousands sep
        .str.replace(",", ".", regex=False)  # decimal sep
        .astype(float)
    )


def parse_date(series: pd.Series) -> pd.Series:
    """Convert YYYYMMDD integer or string to datetime."""
    return pd.to_datetime(series.astype(str), format="%Y%m%d")


def sign_amount(df: pd.DataFrame, amount_col: str, dc_col: str) -> pd.Series:
    """Return signed float: Credit → positive, Debit → negative."""
    amounts = parse_amount(df[amount_col])
    return amounts.where(df[dc_col].str.strip() == "Credit", -amounts)


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------

def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def report_shape(df: pd.DataFrame) -> None:
    section("1. Shape & Memory")
    print(f"  Rows   : {df.shape[0]:,}")
    print(f"  Columns: {df.shape[1]}")
    mem_mb = df.memory_usage(deep=True).sum() / 1024**2
    print(f"  Memory : {mem_mb:.2f} MB")


def report_dtypes(df: pd.DataFrame) -> None:
    section("2. Columns & Dtypes")
    for col in df.columns:
        print(f"  {col!r:<30} {str(df[col].dtype):<12}")


def report_nulls(df: pd.DataFrame) -> None:
    section("3. Null Counts")
    nulls = df.isnull().sum()
    for col, n in nulls.items():
        pct = n / len(df) * 100
        flag = " (!)" if pct > 50 else ""
        print(f"  {col!r:<30} {n:>6,}  ({pct:5.1f}%){flag}")


def report_uniques(df: pd.DataFrame, max_vals: int = 8) -> None:
    section("4. Unique Values per Column")
    for col in df.columns:
        uv = df[col].dropna().unique()
        preview = list(uv[:max_vals])
        more = f"  ... +{len(uv) - max_vals} more" if len(uv) > max_vals else ""
        print(f"\n  [{col}]  ({len(uv)} unique){more}")
        for v in preview:
            print(f"    {v!r}")


def report_date_range(df: pd.DataFrame, date_col: str) -> None:
    section("5. Date Range")
    try:
        dates = parse_date(df[date_col])
        print(f"  Earliest : {dates.min().date()}")
        print(f"  Latest   : {dates.max().date()}")
        print(f"  Span     : {(dates.max() - dates.min()).days} days")
    except Exception as e:
        print(f"  Could not parse dates: {e}")


def report_amount_stats(df: pd.DataFrame, amount_col: str, dc_col: str) -> None:
    section("6. Amount Statistics (EUR)")
    try:
        signed = sign_amount(df, amount_col, dc_col)
        print(f"  Min    : EUR {signed.min():>12,.2f}")
        print(f"  Max    : EUR {signed.max():>12,.2f}")
        print(f"  Mean   : EUR {signed.mean():>12,.2f}")
        print(f"  Median : EUR {signed.median():>12,.2f}")
        print(f"  Sum    : EUR {signed.sum():>12,.2f}")
        debits = signed[signed < 0]
        credits = signed[signed > 0]
        print(f"\n  Debits  count={len(debits):,}  total=EUR {debits.sum():,.2f}")
        print(f"  Credits count={len(credits):,}  total=EUR {credits.sum():,.2f}")
    except Exception as e:
        print(f"  Could not parse amounts: {e}")


def report_transaction_types(df: pd.DataFrame, type_col: str) -> None:
    section("7. Transaction Type Distribution")
    counts = df[type_col].value_counts()
    for t, n in counts.items():
        pct = n / len(df) * 100
        print(f"  {t!r:<30} {n:>6,}  ({pct:5.1f}%)")


def report_schema_mapping() -> None:
    section("8. Canonical Schema Mapping (source -> standard)")
    mapping = [
        ("Date",               "date",             "parse_date(YYYYMMDD int)"),
        ("Name / Description", "description",      "direct copy"),
        ("Account",            "account",          "direct copy (IBAN)"),
        ("Debit/credit",       "-",                "used to sign amount"),
        ("Amount (EUR)",       "amount",           "comma-decimal -> signed float"),
        ("Transaction type",   "transaction_type", "direct copy"),
        ("Code",               "-",                "internal ING code, kept in raw"),
        ("Counterparty",       "-",                "kept in raw, high nulls ok"),
        ("Notifications",      "-",                "kept in raw for audit trail"),
        ("(derived)",          "currency",         "constant 'EUR'"),
        ("(derived)",          "category",         "rule-based or ML classification"),
        ("(derived)",          "status",           "constant 'verified'"),
    ]
    print(f"\n  {'Source Column':<30} {'Standard Field':<20} Note")
    print(f"  {'-'*30} {'-'*20} {'-'*30}")
    for src, dst, note in mapping:
        print(f"  {src:<30} {dst:<20} {note}")


def report_sample(df: pd.DataFrame, n: int = 3) -> None:
    section(f"9. Sample Rows (n={n})")
    for i, row in df.head(n).iterrows():
        print(f"\n  Row {i}:")
        for col, val in row.items():
            print(f"    {col!r:<30} {val!r}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Explore transaction CSV schema")
    parser.add_argument(
        "--data",
        default="data/bank.csv",
        help="Path to the transaction CSV (default: data/bank.csv)",
    )
    args = parser.parse_args()

    data_path = Path(args.data)
    if not data_path.exists():
        print(f"ERROR: data file not found at {data_path}", file=sys.stderr)
        print("Put bank.csv in the data/ folder (gitignored).", file=sys.stderr)
        sys.exit(1)

    print(f"Loading: {data_path}")
    df = load_raw(data_path)

    report_shape(df)
    report_dtypes(df)
    report_nulls(df)
    report_date_range(df, "Date")
    report_amount_stats(df, "Amount (EUR)", "Debit/credit")
    report_transaction_types(df, "Transaction type")
    report_uniques(df)
    report_schema_mapping()
    report_sample(df)

    print("\n" + "=" * 60)
    print("  Exploration complete. See schema mapping above.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
