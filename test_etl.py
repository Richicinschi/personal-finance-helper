"""
Tests for pipeline.py — Extract, Transform, Load pipeline.

Run with:
    pytest test_etl.py -v
"""

import textwrap
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from pipeline import (
    STANDARD_COLUMNS,
    _classify,
    _parse_amount,
    _sign_amount,
    _snake,
    extract,
    load,
    make_engine,
    run_pipeline,
    transform,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ING_CSV_CONTENT = textwrap.dedent("""\
    Date,Name / Description,Account,Counterparty,Code,Debit/credit,Amount (EUR),Transaction type,Notifications
    20240101,Albert Heijn 1521,NL53INGB0001,,BA,Debit,"24,50",Payment terminal,some note
    20240102,Salary Payment,NL53INGB0001,NL99TEST0001,OV,Credit,"1500,00",Transfer,salary note
    20240103,Netflix,NL53INGB0001,,IC,Debit,"12,99",Online Banking,sub note
    20240104,NS Utrecht,NL53INGB0001,,BA,Debit,"10,00",Payment terminal,train note
    20240105,Revolut Transfer,NL53INGB0001,NL25REVO0001,GT,Debit,"50,00",Online Banking,revolut note
""")

# Use a local directory instead of tmp_path (avoids Windows temp permissions)
_FIXTURE_DIR = Path(__file__).parent / ".test_fixtures"
_FIXTURE_DIR.mkdir(exist_ok=True)


@pytest.fixture
def sample_csv() -> Path:
    """Write a small ING-format CSV to a local fixture file."""
    f = _FIXTURE_DIR / "bank.csv"
    f.write_text(ING_CSV_CONTENT, encoding="utf-8")
    yield f
    f.unlink(missing_ok=True)


@pytest.fixture
def raw_df(sample_csv: Path) -> pd.DataFrame:
    return extract(sample_csv)


@pytest.fixture
def processed_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    return transform(raw_df)


@pytest.fixture
def sqlite_engine():
    """In-memory SQLite engine — no PostgreSQL needed for tests."""
    engine = create_engine("sqlite:///:memory:", future=True)
    yield engine
    engine.dispose()


# ---------------------------------------------------------------------------
# _snake
# ---------------------------------------------------------------------------

class TestSnake:
    def test_spaces_become_underscores(self):
        assert _snake("Name / Description") == "name_description"

    def test_parens_stripped(self):
        assert _snake("Amount (EUR)") == "amount_eur"

    def test_already_snake(self):
        assert _snake("account") == "account"

    def test_mixed_case(self):
        assert _snake("TransactionType") == "transactiontype"

    def test_multiple_separators(self):
        assert _snake("Debit/credit") == "debit_credit"


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

class TestExtract:
    def test_returns_dataframe(self, sample_csv):
        df = extract(sample_csv)
        assert isinstance(df, pd.DataFrame)

    def test_row_count(self, sample_csv):
        df = extract(sample_csv)
        assert len(df) == 5

    def test_columns_are_snake_case(self, sample_csv):
        df = extract(sample_csv)
        for col in df.columns:
            assert col == col.lower(), f"Column not lowercase: {col}"
            assert " " not in col, f"Column has space: {col}"

    def test_source_file_column_added(self, sample_csv):
        df = extract(sample_csv)
        assert "source_file" in df.columns
        assert df["source_file"].iloc[0] == sample_csv.name

    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            extract(_FIXTURE_DIR / "definitely_missing_abc123.csv")

    def test_unsupported_extension_raises(self):
        f = _FIXTURE_DIR / "data.json"
        f.write_text("{}")
        try:
            with pytest.raises(ValueError, match="Unsupported file type"):
                extract(f)
        finally:
            f.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# _parse_amount
# ---------------------------------------------------------------------------

class TestParseAmount:
    def test_simple(self):
        s = pd.Series(["10,00", "28,80", "1026,39"])
        result = _parse_amount(s)
        assert list(result) == pytest.approx([10.0, 28.80, 1026.39])

    def test_thousands_separator(self):
        s = pd.Series(["1.500,00"])
        assert _parse_amount(s).iloc[0] == pytest.approx(1500.0)

    def test_integer_string(self):
        s = pd.Series(["100"])
        assert _parse_amount(s).iloc[0] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

class TestTransform:
    def test_output_columns(self, processed_df):
        assert list(processed_df.columns) == STANDARD_COLUMNS

    def test_row_count(self, processed_df):
        assert len(processed_df) == 5

    def test_date_is_date_type(self, processed_df):
        import datetime
        assert all(isinstance(d, datetime.date) for d in processed_df["date"])

    def test_debit_is_negative(self, processed_df):
        # Row 0: Albert Heijn, Debit → negative
        assert processed_df.loc[0, "amount"] < 0

    def test_credit_is_positive(self, processed_df):
        # Row 1: Salary, Credit → positive
        assert processed_df.loc[1, "amount"] > 0

    def test_currency_constant(self, processed_df):
        assert (processed_df["currency"] == "EUR").all()

    def test_status_constant(self, processed_df):
        assert (processed_df["status"] == "verified").all()

    def test_salary_amount(self, processed_df):
        assert processed_df.loc[1, "amount"] == pytest.approx(1500.0)

    def test_albert_heijn_amount(self, processed_df):
        assert processed_df.loc[0, "amount"] == pytest.approx(-24.50)


# ---------------------------------------------------------------------------
# _classify
# ---------------------------------------------------------------------------

class TestClassify:
    def test_groceries(self):
        assert _classify("Albert Heijn 1521") == "Groceries"

    def test_transport(self):
        assert _classify("BCK*NS UTRECHT C.") == "Transport"

    def test_subscriptions(self):
        assert _classify("Netflix monthly") == "Subscriptions"

    def test_revolut(self):
        assert _classify("Revolut**0998*") == "Revolut Transfer"

    def test_other(self):
        assert _classify("Random Unknown Merchant") == "Other"

    def test_case_insensitive(self):
        assert _classify("ALBERT HEIJN") == "Groceries"


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

class TestLoad:
    def test_load_returns_counts(self, raw_df, processed_df, sqlite_engine):
        counts = load(raw_df, processed_df, sqlite_engine)
        assert counts["raw_rows"] == 5
        assert counts["processed_rows"] == 5

    def test_tables_created(self, raw_df, processed_df, sqlite_engine):
        load(raw_df, processed_df, sqlite_engine)
        with sqlite_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM transactions")).fetchone()
            assert result[0] == 5

    def test_raw_table_populated(self, raw_df, processed_df, sqlite_engine):
        load(raw_df, processed_df, sqlite_engine)
        with sqlite_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM raw_transactions")).fetchone()
            assert result[0] == 5

    def test_append_mode(self, raw_df, processed_df, sqlite_engine):
        load(raw_df, processed_df, sqlite_engine, mode="append")
        load(raw_df, processed_df, sqlite_engine, mode="append")
        with sqlite_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM transactions")).fetchone()
            assert result[0] == 10

    def test_replace_mode_does_not_duplicate(self, raw_df, processed_df, sqlite_engine):
        load(raw_df, processed_df, sqlite_engine, mode="replace")
        load(raw_df, processed_df, sqlite_engine, mode="replace")
        with sqlite_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM transactions")).fetchone()
            assert result[0] == 5  # second replace wiped the first load

    def test_invalid_mode_raises(self, raw_df, processed_df, sqlite_engine):
        with pytest.raises(ValueError, match="mode must be"):
            load(raw_df, processed_df, sqlite_engine, mode="upsert")


# ---------------------------------------------------------------------------
# run_pipeline (integration)
# ---------------------------------------------------------------------------

class TestRunPipeline:
    def test_full_pipeline(self, sample_csv, sqlite_engine):
        counts = run_pipeline(sample_csv, sqlite_engine, verbose=False)
        assert counts["raw_rows"] == 5
        assert counts["processed_rows"] == 5

    def test_pipeline_file_not_found(self, sqlite_engine):
        with pytest.raises(FileNotFoundError):
            run_pipeline(_FIXTURE_DIR / "definitely_missing.csv", sqlite_engine, verbose=False)
