"""
Tests for queries.py — SQL query layer.

Run with:
    pytest test_queries.py -v
"""

import datetime
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from query_layer import PERIOD_FORMATS, QUERY_REGISTRY, QueryExecutor


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_TRANSACTIONS = [
    # (transaction_type, date, description, amount, currency, category, account, status)
    ("Payment terminal", "2024-01-05", "Albert Heijn 1521",  -25.50, "EUR", "Groceries",     "NL53INGB0001", "verified"),
    ("Payment terminal", "2024-01-10", "NS Utrecht",          -10.00, "EUR", "Transport",     "NL53INGB0001", "verified"),
    ("Transfer",         "2024-01-31", "Salary January",    1500.00,  "EUR", "Income",        "NL53INGB0001", "verified"),
    ("Payment terminal", "2024-02-03", "Albert Heijn 1521",  -30.00, "EUR", "Groceries",     "NL53INGB0001", "verified"),
    ("Online Banking",   "2024-02-10", "Netflix",             -12.99, "EUR", "Subscriptions", "NL53INGB0001", "verified"),
    ("Payment terminal", "2024-02-15", "Basic-Fit",           -24.99, "EUR", "Health",        "NL53INGB0001", "verified"),
    ("Transfer",         "2024-02-29", "Salary February",   1500.00,  "EUR", "Income",        "NL53INGB0001", "verified"),
    ("Payment terminal", "2024-03-01", "Jumbo Supermarkt",   -42.10, "EUR", "Groceries",     "NL53INGB0001", "verified"),
    ("Payment terminal", "2024-03-05", "McD Utrecht",         -8.50, "EUR", "Dining",        "NL53INGB0001", "verified"),
    ("Payment terminal", "2024-03-20", "Revolut Transfer",  -200.00,  "EUR", "Revolut Transfer", "NL53INGB0001", "verified"),
]


@pytest.fixture(scope="module")
def engine():
    """In-memory SQLite engine pre-populated with sample transactions."""
    eng = create_engine("sqlite:///:memory:", future=True)

    cols = ["transaction_type", "date", "description", "amount",
            "currency", "category", "account", "status"]
    df = pd.DataFrame(SAMPLE_TRANSACTIONS, columns=cols)
    df.to_sql("transactions", eng, if_exists="replace", index=True, index_label="id")

    yield eng
    eng.dispose()


@pytest.fixture(scope="module")
def executor(engine):
    return QueryExecutor(engine)


# ---------------------------------------------------------------------------
# QUERY_REGISTRY
# ---------------------------------------------------------------------------

class TestQueryRegistry:
    def test_all_expected_queries_present(self):
        expected = {"running_balance", "spend_by_period", "category_breakdown",
                    "account_summary", "top_merchants"}
        assert expected.issubset(set(QUERY_REGISTRY.keys()))

    def test_all_queries_have_name_header(self):
        for name, sql in QUERY_REGISTRY.items():
            assert f"-- name: {name}" in sql, (
                f"Query '{name}' missing '-- name: {name}' header"
            )

    def test_all_queries_are_non_empty_strings(self):
        for name, sql in QUERY_REGISTRY.items():
            assert isinstance(sql, str) and len(sql.strip()) > 50, (
                f"Query '{name}' looks empty or too short"
            )


# ---------------------------------------------------------------------------
# QueryExecutor.list_queries
# ---------------------------------------------------------------------------

class TestListQueries:
    def test_returns_sorted_list(self, executor):
        names = executor.list_queries()
        assert names == sorted(names)

    def test_contains_all_registered(self, executor):
        assert set(executor.list_queries()) == set(QUERY_REGISTRY.keys())


# ---------------------------------------------------------------------------
# QueryExecutor.execute — unknown query
# ---------------------------------------------------------------------------

class TestExecuteUnknown:
    def test_raises_key_error(self, executor):
        with pytest.raises(KeyError, match="Unknown query"):
            executor.execute("nonexistent_query_xyz")


# ---------------------------------------------------------------------------
# running_balance
# ---------------------------------------------------------------------------

class TestRunningBalance:
    def test_returns_dataframe(self, executor):
        df = executor.execute("running_balance")
        assert isinstance(df, pd.DataFrame)

    def test_has_running_balance_column(self, executor):
        df = executor.execute("running_balance")
        assert "running_balance" in df.columns

    def test_row_count(self, executor):
        df = executor.execute("running_balance")
        assert len(df) == len(SAMPLE_TRANSACTIONS)

    def test_running_balance_is_cumulative(self, executor):
        df = executor.execute("running_balance")
        # The running balance should end at the sum of all amounts
        total = sum(r[3] for r in SAMPLE_TRANSACTIONS)
        assert df["running_balance"].iloc[-1] == pytest.approx(total, rel=1e-4)

    def test_date_filter(self, executor):
        df = executor.execute("running_balance", {"date_from": "2024-02-01", "date_to": "2024-02-29"})
        assert len(df) == 4  # rows in Feb 2024
        assert all(str(d) >= "2024-02-01" for d in df["date"])

    def test_expected_columns(self, executor):
        df = executor.execute("running_balance")
        for col in ["date", "account", "description", "category", "amount", "running_balance"]:
            assert col in df.columns


# ---------------------------------------------------------------------------
# spend_by_period (via convenience wrapper)
# ---------------------------------------------------------------------------

class TestSpendByPeriod:
    def test_monthly_returns_dataframe(self, executor):
        df = executor.spend_by_period("monthly")
        assert isinstance(df, pd.DataFrame)

    def test_monthly_has_expected_columns(self, executor):
        df = executor.spend_by_period("monthly")
        for col in ["period", "category", "transaction_count", "total_spent"]:
            assert col in df.columns

    def test_monthly_period_format(self, executor):
        df = executor.spend_by_period("monthly")
        # Periods should look like "2024-01", "2024-02", "2024-03"
        assert all(len(p) == 7 for p in df["period"]), (
            f"Unexpected period values: {df['period'].tolist()}"
        )

    def test_daily_returns_results(self, executor):
        df = executor.spend_by_period("daily")
        assert len(df) > 0

    def test_weekly_returns_results(self, executor):
        df = executor.spend_by_period("weekly")
        assert len(df) > 0

    def test_only_debits_included(self, executor):
        df = executor.spend_by_period("monthly")
        # Credits (Salary) must not appear in spend totals
        assert (df["total_spent"] > 0).all()
        # Income category should not appear (it's all credits)
        assert "Income" not in df["category"].values

    def test_invalid_granularity_raises(self, executor):
        with pytest.raises(ValueError, match="granularity must be one of"):
            executor.spend_by_period("quarterly")

    def test_known_monthly_total(self, executor):
        df = executor.spend_by_period("monthly")
        jan = df[df["period"] == "2024-01"]
        jan_total = jan["total_spent"].sum()
        # Jan debits: 25.50 + 10.00 = 35.50
        assert jan_total == pytest.approx(35.50, rel=1e-4)


# ---------------------------------------------------------------------------
# category_breakdown
# ---------------------------------------------------------------------------

class TestCategoryBreakdown:
    def test_returns_dataframe(self, executor):
        df = executor.execute("category_breakdown")
        assert isinstance(df, pd.DataFrame)

    def test_has_expected_columns(self, executor):
        df = executor.execute("category_breakdown")
        for col in ["category", "transaction_count", "total_spent", "total_received", "avg_spent_per_tx"]:
            assert col in df.columns

    def test_groceries_category_present(self, executor):
        df = executor.execute("category_breakdown")
        assert "Groceries" in df["category"].values

    def test_groceries_total(self, executor):
        df = executor.execute("category_breakdown")
        groceries = df[df["category"] == "Groceries"]["total_spent"].iloc[0]
        # Albert Heijn + Jumbo = 25.50 + 30.00 + 42.10 = 97.60
        assert groceries == pytest.approx(97.60, rel=1e-4)

    def test_income_in_total_received(self, executor):
        df = executor.execute("category_breakdown")
        income = df[df["category"] == "Income"]["total_received"].iloc[0]
        # 1500 + 1500 = 3000
        assert income == pytest.approx(3000.00, rel=1e-4)

    def test_ordered_by_total_spent_desc(self, executor):
        df = executor.execute("category_breakdown")
        spent = df["total_spent"].tolist()
        assert spent == sorted(spent, reverse=True)


# ---------------------------------------------------------------------------
# account_summary
# ---------------------------------------------------------------------------

class TestAccountSummary:
    def test_returns_dataframe(self, executor):
        df = executor.execute("account_summary")
        assert isinstance(df, pd.DataFrame)

    def test_one_row_per_account(self, executor):
        df = executor.execute("account_summary")
        assert len(df) == 1  # Only one account in sample data

    def test_has_expected_columns(self, executor):
        df = executor.execute("account_summary")
        for col in ["account", "transaction_count", "total_credits", "total_debits",
                    "net_balance", "first_transaction", "last_transaction"]:
            assert col in df.columns

    def test_transaction_count(self, executor):
        df = executor.execute("account_summary")
        assert df["transaction_count"].iloc[0] == len(SAMPLE_TRANSACTIONS)

    def test_net_balance(self, executor):
        df = executor.execute("account_summary")
        expected_net = sum(r[3] for r in SAMPLE_TRANSACTIONS)
        assert df["net_balance"].iloc[0] == pytest.approx(expected_net, rel=1e-4)

    def test_date_range(self, executor):
        df = executor.execute("account_summary")
        assert df["first_transaction"].iloc[0] == "2024-01-05"
        assert df["last_transaction"].iloc[0] == "2024-03-20"


# ---------------------------------------------------------------------------
# top_merchants
# ---------------------------------------------------------------------------

class TestTopMerchants:
    def test_returns_dataframe(self, executor):
        df = executor.execute("top_merchants", {"limit": 5})
        assert isinstance(df, pd.DataFrame)

    def test_has_expected_columns(self, executor):
        df = executor.execute("top_merchants")
        for col in ["merchant", "category", "visit_count", "total_spent", "avg_per_visit"]:
            assert col in df.columns

    def test_limit_respected(self, executor):
        df = executor.execute("top_merchants", {"limit": 3})
        assert len(df) <= 3

    def test_only_debits(self, executor):
        df = executor.execute("top_merchants")
        # Salary (credit) must not appear
        assert "Salary January" not in df["merchant"].values
        assert "Salary February" not in df["merchant"].values

    def test_ordered_by_total_spent_desc(self, executor):
        df = executor.execute("top_merchants")
        spent = df["total_spent"].tolist()
        assert spent == sorted(spent, reverse=True)


# ---------------------------------------------------------------------------
# PERIOD_FORMATS
# ---------------------------------------------------------------------------

class TestPeriodFormats:
    def test_all_granularities_defined(self):
        for key in ("daily", "weekly", "monthly"):
            assert key in PERIOD_FORMATS

    def test_format_strings_are_valid_strftime(self):
        import time
        # Should not raise
        for name, fmt in PERIOD_FORMATS.items():
            result = time.strftime(fmt)
            assert isinstance(result, str)
