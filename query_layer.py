"""
SQL Query Layer — personal-finance-helper
==========================================
Named SQL queries + QueryExecutor for the analytics layer.

All queries are SQLite-compatible (strftime instead of DATE_TRUNC).
The same :param convention works identically with PostgreSQL.

Usage:
    from queries import QueryExecutor, PERIOD_FORMATS

    executor = QueryExecutor(engine)

    # Running balance — all accounts
    df = executor.execute("running_balance")

    # Monthly spend by category
    df = executor.execute(
        "spend_by_period",
        params={"account": None, "date_from": "2024-01-01", "date_to": None},
        template_vars={"period_fmt": PERIOD_FORMATS["monthly"]},
    )

    # List available query names
    print(executor.list_queries())
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ---------------------------------------------------------------------------
# Period format strings (SQLite strftime / PostgreSQL to_char compatible)
# ---------------------------------------------------------------------------

PERIOD_FORMATS: dict[str, str] = {
    "daily":   "%Y-%m-%d",
    "weekly":  "%Y-%W",
    "monthly": "%Y-%m",
}


# ---------------------------------------------------------------------------
# Named SQL queries
# Each string starts with a -- name: <id> header for documentation.
# The dict key is the canonical query name used by QueryExecutor.
# Template placeholders like {period_fmt} are substituted by the executor
# before the query is sent to the DB (safe: values always come from
# PERIOD_FORMATS, never from raw user input).
# ---------------------------------------------------------------------------

QUERY_REGISTRY: dict[str, str] = {

    # ------------------------------------------------------------------
    # running_balance
    # Computes a cumulative running balance per account using a window
    # function. Rows ordered by date ascending within each account.
    # Params: account (str|None), date_from (str|None), date_to (str|None)
    # ------------------------------------------------------------------
    "running_balance": """
-- name: running_balance
SELECT
    t.date,
    t.account,
    t.description,
    t.category,
    t.amount,
    SUM(t.amount) OVER (
        PARTITION BY t.account
        ORDER BY t.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_balance
FROM transactions t
WHERE (:account IS NULL OR t.account = :account)
  AND (:date_from IS NULL OR t.date >= :date_from)
  AND (:date_to   IS NULL OR t.date <= :date_to)
ORDER BY t.account, t.date
""",

    # ------------------------------------------------------------------
    # spend_by_period
    # Groups debit transactions by time bucket and category.
    # Template var {period_fmt} is one of PERIOD_FORMATS values.
    # Params: account (str|None), date_from (str|None), date_to (str|None)
    # ------------------------------------------------------------------
    "spend_by_period": """
-- name: spend_by_period
SELECT
    strftime('{period_fmt}', t.date)   AS period,
    t.category,
    COUNT(*)                           AS transaction_count,
    SUM(ABS(t.amount))                 AS total_spent
FROM transactions t
WHERE t.amount < 0
  AND (:account   IS NULL OR t.account  = :account)
  AND (:date_from IS NULL OR t.date    >= :date_from)
  AND (:date_to   IS NULL OR t.date    <= :date_to)
GROUP BY period, t.category
ORDER BY period, total_spent DESC
""",

    # ------------------------------------------------------------------
    # category_breakdown
    # Summarises spending and income per category.
    # Params: account (str|None), date_from (str|None), date_to (str|None)
    # ------------------------------------------------------------------
    "category_breakdown": """
-- name: category_breakdown
SELECT
    t.category,
    COUNT(*)                                                         AS transaction_count,
    SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END)       AS total_spent,
    SUM(CASE WHEN t.amount > 0 THEN t.amount       ELSE 0 END)       AS total_received,
    AVG(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE NULL END)     AS avg_spent_per_tx
FROM transactions t
WHERE (:account   IS NULL OR t.account  = :account)
  AND (:date_from IS NULL OR t.date    >= :date_from)
  AND (:date_to   IS NULL OR t.date    <= :date_to)
GROUP BY t.category
ORDER BY total_spent DESC
""",

    # ------------------------------------------------------------------
    # account_summary
    # One row per account: total credits, total debits, net balance,
    # transaction count, and date range.
    # Params: date_from (str|None), date_to (str|None)
    # ------------------------------------------------------------------
    "account_summary": """
-- name: account_summary
SELECT
    t.account,
    COUNT(*)                                                         AS transaction_count,
    SUM(CASE WHEN t.amount > 0 THEN t.amount       ELSE 0 END)       AS total_credits,
    SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount)  ELSE 0 END)       AS total_debits,
    SUM(t.amount)                                                    AS net_balance,
    MIN(t.date)                                                      AS first_transaction,
    MAX(t.date)                                                      AS last_transaction
FROM transactions t
WHERE (:date_from IS NULL OR t.date >= :date_from)
  AND (:date_to   IS NULL OR t.date <= :date_to)
GROUP BY t.account
ORDER BY t.account
""",

    # ------------------------------------------------------------------
    # top_merchants
    # Ranks merchants by total spend (debits only).
    # Params: account (str|None), date_from (str|None), date_to (str|None),
    #         limit (int, default 20)
    # ------------------------------------------------------------------
    "top_merchants": """
-- name: top_merchants
SELECT
    t.description                                                    AS merchant,
    t.category,
    COUNT(*)                                                         AS visit_count,
    SUM(ABS(t.amount))                                               AS total_spent,
    AVG(ABS(t.amount))                                               AS avg_per_visit
FROM transactions t
WHERE t.amount < 0
  AND (:account   IS NULL OR t.account  = :account)
  AND (:date_from IS NULL OR t.date    >= :date_from)
  AND (:date_to   IS NULL OR t.date    <= :date_to)
GROUP BY t.description, t.category
ORDER BY total_spent DESC
LIMIT :limit
""",

}


# ---------------------------------------------------------------------------
# Default parameters (used when caller omits optional params)
# ---------------------------------------------------------------------------

_QUERY_DEFAULTS: dict[str, dict] = {
    "running_balance":    {"account": None, "date_from": None, "date_to": None},
    "spend_by_period":    {"account": None, "date_from": None, "date_to": None},
    "category_breakdown": {"account": None, "date_from": None, "date_to": None},
    "account_summary":    {"date_from": None, "date_to": None},
    "top_merchants":      {"account": None, "date_from": None, "date_to": None, "limit": 20},
}


# ---------------------------------------------------------------------------
# QueryExecutor
# ---------------------------------------------------------------------------

class QueryExecutor:
    """Execute named SQL queries against a SQLAlchemy engine, returning DataFrames.

    All queries are stored in QUERY_REGISTRY and parameterised with SQLAlchemy
    bound parameters (:param_name). Optional template variables ({var}) are
    substituted before the query reaches the DB driver — these are always
    sourced from internal constants (e.g. PERIOD_FORMATS), never raw user input.

    Args:
        engine: SQLAlchemy engine (SQLite or PostgreSQL).

    Example::

        executor = QueryExecutor(engine)
        df = executor.execute("category_breakdown", {"date_from": "2024-01-01"})
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    # ------------------------------------------------------------------

    def execute(
        self,
        name: str,
        params: Optional[dict] = None,
        template_vars: Optional[dict] = None,
    ) -> pd.DataFrame:
        """Run a named query and return the result as a DataFrame.

        Args:
            name: Query name — must be a key in QUERY_REGISTRY.
            params: Bound parameter values. Missing keys fall back to the
                query's default (usually None). Passing None runs with all defaults.
            template_vars: Python-format substitutions applied to the SQL string
                before execution. Use for strftime format strings, etc.

        Returns:
            pandas DataFrame with one column per SELECT field.

        Raises:
            KeyError: If `name` is not registered.
        """
        if name not in QUERY_REGISTRY:
            available = ", ".join(sorted(QUERY_REGISTRY))
            raise KeyError(
                f"Unknown query '{name}'. Available: {available}"
            )

        sql = QUERY_REGISTRY[name]

        # Apply template substitutions (internal constants only)
        if template_vars:
            sql = sql.format(**template_vars)

        # Merge caller params over defaults
        merged = dict(_QUERY_DEFAULTS.get(name, {}))
        if params:
            merged.update(params)

        with self.engine.connect() as conn:
            result = conn.execute(text(sql), merged)
            rows = result.fetchall()
            columns = list(result.keys())

        return pd.DataFrame(rows, columns=columns)

    # ------------------------------------------------------------------

    def list_queries(self) -> list[str]:
        """Return sorted list of available query names."""
        return sorted(QUERY_REGISTRY.keys())

    # ------------------------------------------------------------------

    def spend_by_period(
        self,
        granularity: str = "monthly",
        account: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> pd.DataFrame:
        """Convenience wrapper for spend_by_period with granularity resolution.

        Args:
            granularity: One of 'daily', 'weekly', 'monthly'.
            account: Filter to a specific account IBAN, or None for all.
            date_from: ISO date string lower bound (inclusive), or None.
            date_to: ISO date string upper bound (inclusive), or None.

        Returns:
            DataFrame with columns: period, category, transaction_count, total_spent.

        Raises:
            ValueError: If granularity is not recognised.
        """
        if granularity not in PERIOD_FORMATS:
            raise ValueError(
                f"granularity must be one of {list(PERIOD_FORMATS)}, got '{granularity}'"
            )
        return self.execute(
            "spend_by_period",
            params={"account": account, "date_from": date_from, "date_to": date_to},
            template_vars={"period_fmt": PERIOD_FORMATS[granularity]},
        )
