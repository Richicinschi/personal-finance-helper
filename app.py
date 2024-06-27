"""
Personal Finance Helper — Streamlit Application
================================================
Multi-tab interactive dashboard for ING Bank CSV transaction analysis.

Run locally:
    streamlit run app.py

With Docker:
    docker compose up
    # then open http://localhost:8501
"""

import os
import tempfile
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy.engine import Engine

from pipeline import make_engine, run_pipeline
from query_layer import PERIOD_FORMATS, QueryExecutor

# ---------------------------------------------------------------------------
# Page config (must be first Streamlit call)
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Personal Finance Helper",
    page_icon="💶",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

def _init_state() -> None:
    defaults = {
        "engine":       None,   # SQLAlchemy engine (set after file upload)
        "raw_count":    0,
        "proc_count":   0,
        "accounts":     [],     # list of account IBANs loaded from DB
        "sel_accounts": [],     # multiselect value
        "granularity":  "monthly",
        "date_from":    None,
        "date_to":      None,
        "filename":     None,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

_init_state()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_url() -> str:
    return os.getenv("DATABASE_URL", "sqlite:///finance.db")


def _get_engine() -> Optional[Engine]:
    return st.session_state["engine"]


def _has_data() -> bool:
    return st.session_state["engine"] is not None and st.session_state["proc_count"] > 0


def _executor() -> Optional[QueryExecutor]:
    eng = _get_engine()
    return QueryExecutor(eng) if eng else None


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def render_sidebar() -> None:
    with st.sidebar:
        st.title("Filters")
        st.caption("Controls apply to the Analytics tab.")

        if not _has_data():
            st.info("Upload a file in **Data Management** to enable filters.")
            return

        # Account multiselect
        accounts = st.session_state["accounts"]
        st.session_state["sel_accounts"] = st.multiselect(
            "Accounts",
            options=accounts,
            default=accounts,
            help="Select one or more account IBANs to include.",
        )

        # Granularity
        st.session_state["granularity"] = st.radio(
            "Time granularity",
            options=["daily", "weekly", "monthly"],
            index=["daily", "weekly", "monthly"].index(
                st.session_state["granularity"]
            ),
            horizontal=True,
        )

        # Date range
        st.markdown("**Date range**")
        col1, col2 = st.columns(2)
        with col1:
            df_val = st.date_input("From", value=None, key="date_from_input")
            st.session_state["date_from"] = str(df_val) if df_val else None
        with col2:
            dt_val = st.date_input("To", value=None, key="date_to_input")
            st.session_state["date_to"] = str(dt_val) if dt_val else None

        st.divider()
        st.caption(
            f"Loaded: **{st.session_state['proc_count']:,}** transactions  \n"
            f"File: `{st.session_state['filename']}`"
        )


# ---------------------------------------------------------------------------
# Tab 1 — Landing
# ---------------------------------------------------------------------------

def render_landing() -> None:
    st.title("Personal Finance Helper")
    st.subheader("Transform your bank exports into actionable insights")

    st.markdown("""
Welcome! This dashboard ingests **ING Bank Netherlands CSV exports** and turns raw
transaction data into interactive analytics — no spreadsheets required.

---

### How it works

```
CSV Upload  →  Extract  →  Transform  →  Load (SQLite / PostgreSQL)  →  Charts
```

| Step | What happens |
|---|---|
| **Extract** | Reads CSV or Excel, normalises column names to snake_case |
| **Transform** | Parses YYYYMMDD dates, converts comma-decimal amounts, signs debits negative, classifies transactions into categories |
| **Load** | Inserts raw and processed rows into `raw_transactions` and `transactions` tables |
| **Query** | Named SQL queries using window functions and time-bucketing return pandas DataFrames |
| **Visualise** | Plotly charts rendered in the Analytics tab, filtered by account and time granularity |

---

### Standard transaction schema

After transformation, every transaction has these fields:

| Field | Type | Description |
|---|---|---|
| `transaction_type` | str | Payment terminal, Transfer, iDEAL, etc. |
| `date` | date | ISO 8601 (parsed from YYYYMMDD integer) |
| `description` | str | Merchant / counterparty name |
| `amount` | float | Positive = credit, Negative = debit |
| `currency` | str | Always `EUR` for ING NL exports |
| `category` | str | Rule-based: Groceries, Transport, Dining, … |
| `account` | str | Account IBAN |
| `status` | str | Always `verified` (all exported rows are settled) |

---

### Quick start

**Option A — Local (SQLite)**
```bash
pip install -r requirements.txt
streamlit run app.py
# Upload your bank.csv in the Data Management tab
```

**Option B — Docker (PostgreSQL)**
```bash
cp .env.example .env   # edit DATABASE_URL if needed
docker compose up
# Open http://localhost:8501
```

---

### Analytics available

- **Running balance** — cumulative account balance over time (line chart)
- **Spend by period** — daily / weekly / monthly breakdown by category (bar chart)
- **Category breakdown** — where your money goes, ranked by total spend (horizontal bar)

Use the sidebar filters to narrow by account or date range.
""")

    st.info(
        "**Next step:** Go to the **Data Management** tab and upload your bank CSV export.",
        icon="👆",
    )


# ---------------------------------------------------------------------------
# Tab 2 — Data Management
# ---------------------------------------------------------------------------

def render_data_management() -> None:
    st.header("Data Management")

    # ---- File upload ----
    st.subheader("Upload transaction file")
    uploaded = st.file_uploader(
        "Choose a CSV or Excel file exported from your bank",
        type=["csv", "xlsx", "xls"],
        help="ING Bank NL: Mijn ING > Transacties > Downloaden (CSV)",
    )

    if uploaded is not None:
        suffix = Path(uploaded.name).suffix
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(uploaded.getvalue())
            tmp_path = tmp.name

        with st.spinner(f"Running ETL pipeline on {uploaded.name}…"):
            try:
                engine = make_engine(_db_url())
                counts = run_pipeline(tmp_path, engine, verbose=False)

                # Refresh account list from DB
                ex = QueryExecutor(engine)
                acct_df = ex.execute("account_summary")

                st.session_state["engine"]       = engine
                st.session_state["raw_count"]    = counts["raw_rows"]
                st.session_state["proc_count"]   = counts["processed_rows"]
                st.session_state["accounts"]     = acct_df["account"].tolist()
                st.session_state["sel_accounts"] = acct_df["account"].tolist()
                st.session_state["filename"]     = uploaded.name

                st.success(
                    f"Loaded **{counts['processed_rows']:,}** transactions "
                    f"({counts['raw_rows']:,} raw rows) from `{uploaded.name}`"
                )
            except Exception as e:
                st.error(f"Pipeline error: {e}")
            finally:
                os.unlink(tmp_path)

    # ---- Data explorers ----
    if not _has_data():
        st.info("No data loaded yet. Upload a file above.")
        return

    eng = _get_engine()
    ex = QueryExecutor(eng)

    st.divider()

    # Metrics row
    summary = ex.execute("account_summary")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Transactions", f"{st.session_state['proc_count']:,}")
    col2.metric("Total Credits", f"€{summary['total_credits'].sum():,.2f}")
    col3.metric("Total Debits",  f"€{summary['total_debits'].sum():,.2f}")
    col4.metric("Net Balance",   f"€{summary['net_balance'].sum():,.2f}")

    st.subheader("Processed Transactions")
    proc_df = pd.read_sql("SELECT * FROM transactions ORDER BY date DESC LIMIT 500", eng)
    st.dataframe(proc_df, use_container_width=True, height=350)

    st.subheader("Raw Transactions (last 200 rows)")
    raw_df = pd.read_sql("SELECT * FROM raw_transactions ORDER BY rowid DESC LIMIT 200", eng)
    st.dataframe(raw_df, use_container_width=True, height=300)


# ---------------------------------------------------------------------------
# Tab 3 — Analytics
# ---------------------------------------------------------------------------

def render_analytics() -> None:
    st.header("Analytics Dashboard")

    if not _has_data():
        st.info(
            "No data loaded. Upload your bank CSV in the **Data Management** tab first.",
            icon="📂",
        )
        return

    ex = _executor()
    account_filter = st.session_state["sel_accounts"] or None
    # If all accounts selected pass None (no filter), else first selected
    acct_param = None if not account_filter or len(account_filter) == len(st.session_state["accounts"]) \
        else account_filter[0]
    date_from = st.session_state["date_from"]
    date_to   = st.session_state["date_to"]
    gran      = st.session_state["granularity"]

    # ---- Chart 1: Running Balance ----
    st.subheader("Running Balance Over Time")
    try:
        rb_df = ex.execute(
            "running_balance",
            {"account": acct_param, "date_from": date_from, "date_to": date_to},
        )
        if rb_df.empty:
            st.warning("No data for the selected filters.")
        else:
            fig = px.line(
                rb_df,
                x="date",
                y="running_balance",
                color="account",
                labels={"running_balance": "Balance (EUR)", "date": "Date"},
                template="plotly_white",
            )
            fig.update_traces(mode="lines")
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Running balance chart error: {e}")

    st.divider()

    # ---- Chart 2: Spend by Period ----
    st.subheader(f"Spend by {gran.capitalize()} Period")
    try:
        sp_df = ex.spend_by_period(
            granularity=gran,
            account=acct_param,
            date_from=date_from,
            date_to=date_to,
        )
        if sp_df.empty:
            st.warning("No spend data for the selected filters.")
        else:
            fig2 = px.bar(
                sp_df,
                x="period",
                y="total_spent",
                color="category",
                labels={"total_spent": "Spent (EUR)", "period": gran.capitalize()},
                template="plotly_white",
            )
            st.plotly_chart(fig2, use_container_width=True)
    except Exception as e:
        st.error(f"Spend by period chart error: {e}")

    st.divider()

    # ---- Chart 3: Category Breakdown ----
    st.subheader("Spending by Category")
    try:
        cb_df = ex.execute(
            "category_breakdown",
            {"account": acct_param, "date_from": date_from, "date_to": date_to},
        )
        # Only show categories with actual spend
        cb_df = cb_df[cb_df["total_spent"] > 0]
        if cb_df.empty:
            st.warning("No category spend data for the selected filters.")
        else:
            col_a, col_b = st.columns([2, 1])
            with col_a:
                fig3 = px.bar(
                    cb_df,
                    x="total_spent",
                    y="category",
                    orientation="h",
                    labels={"total_spent": "Total Spent (EUR)", "category": "Category"},
                    template="plotly_white",
                    color="total_spent",
                    color_continuous_scale="Blues",
                )
                fig3.update_layout(showlegend=False, coloraxis_showscale=False)
                st.plotly_chart(fig3, use_container_width=True)
            with col_b:
                st.dataframe(
                    cb_df[["category", "transaction_count", "total_spent", "avg_spent_per_tx"]]
                    .rename(columns={
                        "transaction_count": "# Txn",
                        "total_spent": "Total (EUR)",
                        "avg_spent_per_tx": "Avg (EUR)",
                    })
                    .style.format({"Total (EUR)": "{:.2f}", "Avg (EUR)": "{:.2f}"}),
                    use_container_width=True,
                    height=400,
                )
    except Exception as e:
        st.error(f"Category breakdown chart error: {e}")


# ---------------------------------------------------------------------------
# Main layout
# ---------------------------------------------------------------------------

def main() -> None:
    render_sidebar()

    tab_landing, tab_data, tab_analytics = st.tabs([
        "Home",
        "Data Management",
        "Analytics",
    ])

    with tab_landing:
        render_landing()

    with tab_data:
        render_data_management()

    with tab_analytics:
        render_analytics()


main()
