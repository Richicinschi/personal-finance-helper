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

    upload_mode = st.radio(
        "Load mode",
        options=["Replace all data", "Append to existing"],
        index=0,
        horizontal=True,
        help=(
            "**Replace** clears the database before loading — use this for a fresh import. "
            "**Append** adds the new file on top of existing data — use when loading "
            "multiple CSV files that cover different date ranges."
        ),
    )
    mode_arg = "replace" if upload_mode == "Replace all data" else "append"

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
                counts = run_pipeline(tmp_path, engine, verbose=False, mode=mode_arg)

                # Read total rows now in DB (not just this file's rows)
                from sqlalchemy import text as _text
                with engine.connect() as _conn:
                    total_in_db = _conn.execute(_text("SELECT COUNT(*) FROM transactions")).scalar()

                # Refresh account list from DB
                ex = QueryExecutor(engine)
                acct_df = ex.execute("account_summary")

                st.session_state["engine"]       = engine
                st.session_state["raw_count"]    = counts["raw_rows"]
                st.session_state["proc_count"]   = total_in_db
                st.session_state["accounts"]     = acct_df["account"].tolist()
                st.session_state["sel_accounts"] = acct_df["account"].tolist()
                st.session_state["filename"]     = uploaded.name

                action = "Replaced with" if mode_arg == "replace" else "Appended"
                st.success(
                    f"{action} **{counts['processed_rows']:,}** transactions from "
                    f"`{uploaded.name}`. "
                    f"**Total in database: {total_in_db:,}**"
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
                    color="category",
                    color_discrete_sequence=px.colors.qualitative.Plotly,
                )
                fig3.update_layout(showlegend=False)
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
# Tab 4 — Custom Explorer
# ---------------------------------------------------------------------------

# Human-readable labels for known raw column names (both ING and Revolut)
_COLUMN_LABELS: dict[str, str] = {
    # ING
    "name_description": "Description",
    "counterparty":     "Counterparty IBAN",
    "transaction_type": "Transaction Type",
    "code":             "ING Code (BA / GT / OV …)",
    "debit_credit":     "Debit / Credit",
    "notifications":    "Notifications",
    # Revolut
    "description":      "Description",
    "type":             "Transaction Type",
    "state":            "State",
    "product":          "Product",
    "currency":         "Currency",
    # Date buckets (added by _load_raw_df)
    "date_day":         "Date — Daily",
    "date_week":        "Date — Weekly",
    "date_month":       "Date — Monthly",
}

# Columns that are never useful for grouping
_GROUPBY_EXCLUDE = {
    "id", "source_file", "date", "started_date", "completed_date",
    "amount_eur", "amount", "fee", "balance",
    "amount_signed", "date_parsed",
}


def _build_groupby_options(df: pd.DataFrame) -> dict[str, str]:
    """Return groupby options from the columns actually present in df."""
    date_cols = {"date_day": "Date — Daily", "date_week": "Date — Weekly", "date_month": "Date — Monthly"}
    options = {}
    for col in df.columns:
        if col in _GROUPBY_EXCLUDE:
            continue
        if col in date_cols:
            continue  # added below in fixed order
        label = _COLUMN_LABELS.get(col, col.replace("_", " ").title())
        options[col] = label
    # Always append date buckets at the end in consistent order
    for col, label in date_cols.items():
        if col in df.columns:
            options[col] = label
    return options

_METRIC_OPTIONS: dict[str, str] = {
    "total_eur":  "Total EUR",
    "count":      "Transaction count",
    "avg_eur":    "Average EUR per transaction",
}


def _load_raw_df(engine) -> pd.DataFrame:
    """Load raw_transactions and enrich with parsed amount and date columns.

    Supports both ING (amount_eur + debit_credit) and Revolut (amount, pre-signed).
    """
    df = pd.read_sql("SELECT * FROM raw_transactions", engine)
    cols = set(df.columns)

    # --- Amount ---
    if "amount_eur" in cols:
        # ING: comma-decimal unsigned string + Debit/Credit column
        amounts = (
            df["amount_eur"].astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )
        df["amount_signed"] = amounts.where(
            df["debit_credit"].str.strip().str.lower() == "credit", -amounts
        )
    elif "amount" in cols:
        # Revolut (and other formats): already a signed float
        df["amount_signed"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    else:
        df["amount_signed"] = 0.0

    # --- Date ---
    # ING uses YYYYMMDD integers; Revolut uses ISO 8601 strings with time
    date_col = "date" if "date" in cols else (
        "completed_date" if "completed_date" in cols else None
    )
    if date_col:
        sample = str(df[date_col].dropna().iloc[0]) if len(df) > 0 else ""
        if "-" in sample:
            dates = pd.to_datetime(df[date_col], format="mixed", dayfirst=False)
        else:
            dates = pd.to_datetime(df[date_col].astype(str), format="%Y%m%d")
    else:
        dates = pd.Series([pd.NaT] * len(df))

    df["date_day"]    = dates.dt.strftime("%Y-%m-%d")
    df["date_week"]   = dates.dt.strftime("%Y-%W")
    df["date_month"]  = dates.dt.strftime("%Y-%m")
    df["date_parsed"] = dates

    return df


def render_explorer() -> None:
    st.header("Transaction Explorer")
    st.caption(
        "Group and visualise any field from the raw transaction data. "
        "All original columns are preserved — no renaming."
    )

    if not _has_data():
        st.info("Upload data in the **Data Management** tab first.", icon="📂")
        return

    raw = _load_raw_df(_get_engine())
    groupby_options = _build_groupby_options(raw)

    # ---- Controls ----
    with st.container():
        c1, c2, c3 = st.columns(3)

        with c1:
            group_key = st.selectbox(
                "Group by",
                options=list(groupby_options.keys()),
                format_func=lambda k: groupby_options[k],
            )

        with c2:
            metric_key = st.selectbox(
                "Metric",
                options=list(_METRIC_OPTIONS.keys()),
                format_func=lambda k: _METRIC_OPTIONS[k],
            )

        with c3:
            direction = st.radio(
                "Include",
                options=["All", "Debits only", "Credits only"],
                horizontal=True,
            )

        c4, c5, c6 = st.columns(3)
        with c4:
            _n_unique = raw[group_key].nunique()
            top_n = st.slider(
                f"Show top N groups (of {_n_unique:,})",
                min_value=1,
                max_value=_n_unique,
                value=min(20, _n_unique),
                step=1,
            )
        with c5:
            date_from_raw = st.date_input("Date from", value=None, key="explorer_date_from")
        with c6:
            date_to_raw   = st.date_input("Date to",   value=None, key="explorer_date_to")

    # ---- Apply filters ----
    df = raw.copy()

    if direction == "Debits only":
        df = df[df["amount_signed"] < 0]
    elif direction == "Credits only":
        df = df[df["amount_signed"] > 0]

    if date_from_raw:
        df = df[df["date_parsed"] >= pd.Timestamp(date_from_raw)]
    if date_to_raw:
        df = df[df["date_parsed"] <= pd.Timestamp(date_to_raw)]

    if df.empty:
        st.warning("No rows match the current filters.")
        return

    # ---- Aggregate ----
    group_col = group_key  # the actual column name in df

    grouped = (
        df.groupby(group_col, dropna=False)
        .agg(
            total_eur  =("amount_signed", "sum"),
            count      =("amount_signed", "count"),
            avg_eur    =("amount_signed", "mean"),
        )
        .reset_index()
    )

    # For debits we want "money out" as positive numbers in the chart
    if direction == "Debits only":
        grouped["total_eur"] = grouped["total_eur"].abs()
        grouped["avg_eur"]   = grouped["avg_eur"].abs()

    metric_col = metric_key
    grouped = grouped.sort_values(metric_col, ascending=False).head(top_n)

    # ---- Chart ----
    st.subheader(f"{_METRIC_OPTIONS[metric_key]} by {groupby_options[group_key]}")

    is_date_group = group_key.startswith("date_")

    if is_date_group:
        # Vertical bar for time-series — single neutral color, no gradient
        grouped_sorted = grouped.sort_values(group_col)
        fig = px.bar(
            grouped_sorted,
            x=group_col,
            y=metric_col,
            labels={group_col: "Period", metric_col: _METRIC_OPTIONS[metric_key]},
            template="plotly_white",
        )
    else:
        # Horizontal bar — distinct color per bar
        fig = px.bar(
            grouped,
            x=metric_col,
            y=group_col,
            orientation="h",
            labels={group_col: groupby_options[group_key], metric_col: _METRIC_OPTIONS[metric_key]},
            template="plotly_white",
            color=group_col,
            color_discrete_sequence=px.colors.qualitative.Plotly,
        )
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            showlegend=False,
        )

    st.plotly_chart(fig, use_container_width=True)

    # ---- Table ----
    st.subheader("Detail table")

    display = grouped.rename(columns={
        group_col:   groupby_options[group_key],
        "total_eur": "Total (EUR)",
        "count":     "# Transactions",
        "avg_eur":   "Avg (EUR)",
    })

    st.dataframe(
        display.style.format({
            "Total (EUR)": "{:.2f}",
            "Avg (EUR)":   "{:.2f}",
        }),
        use_container_width=True,
        height=min(60 + len(display) * 35, 600),
    )

    # ---- Raw drill-down ----
    with st.expander("Raw transactions for selected group"):
        options = grouped[group_col].tolist()
        selected_vals = st.multiselect(
            f"Pick one or more {groupby_options[group_key]} values to inspect",
            options=options,
            default=options[:1],
            key="drilldown_select",
        )
        if not selected_vals:
            st.info("Select at least one value above to see details.")
        else:
            drill = df[df[group_col].isin(selected_vals)].sort_values("date_parsed", ascending=False)

            # Total sum metric
            total = drill["amount_signed"].sum()
            st.metric("Total amount (EUR)", f"{total:+,.2f}")

            # Pie chart — one slice per selected value, sized by abs sum
            pie_data = (
                drill.groupby(group_col)["amount_signed"]
                .sum()
                .reset_index()
                .rename(columns={"amount_signed": "sum_amount"})
            )
            pie_data["abs_sum"] = pie_data["sum_amount"].abs()
            pie_data["label"] = pie_data.apply(
                lambda r: f"{r[group_col]}: {r['sum_amount']:+,.2f} EUR", axis=1
            )
            fig_pie = px.pie(
                pie_data,
                names=group_col,
                values="abs_sum",
                hover_name="label",
                color_discrete_sequence=px.colors.qualitative.Plotly,
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig_pie, use_container_width=True)

            # Raw transactions table
            st.write(f"**{len(drill)} transactions**")
            st.dataframe(
                drill.drop(columns=["date_parsed", "date_day", "date_week", "date_month",
                                     "amount_signed"], errors="ignore"),
                use_container_width=True,
                height=400,
            )


# ---------------------------------------------------------------------------
# Main layout
# ---------------------------------------------------------------------------

def main() -> None:
    render_sidebar()

    tab_landing, tab_data, tab_analytics, tab_explorer = st.tabs([
        "Home",
        "Data Management",
        "Analytics",
        "Explorer",
    ])

    with tab_landing:
        render_landing()

    with tab_data:
        render_data_management()

    with tab_analytics:
        render_analytics()

    with tab_explorer:
        render_explorer()


main()
