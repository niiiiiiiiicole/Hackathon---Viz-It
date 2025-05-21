# This is the playground for the Viz It tool and will be cleaned up to be a production version

import trino
import os
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
import streamlit as st
import threading
import time

load_dotenv(dotenv_path="/Users/nicole.li/PycharmProjects/Hackathon---Viz-It/cre.env")

conn = trino.dbapi.connect(
    host=os.getenv("TRINO_HOST"),
    port=int(os.getenv("TRINO_PORT", 443)),
    user=os.getenv("TRINO_USER"),
    catalog=os.getenv("TRINO_CATALOG"),
    http_scheme='https',
    auth=trino.auth.BasicAuthentication(
        os.getenv("TRINO_USER"),
        os.getenv("TRINO_PASSWORD")
    )
)

# Query input
default_query = os.getenv("DEFAULT_QUERY", "SELECT activity_month, activity_type, mrr__total FROM awsdatacatalog.product_analytics.cc_relational_migrator_cluster_monthly")
query = st.text_area("Enter your SQL query", value=default_query, height=200)

# Kill Trino button
trino_ui_url = "https://trino-adhoc.dataplatform.prod.corp.mongodb.com/ui/"
st.markdown(f"[🔗 Open Trino Query UI to Kill Query]({trino_ui_url})", unsafe_allow_html=True)


if "query_result" not in st.session_state:
    st.session_state["query_result"] = {"df": None, "error": None, "status": None}
if "stop_flag" not in st.session_state:
    st.session_state["stop_flag"] = {"stop": False}

if st.button("Run Query"):
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        st.session_state["df"] = pd.DataFrame(rows, columns=columns)
        st.success("Query completed!")
    except Exception as e:
        st.error(f"Query failed: {e}")

if st.button("Cancel Query"):
    st.session_state["stop_flag"]["stop"] = True
    st.warning("Query cancellation requested. Reload page if needed.")

if "df" in st.session_state:
    df = st.session_state["df"]
    st.success("Query completed!")
    st.dataframe(df)

    st.subheader("📊 Visualize Your Data")

    x_col = st.selectbox("Select X-axis", df.columns)
    y_cols = st.multiselect("Select Y-axis columns", df.select_dtypes(include=["number"]).columns)

    agg_map = {
        "Sum": "sum",
        "Average": "mean",
        "Min": "min",
        "Max": "max",
        "Median": "median"

    }

    selected_aggs = {}
    for y_col in y_cols:
        selected_aggs[y_col] = st.selectbox(f"Aggregation for {y_col}", ["None"] + list(agg_map.keys()), key=y_col)

    st.markdown("### 🔍 Filter Data")

    def add_filter_ui(col):
        col_data = df[col]
        if pd.api.types.is_numeric_dtype(col_data):
            min_val, max_val = float(col_data.min()), float(col_data.max())
            selected_range = st.slider(f"Filter {col} (range)", min_val, max_val, (min_val, max_val))
            return col_data.between(*selected_range)
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            min_date, max_date = pd.to_datetime(col_data.min()), pd.to_datetime(col_data.max())
            default_start = min_date.date()
            default_end = max_date.date()
            selected_dates = st.date_input(f"Filter {col} (date range)", (default_start, default_end))
            if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
                start_date = pd.to_datetime(selected_dates[0])
                end_date = pd.to_datetime(selected_dates[1])
                return pd.to_datetime(col_data).between(start_date, end_date)
            else:
                return pd.Series([True] * len(col_data))
        elif pd.api.types.is_bool_dtype(col_data):
            selected = st.multiselect(f"Filter {col} (boolean)", [True, False], default=[True, False])
            return col_data.isin(selected)
        elif pd.api.types.is_categorical_dtype(col_data) or pd.api.types.is_object_dtype(col_data):
            options = col_data.dropna().unique().tolist()
            if len(options) > 100:
                st.warning(f"{col} has too many unique values, filter skipped.")
                return pd.Series([True] * len(col_data))
            selected = st.multiselect(f"Filter {col} (categories)", options, default=options)
            return col_data.isin(selected)
        else:
            return pd.Series([True] * len(col_data))

    filter_mask = add_filter_ui(x_col)
    for y_col in y_cols:
        filter_mask &= add_filter_ui(y_col)

    df = df[filter_mask]

    agg_dict = {y: agg_map[agg] for y, agg in selected_aggs.items() if agg != "None"}
    if agg_dict:
        df = df.groupby(x_col, as_index=False).agg(agg_dict)

    if st.checkbox("🧪 Show Debug Panel"):
        st.write(f"Filtered rows: {df.shape[0]}")
        st.write("X-axis column:", x_col)
        st.write("Y-axis columns:", y_cols)
        st.write("Preview data:")
        st.dataframe(df[[x_col] + y_cols].head())

    chart_type = st.selectbox("Chart type", ["Line", "Bar", "Scatter"])

    for y_col in y_cols:
        if not pd.api.types.is_numeric_dtype(df[y_col]):
            st.warning(f"Skipping non-numeric Y-axis column: {y_col}")
            continue
        if chart_type == "Line" and (pd.api.types.is_bool_dtype(df[x_col]) or pd.api.types.is_bool_dtype(df[y_col])):
            st.error("Line charts are not supported for boolean data types.")
            continue
        if chart_type == "Line":
            fig = px.line(df, x=x_col, y=y_col, title=f"{y_col} over {x_col}")
        elif chart_type == "Bar":
            fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}")
        elif chart_type == "Scatter":
            fig = px.scatter(df, x=x_col, y=y_col, title=f"{y_col} vs {x_col}")

        st.plotly_chart(fig, use_container_width=True)

# Collapsible debug panel for session state
if st.checkbox("🔍 Debug: Show session state"):
    st.write("Query status:", st.session_state.get("query_result", {}).get("status"))
    st.write("Query error:", st.session_state.get("query_result", {}).get("error"))
    st.write("DataFrame in session state:", "df" in st.session_state)
    if "df" in st.session_state:
        st.write("df shape:", st.session_state["df"].shape)
    st.write("Session state keys:", list(st.session_state.keys()))
