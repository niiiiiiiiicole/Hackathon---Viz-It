# This is the playground for the Viz It tool and will be cleaned up to be a production version

import trino
import os
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
import streamlit as st
import threading
import time
import subprocess

# load_dotenv(dotenv_path="/Users/nicole.li/PycharmProjects/Hackathon---Viz-It/cre.env")
#
# conn = trino.dbapi.connect(
#     host=os.getenv("TRINO_HOST"),
#     port=int(os.getenv("TRINO_PORT", 443)),
#     user=os.getenv("TRINO_USER"),
#     catalog=os.getenv("TRINO_CATALOG"),
#     http_scheme='https',
#     auth=trino.auth.BasicAuthentication(
#         os.getenv("TRINO_USER"),
#         os.getenv("TRINO_PASSWORD")
#     )
# )

# Use 1password CLI to get credentials
def get_1password_secret(field, item):
    result = subprocess.run(
        ["op", "item", "get", item, f"--fields", field, "--reveal"],
        stdout=subprocess.PIPE,
        text=True
    )
    return result.stdout.strip()

trino_user = get_1password_secret("username", "TrinoCredentials")
trino_password = get_1password_secret("password", "TrinoCredentials")

# Connect to Trino using the credentials retrieved from 1Password
conn = trino.dbapi.connect(
    host='presto-gateway.corp.mongodb.com',
    port=443,
    user=trino_user,
    catalog='awsdatacatalog',
    http_scheme='https',
    auth=trino.auth.BasicAuthentication(trino_user, trino_password),
)

# Query input
default_query = os.getenv("DEFAULT_QUERY", "SELECT activity_month, org_id, activity_type, mrr__total FROM awsdatacatalog.product_analytics.cc_relational_migrator_cluster_monthly")
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
        st.session_state["raw_df"] = pd.DataFrame(rows, columns=columns)
        st.session_state["df"] = st.session_state["raw_df"].copy()
        st.success("Query completed!")
    except Exception as e:
        st.error(f"Query failed: {e}")

if st.button("Cancel Query"):
    st.session_state["stop_flag"]["stop"] = True
    st.warning("Query cancellation requested. Reload page if needed.")

if "raw_df" in st.session_state:
    df = st.session_state["raw_df"].copy()
    st.dataframe(df)

    st.markdown("### 📈 Summary Statistics (Numerical Columns Only)")
    if not df.select_dtypes(include="number").empty:
        stats_df = df.describe(percentiles=[0.25, 0.5, 0.75]).T.rename(columns={
            "25%": "Q1",
            "50%": "Median",
            "75%": "Q3"
        })[["count", "mean", "min", "Q1", "Median", "Q3", "max"]]
        # Add missing value counts per numeric column
        missing_counts = df.select_dtypes(include="number").isna().sum()
        stats_df["missing"] = missing_counts
        stats_df = stats_df[["count", "missing", "mean", "min", "Q1", "Median", "Q3", "max"]]
        st.dataframe(stats_df.style.format(precision=2))
    else:
        st.info("No numeric columns to summarize.")

    st.subheader("📊 Visualize Your Data")

    x_cols = st.multiselect("Select X-axis columns", df.columns, default=[df.columns[0]])
    # Determine color_by_default as the x_col with the fewest unique values
    color_by_default = x_cols[0] if x_cols else None
    if x_cols:
        unique_counts = {col: df[col].nunique() for col in x_cols}
        color_by_default = min(unique_counts, key=unique_counts.get)
    # Make color_col optional (allow None) with safe index logic
    color_options = ["None"] + x_cols
    color_index = 0
    color_col = st.selectbox("Group/color by", color_options, index=color_index)
    color_col = None if color_col == "None" else color_col
    # Allow sorting by any column, not just X-axis columns, and make sort_col optional
    sort_options = list(df.columns)
    sort_options_full = ["None"] + sort_options
    sort_index = 0
    sort_col = st.selectbox("Sort chart by", sort_options_full, index=sort_index)
    sort_col = None if sort_col == "None" else sort_col
    y_cols = st.multiselect("Select Y-axis columns", df.columns)

    agg_map = {
        "Sum": "sum",
        "Average": "mean",
        "Min": "min",
        "Max": "max",
        "Median": "median",
        "Count": "count",
        "Count Distinct": "nunique"
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
            options = sorted(col_data.dropna().unique().tolist())
            if len(options) > 100:
                st.warning(f"{col} has too many unique values, filter skipped.")
                return pd.Series([True] * len(col_data))

            select_all = st.checkbox(f"Select all for {col}", value=True, key=f"{col}_select_all")
            selected = st.multiselect(
                f"Filter {col} (categories)",
                options,
                default=options if select_all else [],
                key=f"{col}_filter",
                help="Type to filter options"
            )
            return col_data.isin(selected)
        else:
            return pd.Series([True] * len(col_data))

    filter_mask = add_filter_ui(x_cols[0]) if x_cols else pd.Series([True] * len(df))
    for y_col in y_cols:
        filter_mask &= add_filter_ui(y_col)

    df = df[filter_mask]

    if x_cols and len(x_cols) > 0:
        df["_x_combined"] = df[x_cols].astype(str).agg(" | ".join, axis=1)
    else:
        df["_x_combined"] = ""

    agg_dict = {
        y: agg_map[agg]
        for y, agg in selected_aggs.items() if agg != "None"
    }
    if agg_dict and x_cols:
        df = df.groupby(x_cols, as_index=False).agg(agg_dict)
        # Re-create _x_combined after grouping for chart plotting
        if x_cols and len(x_cols) > 0:
            df["_x_combined"] = df[x_cols].astype(str).agg(" | ".join, axis=1)

    if st.checkbox("🧪 Show Debug Panel"):
        st.write(f"Filtered rows: {df.shape[0]}")
        st.write("X-axis column:", x_cols)
        st.write("Y-axis columns:", y_cols)
        st.write("Preview data:")
        st.dataframe(df[['_x_combined'] + y_cols].head())

    chart_type = st.selectbox("Chart type", ["Line", "Bar", "Scatter"])

    if "_x_combined" in df.columns:
        numeric_y_cols = [
            y for y in y_cols
            if selected_aggs.get(y) != "Count" or pd.api.types.is_numeric_dtype(df[y])
        ]
        if not numeric_y_cols:
            st.warning("No numeric Y-axis columns selected.")
        elif chart_type == "Scatter" and len(numeric_y_cols) > 1:
            st.warning("Scatter plots support only one Y-axis column.")
        else:
            # Sort the DataFrame by the selected sort column before plotting, if set
            if sort_col:
                df = df.sort_values(by=sort_col)
            # Use selected color_col and sort_col, fallback to _x_combined for x if sort_col not set
            if chart_type == "Line":
                if len(numeric_y_cols) > 1 or color_col:
                    id_vars = [col for col in [sort_col or "_x_combined", color_col] if col and col in df.columns]
                    df_long = df.melt(
                        id_vars=id_vars,
                        value_vars=numeric_y_cols,
                        var_name="variable",
                        value_name="value"
                    )
                    if color_col:
                        df_long["series_label"] = df_long[color_col].astype(str) + " | " + df_long["variable"]
                    else:
                        df_long["series_label"] = df_long["variable"]

                    fig = px.line(
                        df_long,
                        x=sort_col or "_x_combined",
                        y="value",
                        color="series_label",
                        markers=True,
                        title=f"{', '.join(numeric_y_cols)} over {sort_col or '_x_combined'}"
                    )
                else:
                    fig = px.line(
                        df,
                        x=sort_col or "_x_combined",
                        y=numeric_y_cols[0],
                        markers=True,
                        title=f"{numeric_y_cols[0]} over {sort_col or '_x_combined'}"
                    )
            elif chart_type == "Bar":
                fig = px.bar(
                    df,
                    x=sort_col or "_x_combined",
                    y=numeric_y_cols,
                    color=color_col,
                    barmode="group",
                    title=f"{', '.join(numeric_y_cols)} by {sort_col or '_x_combined'}" + (f" and {color_col}" if color_col else "")
                )
            elif chart_type == "Scatter":
                fig = px.scatter(
                    df,
                    x=sort_col or "_x_combined",
                    y=numeric_y_cols[0],
                    color=color_col,
                    title=f"{numeric_y_cols[0]} vs {sort_col or '_x_combined'}" + (f" by {color_col}" if color_col else "")
                )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No valid X-axis selected to generate charts.")

    # Attempt to parse object-type columns as datetime if not already parsed
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    # Optional Period-over-Period (PoP) Comparison Chart
    st.markdown("### 📈 Period-over-Period Comparison (YoY / MoM / WoW)")

    if st.checkbox("Enable period-over-period comparison"):
        date_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
        if not date_cols:
            st.warning("No datetime columns available for comparison.")
        else:
            pop_date_col = st.selectbox("Select date column for comparison", date_cols)
            pop_metric_col = st.selectbox("Select metric for comparison", numeric_y_cols if 'numeric_y_cols' in locals() and numeric_y_cols else df.select_dtypes(include='number').columns)
            pop_mode = st.selectbox("Comparison type", ["Year over Year", "Month over Month", "Week over Week"])
            group_col = st.selectbox(
                "Optional group by field",
                ["None"] + [col for col in df.columns if col not in [pop_date_col, pop_metric_col]],
                index=0
            )
            group_col = None if group_col == "None" else group_col

            base_cols = [pop_date_col, pop_metric_col] + ([group_col] if group_col else [])
            pop_df = df[base_cols].dropna()
            if pop_mode == "Year over Year":
                pop_df["year"] = pop_df[pop_date_col].dt.year
                pop_df["month"] = pop_df[pop_date_col].dt.month
                x_col = "month"
                color_col = "year"
            elif pop_mode == "Month over Month":
                pop_df["year_month"] = pop_df[pop_date_col].dt.to_period("M").astype(str)
                pop_df["day"] = pop_df[pop_date_col].dt.day
                x_col = "day"
                color_col = "year_month"
            elif pop_mode == "Week over Week":
                pop_df["week"] = pop_df[pop_date_col].dt.isocalendar().week
                pop_df["week_year"] = pop_df[pop_date_col].dt.strftime("%Y-W%U")
                x_col = "week"
                color_col = "week_year"

            group_fields = [x_col, color_col] + ([group_col] if group_col else [])
            agg_pop_df = pop_df.groupby(group_fields, as_index=False)[pop_metric_col].mean()

            agg_pop_df["series_label"] = agg_pop_df[color_col].astype(str)
            if group_col:
                agg_pop_df["series_label"] = agg_pop_df[group_col].astype(str) + " | " + agg_pop_df["series_label"]

            fig = px.line(
                agg_pop_df,
                x=x_col,
                y=pop_metric_col,
                color="series_label",
                markers=True,
                title=f"{pop_metric_col} - {pop_mode}"
            )
            st.plotly_chart(fig, use_container_width=True)

# Collapsible debug panel for session state
if st.checkbox("🔍 Debug: Show session state"):
    st.write("Query status:", st.session_state.get("query_result", {}).get("status"))
    st.write("Query error:", st.session_state.get("query_result", {}).get("error"))
    st.write("DataFrame in session state:", "df" in st.session_state)
    if "df" in st.session_state:
        st.write("df shape:", st.session_state["df"].shape)
    st.write("Session state keys:", list(st.session_state.keys()))


