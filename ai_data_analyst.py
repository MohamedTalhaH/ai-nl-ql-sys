# ========================= IMPORTS =========================
import streamlit as st
import pandas as pd
import re
import time
import json
import os
from sqlalchemy import create_engine
from difflib import get_close_matches

st.set_page_config(layout="wide")

# ========================= CONFIG =========================
DASHBOARD_FILE = "dashboards.json"

# ========================= STATE =========================
if "metrics" not in st.session_state:
    st.session_state.metrics = {"q":0,"success":0,"fail":0,"time":0}

if "widgets" not in st.session_state:
    st.session_state.widgets = []

# ========================= STORAGE =========================
def load_dashboards():
    if not os.path.exists(DASHBOARD_FILE):
        return {}
    with open(DASHBOARD_FILE, "r") as f:
        return json.load(f)

def save_dashboards(data):
    with open(DASHBOARD_FILE, "w") as f:
        json.dump(data, f, indent=2)

# ========================= COLUMN MATCH =========================
def map_columns(query, df_cols):
    query = query.lower()
    detected = []
    for col in df_cols:
        if col.lower() in query:
            detected.append(col)
        if get_close_matches(col.lower(), query.split(), cutoff=0.7):
            detected.append(col)
    return list(set(detected))

# ========================= AGGREGATION =========================
def detect_aggregation(query, df):
    q = query.lower()

    agg_map = {
        "sum":"SUM","total":"SUM",
        "avg":"AVG","average":"AVG",
        "count":"COUNT",
        "max":"MAX","maximum":"MAX",
        "min":"MIN","minimum":"MIN"
    }

    agg_func = None
    for k in agg_map:
        if k in q:
            agg_func = agg_map[k]

    value_col = None
    for col in df.columns:
        if df[col].dtype != "object" and col.lower() in q:
            value_col = col

    group_col = None
    for col in df.columns:
        if f"by {col.lower()}" in q:
            group_col = col

    return agg_func, value_col, group_col

# ========================= SQL =========================
def generate_sql(query, df):

    agg, val, grp = detect_aggregation(query, df)

    limit = None
    m = re.search(r"\d+", query)
    if m:
        limit = int(m.group())

    if agg and val:
        if grp:
            sql = f"SELECT `{grp}`, {agg}(`{val}`) FROM data GROUP BY `{grp}`"
            if limit:
                sql += f" LIMIT {limit}"
            return sql
        return f"SELECT {agg}(`{val}`) FROM data"

    cols = map_columns(query, df.columns.tolist())
    if cols:
        return f"SELECT {', '.join(cols)} FROM data LIMIT {limit or 10}"

    return "SELECT * FROM data LIMIT 10"

# ========================= UI =========================
st.title("🚀 Natural Language Data Analysis System")

file = st.file_uploader("Upload CSV")

if file:
    df = pd.read_csv(file)

    # ================= FILTERS =================
    st.sidebar.header("🔎 Filters")
    filtered_df = df.copy()

    for col in df.columns:
        values = df[col].dropna().astype(str).unique().tolist()

        try:
            values = sorted(values)
        except:
            pass

        selected = st.sidebar.multiselect(col, values, default=values)

        if selected:
            filtered_df = filtered_df[
                filtered_df[col].astype(str).isin(selected)
            ]

    # ================= DATABASE =================
    engine = create_engine("sqlite:///:memory:")
    filtered_df.to_sql("data", engine, index=False)

    # ================= DATA =================
    st.subheader("📂 Data Preview")
    st.dataframe(filtered_df, use_container_width=True)

    # ================= KPI =================
    m = st.session_state.metrics
    k1,k2,k3,k4 = st.columns(4)

    k1.metric("Queries", m["q"])
    k2.metric("Success %", (m["success"]/m["q"]*100) if m["q"] else 0)
    k3.metric("Failures", m["fail"])
    k4.metric("Avg Time", (m["time"]/m["q"]) if m["q"] else 0)

    # ================= QUERY =================
    query = st.text_input("Ask your question")

    if query:
        start = time.time()

        sql = generate_sql(query.lower(), filtered_df)

        try:
            result = pd.read_sql(sql, engine)
            success = True
        except:
            result = filtered_df.head(10)
            success = False

        # metrics update
        st.session_state.metrics["q"] += 1
        if success:
            st.session_state.metrics["success"] += 1
        else:
            st.session_state.metrics["fail"] += 1

        st.session_state.metrics["time"] += (time.time()-start)

        # ================= SQL =================
        st.subheader("🧾 SQL")
        st.code(sql, language="sql")

        # ================= RESULT =================
        st.subheader("📊 Result")
        st.dataframe(result, use_container_width=True)

        # ================= EXPORT =================
        st.subheader("📤 Export Result")

        export_format = st.selectbox("Choose format", ["CSV", "Excel", "PDF", "Word"])

        def convert_data(df, fmt):
            if fmt == "CSV":
                return df.to_csv(index=False).encode("utf-8")

            elif fmt == "Excel":
                from io import BytesIO
                buffer = BytesIO()
                df.to_excel(buffer, index=False)
                return buffer.getvalue()

            elif fmt == "PDF":
                from reportlab.platypus import SimpleDocTemplate, Table
                from io import BytesIO

                buffer = BytesIO()
                doc = SimpleDocTemplate(buffer)

                data = [df.columns.tolist()] + df.values.tolist()
                table = Table(data)

                doc.build([table])
                return buffer.getvalue()

            elif fmt == "Word":
                from docx import Document
                from io import BytesIO

                buffer = BytesIO()
                doc = Document()

                table = doc.add_table(rows=len(df)+1, cols=len(df.columns))

                for i, col in enumerate(df.columns):
                    table.rows[0].cells[i].text = col

                for i, row in df.iterrows():
                    for j, val in enumerate(row):
                        table.rows[i+1].cells[j].text = str(val)

                doc.save(buffer)
                return buffer.getvalue()

        file_data = convert_data(result, export_format)

        st.download_button(
            label="⬇ Download",
            data=file_data,
            file_name=f"result.{export_format.lower()}",
            mime="application/octet-stream"
        )

        # ================= INSIGHTS =================
        st.subheader("📊 Insights")

        if not result.empty:
            num_cols = result.select_dtypes(include=['number']).columns

            if len(num_cols) > 0:
                col = num_cols[0]

                c1,c2,c3,c4,c5 = st.columns(5)
                c1.metric("SUM", result[col].sum())
                c2.metric("AVG", result[col].mean())
                c3.metric("MAX", result[col].max())
                c4.metric("MIN", result[col].min())
                c5.metric("COUNT", result[col].count())

                st.dataframe(pd.DataFrame({
                    "Metric":["SUM","AVG","MAX","MIN","COUNT"],
                    "Value":[
                        result[col].sum(),
                        result[col].mean(),
                        result[col].max(),
                        result[col].min(),
                        result[col].count()
                    ]
                }), use_container_width=True)

        # ================= CHART =================
        if len(result.columns) >= 2:
            x, y = result.columns[:2]
            st.bar_chart(result.set_index(x)[y])
