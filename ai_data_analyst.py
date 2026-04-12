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
    st.sidebar.header("Filters")
    filtered_df = df.copy()

    for col in df.columns:
        if df[col].dtype == "object":
            val = st.sidebar.multiselect(col, df[col].unique())
            if val:
                filtered_df = filtered_df[filtered_df[col].isin(val)]

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

        # metrics
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

        # ================= INSIGHTS =================
        st.subheader("📊 Insights")

        if not result.empty:
            num_cols = result.select_dtypes(include=['number']).columns

            if len(num_cols) > 0:
                col = num_cols[0]

                total = result[col].sum()
                avg = result[col].mean()
                mx = result[col].max()
                mn = result[col].min()
                cnt = result[col].count()

                c1,c2,c3,c4,c5 = st.columns(5)
                c1.metric("SUM", f"{total:.2f}")
                c2.metric("AVG", f"{avg:.2f}")
                c3.metric("MAX", f"{mx:.2f}")
                c4.metric("MIN", f"{mn:.2f}")
                c5.metric("COUNT", cnt)

                st.dataframe(pd.DataFrame({
                    "Metric":["SUM","AVG","MAX","MIN","COUNT"],
                    "Value":[total,avg,mx,mn,cnt]
                }), use_container_width=True)

        # ================= DEFAULT CHART =================
        if len(result.columns)>=2:
            x,y = result.columns[:2]
            st.bar_chart(result.set_index(x)[y])

        # ================= BUILDER =================
        st.subheader("🧩 Dashboard Builder")

        with st.expander("Add Chart"):
            t = st.selectbox("Type", ["Bar","Line","Scatter"])
            x = st.selectbox("X", result.columns)
            y = st.selectbox("Y", result.columns)

            if st.button("Add Chart"):
                st.session_state.widgets.append({
                    "type": t,
                    "x": x,
                    "y": y
                })

        # SAFE RENDER
        for i, w in enumerate(st.session_state.widgets):

            chart_type = w.get("type")
            x_col = w.get("x")
            y_col = w.get("y")

            if x_col not in result.columns or y_col not in result.columns:
                continue

            try:
                if chart_type == "Bar":
                    st.bar_chart(result.set_index(x_col)[y_col])
                elif chart_type == "Line":
                    st.line_chart(result.set_index(x_col)[y_col])
                elif chart_type == "Scatter":
                    st.scatter_chart(result[[x_col, y_col]])
            except:
                pass

            if st.button(f"Remove {i}", key=f"remove_{i}"):
                st.session_state.widgets.pop(i)
                st.rerun()

        # ================= SAVE =================
        st.subheader("💾 Save Report")

        name = st.text_input("Report Name")

        if st.button("Save Report"):
            db = load_dashboards()
            db[name] = st.session_state.widgets
            save_dashboards(db)
            st.success("Saved!")

        db = load_dashboards()

        if db:
            sel = st.selectbox("Load Report", list(db.keys()))
            if st.button("Load"):
                st.session_state.widgets = db[sel]
