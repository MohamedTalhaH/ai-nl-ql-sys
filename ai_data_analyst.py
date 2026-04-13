# ========================= IMPORTS =========================
import streamlit as st
import pandas as pd
import re
import time
import json
import os
from sqlalchemy import create_engine
from difflib import get_close_matches
from io import BytesIO

st.set_page_config(layout="wide")

# ========================= CONFIG =========================
DASHBOARD_FILE = "dashboards.json"

# ========================= STATE =========================
if "metrics" not in st.session_state:
    st.session_state.metrics = {"q":0,"success":0,"fail":0,"time":0}

if "widgets" not in st.session_state:
    st.session_state.widgets = []

# ========================= GROQ =========================
def groq_sql(query, df):
    try:
        from groq import Groq
    except:
        return None

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return None

    try:
        client = Groq(api_key=api_key)

        schema = ", ".join(df.columns)

        prompt = f"""
        Convert this natural language to SQL.
        Table: data
        Columns: {schema}
        Only return SQL.

        Query: {query}
        """

        res = client.chat.completions.create(
            model="llama3-70b-8192",
            messages=[{"role": "user", "content": prompt}]
        )

        text = res.choices[0].message.content

        match = re.search(r"(SELECT.*)", text, re.IGNORECASE)
        return match.group(1) if match else None

    except:
        return None

# ========================= STORAGE =========================
def load_dashboards():
    if not os.path.exists(DASHBOARD_FILE):
        return {}
    with open(DASHBOARD_FILE, "r") as f:
        return json.load(f)

def save_dashboards(data):
    with open(DASHBOARD_FILE, "w") as f:
        json.dump(data, f, indent=2)

# ========================= SQL HELPERS =========================
def map_columns(query, df_cols):
    query = query.lower()
    detected = []
    for col in df_cols:
        if col.lower() in query:
            detected.append(col)
        if get_close_matches(col.lower(), query.split(), cutoff=0.7):
            detected.append(col)
    return list(set(detected))

def detect_aggregation(query, df):
    q = query.lower()
    agg_map = {
        "sum":"SUM","total":"SUM",
        "avg":"AVG","average":"AVG",
        "count":"COUNT",
        "max":"MAX","maximum":"MAX",
        "min":"MIN","minimum":"MIN"
    }

    agg_func = next((agg_map[k] for k in agg_map if k in q), None)

    value_col = next((col for col in df.columns if df[col].dtype != "object" and col.lower() in q), None)
    group_col = next((col for col in df.columns if f"by {col.lower()}" in q), None)

    return agg_func, value_col, group_col

# ========================= HYBRID SQL =========================
def generate_sql(query, df):
    # Try Groq AI first
    sql = groq_sql(query, df)

    if sql:
        return sql

    # Fallback rule engine
    agg, val, grp = detect_aggregation(query, df)

    if agg and val:
        if grp:
            return f"SELECT `{grp}`, {agg}(`{val}`) FROM data GROUP BY `{grp}`"
        return f"SELECT {agg}(`{val}`) FROM data"

    cols = map_columns(query, df.columns.tolist())
    if cols:
        return f"SELECT {', '.join(cols)} FROM data LIMIT 10"

    return "SELECT * FROM data LIMIT 10"

# ========================= UI =========================
st.title("🚀 AI Data Analysis System")

# DEBUG (optional)
st.write("AI Enabled:", bool(os.getenv("GROQ_API_KEY")))

file = st.file_uploader("Upload CSV")

if file:
    df = pd.read_csv(file)

    # ================= FILTERS =================
    st.sidebar.header("🔎 Filters")
    filtered_df = df.copy()

    for col in df.columns:
        values = sorted(df[col].dropna().astype(str).unique())
        selected = st.sidebar.multiselect(col, values, default=values)

        if selected:
            filtered_df = filtered_df[filtered_df[col].astype(str).isin(selected)]

    if filtered_df.empty:
        st.warning("No data after filtering")
        st.stop()

    engine = create_engine("sqlite:///:memory:")
    filtered_df.to_sql("data", engine, index=False)

    # ================= DATA =================
    st.subheader("📂 Data Preview")
    st.dataframe(filtered_df)

    # ================= KPI =================
    m = st.session_state.metrics
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Queries", m["q"])
    c2.metric("Success %", (m["success"]/m["q"]*100) if m["q"] else 0)
    c3.metric("Failures", m["fail"])
    c4.metric("Avg Time", (m["time"]/m["q"]) if m["q"] else 0)

    # ================= QUERY =================
    query = st.text_input("Ask your question")

    if query:
        start = time.time()
        sql = generate_sql(query, filtered_df)

        try:
            result = pd.read_sql(sql, engine)
            success = True
        except Exception as e:
            st.error(f"SQL Error: {str(e)}")
            result = filtered_df.head(10)
            success = False

        # update metrics
        st.session_state.metrics["q"] += 1
        st.session_state.metrics["success"] += int(success)
        st.session_state.metrics["fail"] += int(not success)
        st.session_state.metrics["time"] += (time.time()-start)

        # SQL DISPLAY
        st.subheader("🧾 Generated SQL")
        st.code(sql, language="sql")

        # RESULT
        st.subheader("📊 Result")
        st.dataframe(result)

        # ================= EXPORT =================
        if not result.empty:
            st.subheader("📤 Export")

            format_type = st.selectbox("Format", ["CSV","Excel"])

            if format_type == "CSV":
                st.download_button("Download CSV", result.to_csv(index=False), "data.csv")

            if format_type == "Excel":
                buf = BytesIO()
                result.to_excel(buf, index=False)
                st.download_button("Download Excel", buf.getvalue(), "data.xlsx")

        # ================= INSIGHTS =================
        if not result.empty:
            num_cols = result.select_dtypes(include=['number']).columns
            if len(num_cols):
                col = num_cols[0]
                c1,c2,c3,c4,c5 = st.columns(5)
                c1.metric("SUM", result[col].sum())
                c2.metric("AVG", result[col].mean())
                c3.metric("MAX", result[col].max())
                c4.metric("MIN", result[col].min())
                c5.metric("COUNT", result[col].count())

        # ================= CHART =================
        if len(result.columns)>=2:
            x,y = result.columns[:2]
            st.bar_chart(result.set_index(x)[y])

        # ================= BUILDER =================
        st.subheader("🧩 Dashboard Builder")

        with st.expander("Add Chart"):
            chart_type = st.selectbox("Type", ["Bar","Line","Scatter"])
            x = st.selectbox("X Axis", result.columns)
            y = st.selectbox("Y Axis", result.columns)

            if st.button("Add Chart"):
                st.session_state.widgets.append({
                    "type": chart_type,
                    "x": x,
                    "y": y
                })

        for i,w in enumerate(st.session_state.widgets):
            try:
                if w["type"]=="Bar":
                    st.bar_chart(result.set_index(w["x"])[w["y"]])
                elif w["type"]=="Line":
                    st.line_chart(result.set_index(w["x"])[w["y"]])
                elif w["type"]=="Scatter":
                    st.scatter_chart(result[[w["x"],w["y"]]])
            except:
                pass

            if st.button(f"Remove {i}", key=i):
                st.session_state.widgets.pop(i)
                st.rerun()

        # ================= SAVE =================
        st.subheader("💾 Save / Load Report")

        name = st.text_input("Report Name")

        if st.button("Save"):
            db = load_dashboards()
            db[name] = st.session_state.widgets
            save_dashboards(db)
            st.success("Saved")

        db = load_dashboards()
        if db:
            sel = st.selectbox("Load", list(db.keys()))
            if st.button("Load"):
                st.session_state.widgets = db[sel]
                st.rerun()
