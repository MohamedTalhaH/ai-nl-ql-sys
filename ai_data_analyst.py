# ========================= IMPORTS =========================
import streamlit as st
import pandas as pd
import re
import json
import os
import time
from sqlalchemy import create_engine
from difflib import get_close_matches
from io import BytesIO
import google.generativeai as genai

st.set_page_config(layout="wide")

DASHBOARD_FILE = "dashboards.json"

# ========================= STATE =========================
if "widgets" not in st.session_state:
    st.session_state.widgets = []

if "metrics" not in st.session_state:
    st.session_state.metrics = {
        "total": 0,
        "success": 0,
        "fail": 0,
        "time": 0
    }

# ========================= GEMINI =========================
st.sidebar.header("⚙️ AI Settings")
gemini_key = st.sidebar.text_input("Gemini API Key", type="password")

if gemini_key:
    genai.configure(api_key=gemini_key)
    st.sidebar.success("Gemini Connected ✅")
else:
    st.sidebar.warning("No API Key ❌")

# ========================= GEMINI → INTENT =========================
def gemini_to_intent(query, df):

    if not gemini_key:
        return None

    models = [
        "gemini-3.1-flash-lite-preview",  # your requested
        "gemini-2.5-flash-latest",        # fallback
        "gemini-1.5-flash-latest"         # safe fallback
    ]

    prompt = f"""
Return ONLY valid JSON.

Columns: {df.columns.tolist()}

FORMAT:
{{
"agg": "sum/avg/count/max/min/none",
"column": "column_name",
"group_by": "column_name or none",
"condition": "SQL condition or none",
"limit": "number",
"sort": "asc/desc/none"
}}

Rules:
- No explanation
- Only JSON
- Use double quotes

Query: {query}
"""

    for m in models:
        try:
            model = genai.GenerativeModel(m)

            res = model.generate_content(prompt, generation_config={"temperature":0})

            text = res.text.strip()

            # extract JSON safely
            match = re.search(r"\{[\s\S]*\}", text)
            if match:
                parsed = json.loads(match.group())
                st.success(f"Using Model: {m} ✅")
                return parsed

        except Exception as e:
            continue

    st.warning("All Gemini models failed → fallback")
    return None

# ========================= RULE ENGINE =========================
def safe_col(c):
    return f"`{c}`"

def match_col(name, cols):
    matches = get_close_matches(str(name), cols, n=1)
    return matches[0] if matches else None

def build_sql(intent, df):

    if not intent:
        return "SELECT * FROM data LIMIT 10"

    agg = intent.get("agg", "none")
    col = match_col(intent.get("column"), df.columns)
    grp = match_col(intent.get("group_by"), df.columns)
    cond = intent.get("condition")
    limit = intent.get("limit") or "10"
    sort = intent.get("sort")

    sql = ""

    if agg != "none" and col:
        if grp:
            sql = f"SELECT {safe_col(grp)}, {agg.upper()}({safe_col(col)})"
        else:
            sql = f"SELECT {agg.upper()}({safe_col(col)})"
    else:
        sql = "SELECT *"

    sql += " FROM data"

    if cond and cond != "none":
        sql += f" WHERE {cond}"

    if grp:
        sql += f" GROUP BY {safe_col(grp)}"

    if sort != "none" and col:
        sql += f" ORDER BY {safe_col(col)} {sort.upper()}"

    sql += f" LIMIT {limit}"

    return sql

# ========================= STORAGE =========================
def load_dashboards():
    if not os.path.exists(DASHBOARD_FILE):
        return {}
    return json.load(open(DASHBOARD_FILE))

def save_dashboards(data):
    json.dump(data, open(DASHBOARD_FILE,"w"), indent=2)

def normalize_result(result):
    if result is None or result.empty:
        return result

    # clean column names
    result.columns = [col.strip() for col in result.columns]

    # rename SQL expressions to clean names
    new_cols = []
    for col in result.columns:
        if "SUM(" in col:
            new_cols.append("sum_" + col.split("(")[-1].replace(")", ""))
        elif "AVG(" in col:
            new_cols.append("avg_" + col.split("(")[-1].replace(")", ""))
        elif "COUNT(" in col:
            new_cols.append("count")
        elif "MAX(" in col:
            new_cols.append("max_" + col.split("(")[-1].replace(")", ""))
        elif "MIN(" in col:
            new_cols.append("min_" + col.split("(")[-1].replace(")", ""))
        else:
            new_cols.append(col)

    result.columns = new_cols

    return result

def prepare_chart_data(result, x, y):

    # remove duplicate columns
    df = result.loc[:, ~result.columns.duplicated()].copy()

    if x not in df.columns or y not in df.columns:
        return None

    x_data = df[x]
    y_data = df[y]

    # ensure y is 1D
    if isinstance(y_data, pd.DataFrame):
        y_data = y_data.iloc[:, 0]

    # convert numeric safely
    y_data = pd.to_numeric(y_data, errors="coerce")

    df_plot = pd.DataFrame({
        x: x_data,
        y: y_data
    }).dropna()

    return df_plot if not df_plot.empty else None
# ========================= UI =========================
st.title("🚀 AI Data Analysis System")
result = st.session_state.get("last_result", None)
file = st.file_uploader("Upload CSV")

if file:
    df = pd.read_csv(file)

    # FILTERS
    st.sidebar.header("🔎 Filters")
    filtered_df = df.copy()

    for col in df.columns:
        vals = df[col].astype(str).unique()
        sel = st.sidebar.multiselect(col, vals, default=vals)
        filtered_df = filtered_df[filtered_df[col].astype(str).isin(sel)]

    if filtered_df.empty:
        st.warning("No data after filtering")
        st.stop()

    engine = create_engine("sqlite:///:memory:")
    filtered_df.to_sql("data", engine, index=False)

    st.dataframe(filtered_df)

    query = st.text_input("Ask your question")

    if query:
        start = time.time()
        st.session_state.metrics["total"] += 1

        intent = gemini_to_intent(query, filtered_df)

        st.subheader("🧠 Parsed Intent")
        st.json(intent)

        sql = build_sql(intent, filtered_df)

        st.subheader("🧾 SQL")
        st.code(sql)

        try:
            result = pd.read_sql(sql, engine)
            result = normalize_result(result)

# ✅ store CLEAN result
            st.session_state["last_result"] = result
            st.session_state.metrics["success"] += 1
        except:
            result = filtered_df.head(10)
            st.session_state.metrics["fail"] += 1

        st.session_state.metrics["time"] += (time.time() - start)

        st.subheader("📊 Result")
        st.dataframe(result)
        
        # ================= INSIGHTS =================
        st.subheader("📈 Insights")

        num_cols = result.select_dtypes(include=['number']).columns.tolist()

        if len(num_cols):
            col = num_cols[0]
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("SUM", result[col].sum())
            c2.metric("AVG", result[col].mean())
            c3.metric("MAX", result[col].max())
            c4.metric("MIN", result[col].min())
            c5.metric("COUNT", result[col].count())
            
        # ================= PERFORMANCE =================
        st.subheader("📊 Query Performance")

        m = st.session_state.metrics
        total = m["total"]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Queries", total)
        c2.metric("Success", m["success"])
        c3.metric("Failed", m["fail"])
        c4.metric("Avg Time (s)", round(m["time"]/max(total,1), 3))

        # ================= EXPORT =================
        st.subheader("📤 Export")

        fmt = st.selectbox("Format", ["CSV", "Excel"])

        if fmt == "CSV":
            st.download_button("Download CSV", result.to_csv(index=False), "data.csv")

        elif fmt == "Excel":
            buf = BytesIO()
            result.to_excel(buf, index=False)
            st.download_button("Download Excel", buf.getvalue(), "data.xlsx")

if "last_result" in st.session_state:
    result = st.session_state["last_result"]
else:
    result = None
# ================= CHART BUILDER =================
st.subheader("📊 Chart Builder")

result = st.session_state.get("last_result", None)

if result is not None and len(result.columns) >= 2:

    cols = list(result.columns)

    chart_type = st.selectbox("Chart Type", ["Bar", "Line", "Scatter"])

    x = st.selectbox("X Axis", cols)
    y = st.selectbox("Y Axis", cols)

    if x == y:
        st.warning("X and Y cannot be same")

    elif x in cols and y in cols:

        try:
            if chart_type == "Bar":
                st.bar_chart(result.set_index(x)[y])
            elif chart_type == "Line":
                st.line_chart(result.set_index(x)[y])
            elif chart_type == "Scatter":
                st.scatter_chart(result[[x, y]])

        except Exception as e:
            st.error(f"Chart failed: {e}")

    if st.button("Add Chart"):
        st.session_state.widgets.append({
            "x": x,
            "y": y,
            "type": chart_type
        })
# ================= DASHBOARD =================
st.subheader("🧩 Dashboard")

result = st.session_state.get("last_result", None)

if result is not None and st.session_state.widgets:

    for i, w in enumerate(st.session_state.widgets):

        try:
            if w["type"] == "Bar":
                st.bar_chart(result.set_index(w["x"])[w["y"]])
            elif w["type"] == "Line":
                st.line_chart(result.set_index(w["x"])[w["y"]])
            elif w["type"] == "Scatter":
                st.scatter_chart(result[[w["x"], w["y"]]])

        except Exception:
            st.warning(f"Chart {i} failed")

        if st.button(f"Remove {i}", key=f"remove_{i}"):
            st.session_state.widgets.pop(i)
            st.rerun()
# ================= SAVE =================
st.subheader("💾 Save Report")

name = st.text_input("Report Name")

if st.button("Save"):
    db = load_dashboards()
    db[name] = st.session_state.widgets
    save_dashboards(db)
    st.success("Saved")

# ================= LOAD =================
st.subheader("📂 Load Report")

db = load_dashboards()

if db:
    selected = st.selectbox("Select Report", list(db.keys()))

    if st.button("Load"):
        st.session_state.widgets = db[selected]
        st.success(f"Loaded: {selected}")
        st.rerun()

