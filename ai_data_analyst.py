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
from reportlab.platypus import SimpleDocTemplate, Table
from docx import Document

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
    return json.load(open(DASHBOARD_FILE))

def save_dashboards(data):
    json.dump(data, open(DASHBOARD_FILE,"w"), indent=2)

# ========================= NLP SQL =========================
def map_columns(query, cols):
    query = query.lower()
    detected = []
    for c in cols:
        if c.lower() in query or any(get_close_matches(c.lower(), query.split(), cutoff=0.7)):
            detected.append(c)
    return list(set(detected))

def detect_agg(query, df):
    q = query.lower()
    agg_map = {
        "sum":"SUM","total":"SUM",
        "avg":"AVG","average":"AVG",
        "count":"COUNT",
        "max":"MAX","maximum":"MAX",
        "min":"MIN","minimum":"MIN"
    }

    agg = next((agg_map[k] for k in agg_map if k in q), None)
    val = next((c for c in df.columns if c.lower() in q), None)
    grp = next((c for c in df.columns if f"by {c.lower()}" in q), None)

    return agg, val, grp

def generate_sql(query, df):
    agg,val,grp = detect_agg(query, df)

    if agg and val:
        if grp:
            return f"SELECT `{grp}`, {agg}(`{val}`) FROM data GROUP BY `{grp}`"
        return f"SELECT {agg}(`{val}`) FROM data"

    cols = map_columns(query, df.columns)
    if cols:
        return f"SELECT {', '.join(cols)} FROM data LIMIT 10"

    return "SELECT * FROM data LIMIT 10"

# ========================= UI =========================
st.title("🚀 AI Data Analysis Dashboard")

file = st.file_uploader("Upload CSV")

if file:
    df = pd.read_csv(file)

    # ================= FILTERS =================
    st.sidebar.header("Filters")
    filtered_df = df.copy()

    for col in df.columns:
        vals = sorted(df[col].dropna().astype(str).unique())
        selected = st.sidebar.multiselect(col, vals, default=vals)
        if selected:
            filtered_df = filtered_df[filtered_df[col].astype(str).isin(selected)]

    engine = create_engine("sqlite:///:memory:")
    filtered_df.to_sql("data", engine, index=False)

    # ================= KPI =================
    m = st.session_state.metrics
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Queries", m["q"])
    c2.metric("Success %", (m["success"]/m["q"]*100) if m["q"] else 0)
    c3.metric("Failures", m["fail"])
    c4.metric("Avg Time", (m["time"]/m["q"]) if m["q"] else 0)

    # ================= DATA =================
    st.subheader("Data Preview")
    st.dataframe(filtered_df)

    # ================= QUERY =================
    query = st.text_input("Ask your query")

    if query:
        start = time.time()
        sql = generate_sql(query, filtered_df)

        # metrics
        st.session_state.metrics["q"] += 1

        try:
            result = pd.read_sql(sql, engine)
            st.session_state.metrics["success"] += 1
        except:
            result = filtered_df.head(10)
            st.session_state.metrics["fail"] += 1

        st.session_state.metrics["time"] += (time.time()-start)

        # SQL display
        st.subheader("Generated SQL")
        st.code(sql, language="sql")

        # result
        st.subheader("Result")
        st.dataframe(result)

        # ================= EXPORT =================
        st.subheader("Export")

        fmt = st.selectbox("Format", ["CSV","Excel","PDF","Word"])

        if fmt == "CSV":
            st.download_button("Download", result.to_csv(index=False), "data.csv")

        elif fmt == "Excel":
            buf = BytesIO()
            result.to_excel(buf, index=False)
            st.download_button("Download", buf.getvalue(), "data.xlsx")

        elif fmt == "PDF":
            buf = BytesIO()
            doc = SimpleDocTemplate(buf)
            data = [result.columns.tolist()] + result.values.tolist()
            doc.build([Table(data)])
            st.download_button("Download", buf.getvalue(), "data.pdf")

        elif fmt == "Word":
            buf = BytesIO()
            doc = Document()
            table = doc.add_table(rows=len(result)+1, cols=len(result.columns))
            for i,col in enumerate(result.columns):
                table.rows[0].cells[i].text = col
            for i,row in result.iterrows():
                for j,val in enumerate(row):
                    table.rows[i+1].cells[j].text = str(val)
            doc.save(buf)
            st.download_button("Download", buf.getvalue(), "data.docx")

        # ================= INSIGHTS =================
        nums = result.select_dtypes(include=['number']).columns
        if len(nums) > 0:
            col = nums[0]
            c1,c2,c3,c4,c5 = st.columns(5)
            c1.metric("SUM", result[col].sum())
            c2.metric("AVG", result[col].mean())
            c3.metric("MAX", result[col].max())
            c4.metric("MIN", result[col].min())
            c5.metric("COUNT", len(result))

        # ================= CHART =================
        if len(result.columns) >= 2:
            x,y = result.columns[:2]
            st.bar_chart(result.set_index(x)[y])

        # ================= DASHBOARD BUILDER =================
        st.subheader("Dashboard Builder")

        with st.expander("Add Chart"):
            t = st.selectbox("Type", ["Bar","Line","Scatter"])
            x = st.selectbox("X", result.columns)
            y = st.selectbox("Y", result.columns)

            if st.button("Add Chart"):
                st.session_state.widgets.append({"type":t,"x":x,"y":y})

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

            if st.button(f"Remove {i}", key=f"r{i}"):
                st.session_state.widgets.pop(i)
                st.rerun()

        # ================= SAVE / LOAD =================
        st.subheader("Save / Load Report")

        name = st.text_input("Report Name")

        if st.button("Save"):
            db = load_dashboards()
            db[name] = st.session_state.widgets.copy()
            save_dashboards(db)
            st.success("Saved")

        db = load_dashboards()
        if db:
            sel = st.selectbox("Load Report", list(db.keys()))
            if st.button("Load"):
                st.session_state.widgets = db[sel]
                st.rerun()
