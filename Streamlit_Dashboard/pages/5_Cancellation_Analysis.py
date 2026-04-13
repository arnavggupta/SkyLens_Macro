import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px

st.title("❌ Cancellation Analysis")

session = get_active_session()

# ================================
# DATA (FACT + DIM)
# ================================

# FILTER
airlines = session.sql("""
    SELECT DISTINCT "airline_name"
    FROM GOLD.DIM_AIRLINE
    WHERE "airline_name" IS NOT NULL
""").to_pandas()["airline_name"].tolist()

selected_airline = st.selectbox("Airline", ["All"] + airlines)

filter_clause = ""
if selected_airline != "All":
    filter_clause = f""" AND A."airline_name" = '{selected_airline}' """

df = session.sql(f"""
    SELECT 
        A."airline_name",
        COALESCE(C."cancellation_desc", 'Unknown') AS reason,
        COUNT(*) AS total_cancellations
    FROM GOLD.FACT_FLIGHTS F
    JOIN GOLD.DIM_AIRLINE A
        ON F."airline_key" = A."airline_key"
    LEFT JOIN GOLD.DIM_CANCELLATION_REASON C
        ON F."cancellation_key" = C."cancellation_key"
    WHERE F."is_cancelled" = TRUE
    {filter_clause}
    GROUP BY 1, 2
""").to_pandas()

df.columns = df.columns.str.lower()

# ================================
# KPI
# ================================

total_cancel = int(df["total_cancellations"].sum())
st.metric("Total Cancellations", f"{total_cancel:,}")

st.markdown("---")

# ================================
# 1️⃣ Airline-wise Cancellations
# ================================

st.subheader("📊 Cancellations by Airline")

df_airline = df.groupby("airline_name", as_index=False)["total_cancellations"].sum()

fig1 = px.bar(
    df_airline,
    x="airline_name",
    y="total_cancellations",
    title="Total Cancellations by Airline"
)

st.plotly_chart(fig1, use_container_width=True)

# ================================
# 2️⃣ Reason Breakdown (STACKED)
# ================================

st.subheader("📊 Cancellation Reasons Breakdown")

fig2 = px.bar(
    df,
    x="airline_name",
    y="total_cancellations",
    color="reason",
    title="Cancellation Reasons by Airline",
    barmode="stack"
)

st.plotly_chart(fig2, use_container_width=True)

# ================================
# 3️⃣ % Contribution (🔥 IMPORTANT)
# ================================

st.subheader("📊 Cancellation Reason % Contribution")

df_pct = df.copy()

# total per airline
df_pct["total"] = df_pct.groupby("airline_name")["total_cancellations"].transform("sum")

df_pct["percentage"] = (df_pct["total_cancellations"] / df_pct["total"]) * 100

fig3 = px.bar(
    df_pct,
    x="airline_name",
    y="percentage",
    color="reason",
    title="Cancellation Reason Contribution (%)",
    barmode="stack"
)

st.plotly_chart(fig3, use_container_width=True)

# ================================
# TABLE
# ================================
with st.expander("🔍 View Data"):
    st.dataframe(df)