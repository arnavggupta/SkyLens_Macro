import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px

st.title("🛫 Route Analysis")

session = get_active_session()

# ================================
# FILTER (Origin State)
# ================================

states = session.sql("""
    SELECT DISTINCT "origin_state"
    FROM GOLD.GOLD_ROUTE_PERFORMANCE
    WHERE "origin_state" IS NOT NULL
    ORDER BY 1
""").to_pandas()["origin_state"].tolist()

selected_state = st.selectbox("Filter by Origin State", ["All"] + states)

filter_clause = ""
if selected_state != "All":
    filter_clause = f""" WHERE "origin_state" = '{selected_state}' """

# ================================
# DATA
# ================================

df = session.sql(f"""
    SELECT 
        "origin_city",
        "dest_city",
        "origin_state",
        "dest_state",
        "total_flights",
        "avg_arrival_delay_min",
        "on_time_pct"
    FROM GOLD.GOLD_ROUTE_PERFORMANCE
    {filter_clause}
    ORDER BY "total_flights" DESC
    LIMIT 25
""").to_pandas()

df.columns = df.columns.str.lower()

# Create route column
df["route"] = df["origin_city"] + " → " + df["dest_city"]

# ================================
# KPIs
# ================================

c1, c2, c3 = st.columns(3)

c1.metric("Total Flights", f"{int(df['total_flights'].sum()):,}")
c2.metric("Avg Delay", round(df["avg_arrival_delay_min"].mean(), 2))
c3.metric("Avg On-Time %", round(df["on_time_pct"].mean(), 2))

st.markdown("---")

# ================================
# 1️⃣ Top Routes by Flights
# ================================

st.subheader("📊 Top Routes by Traffic")

fig1 = px.bar(
    df,
    x="route",
    y="total_flights",
    color="origin_state",
    title="Top Routes by Total Flights"
)

st.plotly_chart(fig1, use_container_width=True)

# ================================
# 2️⃣ Delay Analysis
# ================================

st.subheader("⏱ Delay by Route")

fig2 = px.bar(
    df,
    x="route",
    y="avg_arrival_delay_min",
    color="origin_state",
    title="Average Arrival Delay per Route"
)

st.plotly_chart(fig2, use_container_width=True)

# ================================
# 3️⃣ On-Time Performance
# ================================

st.subheader("📊 On-Time Performance")

fig3 = px.bar(
    df,
    x="route",
    y="on_time_pct",
    color="origin_state",
    title="On-Time % per Route"
)

st.plotly_chart(fig3, use_container_width=True)

# ================================
# TABLE
# ================================

# Cleaned table
clean_df = df[[
    "route",
    "total_flights",
    "avg_arrival_delay_min",
    "on_time_pct"
]]

clean_df.columns = [
    "Route",
    "Flights",
    "Avg Delay (min)",
    "On-Time %"
]

# Optional: Hide under expander (BEST UX)
with st.expander("🔍 View Detailed Route Data"):
    st.dataframe(clean_df)