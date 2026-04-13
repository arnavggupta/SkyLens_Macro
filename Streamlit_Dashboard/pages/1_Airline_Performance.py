import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px

st.title("✈️ Airline Performance")

session = get_active_session()

# ================================
# FILTER
# ================================

airlines = session.sql("""
    SELECT DISTINCT "airline_name"
    FROM GOLD.DIM_AIRLINE
    WHERE "airline_name" IS NOT NULL
    ORDER BY 1
""").to_pandas()["airline_name"].tolist()

selected_airline = st.selectbox("Select Airline", ["All"] + airlines)

view_type = st.radio("View", ["Top Airlines", "Worst Airlines (Delay)"])

filter_clause = ""
if selected_airline != "All":
    filter_clause = f""" AND A."airline_name" = '{selected_airline}' """

# ================================
# QUERY (FACT + DIM ONLY)
# ================================

df = session.sql(f"""
SELECT 
    A."airline_name",
    COUNT(*) AS total_flights,
    ROUND(AVG(F."arrival_delay_min"), 2) AS avg_arr_delay,
    ROUND(AVG(F."departure_delay_min"), 2) AS avg_dep_delay,
    ROUND(100 * AVG(IFF(F."is_cancelled", 1, 0)), 2) AS cancel_rate,
    ROUND(100 * AVG(IFF(F."arrival_delay_min" <= 15, 1, 0)), 2) AS on_time_pct
FROM GOLD.FACT_FLIGHTS F
JOIN GOLD.DIM_AIRLINE A
    ON F."airline_key" = A."airline_key"
WHERE A."airline_name" IS NOT NULL
{filter_clause}
GROUP BY 1
""").to_pandas()

df.columns = df.columns.str.lower()

# ================================
# SORTING LOGIC
# ================================

if view_type == "Top Airlines":
    df = df.sort_values("total_flights", ascending=False).head(10)
else:
    df = df.sort_values("avg_arr_delay", ascending=False).head(10)

# ================================
# KPIs
# ================================

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Flights", f"{int(df['total_flights'].sum()):,}")
c2.metric("Avg Arrival Delay", round(df["avg_arr_delay"].mean(), 2))
c3.metric("Avg Departure Delay", round(df["avg_dep_delay"].mean(), 2))
c4.metric("On-Time %", round(df["on_time_pct"].mean(), 2))

st.markdown("---")

# ================================
# 1️⃣ Flights by Airline (Plotly)
# ================================

st.subheader("📊 Flights by Airline")

fig1 = px.bar(
    df,
    x="airline_name",
    y="total_flights",
    title="Total Flights",
    color="airline_name"
)

st.plotly_chart(fig1, use_container_width=True)

# ================================
# 2️⃣ Delay Comparison
# ================================

st.subheader("⏱ Delay Comparison")

fig2 = px.bar(
    df,
    x="airline_name",
    y=["avg_arr_delay", "avg_dep_delay"],
    barmode="group",
    title="Arrival vs Departure Delay"
)

st.plotly_chart(fig2, use_container_width=True)

# ================================
# 3️⃣ On-Time Performance
# ================================

st.subheader("🎯 On-Time Performance")

fig3 = px.bar(
    df,
    x="airline_name",
    y="on_time_pct",
    color="on_time_pct",
    title="On-Time %",
)

st.plotly_chart(fig3, use_container_width=True)

# ================================
# CLEAN TABLE
# ================================

clean_df = df.copy()
clean_df.columns = [
    "Airline",
    "Flights",
    "Avg Arrival Delay",
    "Avg Departure Delay",
    "Cancellation %",
    "On-Time %"
]

with st.expander("🔍 View Detailed Data"):
    st.dataframe(clean_df)