import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px

st.title("🏢 Airport Operations")

session = get_active_session()

# ================================
# FILTER
# ================================

traffic_types = session.sql("""
    SELECT DISTINCT "traffic_type"
    FROM GOLD.GOLD_AIRPORT_TRAFFIC
""").to_pandas()["traffic_type"].dropna().tolist()

selected_type = st.selectbox("Traffic Type", ["All"] + traffic_types)

view_type = st.radio("View", ["Top Traffic", "Highest Delay"])

filter_clause = ""
if selected_type != "All":
    filter_clause = f""" WHERE "traffic_type" = '{selected_type}' """

df = session.sql(f"""
    SELECT 
        "airport_code",
        "city",
        "state",
        "total_flights",
        "total_cancelled",
        "avg_delay_min"
    FROM GOLD.GOLD_AIRPORT_TRAFFIC
    {filter_clause}
""").to_pandas()

df.columns = df.columns.str.lower()

# ================================
# SORT LOGIC
# ================================

if view_type == "Top Traffic":
    df = df.sort_values("total_flights", ascending=False).head(15)
else:
    df = df.sort_values("avg_delay_min", ascending=False).head(15)

# ================================
# KPIs
# ================================

c1, c2, c3 = st.columns(3)
c1.metric("Flights", f"{int(df['total_flights'].sum()):,}")
c2.metric("Cancelled", f"{int(df['total_cancelled'].sum()):,}")
c3.metric("Avg Delay", round(df["avg_delay_min"].mean(), 2))

# ================================
# CHART
# ================================

fig = px.bar(
    df,
    x="airport_code",
    y="total_flights" if view_type == "Top Traffic" else "avg_delay_min",
    color="state",
    title=view_type
)

st.plotly_chart(fig, use_container_width=True)

# ================================
# 🚦 Airport Congestion (Taxi Time)
# ================================

st.subheader("🚦 Airport Congestion (Taxi Time)")

df_taxi = session.sql("""
    SELECT 
        O."iata_code",
        O."city",
        ROUND(AVG(F."taxi_out_min"), 2) AS avg_taxi_out,
        ROUND(AVG(F."taxi_in_min"), 2) AS avg_taxi_in,
        ROUND(AVG(F."departure_delay_min"), 2) AS avg_delay
    FROM GOLD.FACT_FLIGHTS F
    JOIN GOLD.DIM_ORIGIN_AIRPORT O
        ON F."origin_airport_key" = O."airport_key"
    WHERE NOT F."is_cancelled"
    GROUP BY 1,2
    ORDER BY avg_taxi_out DESC
    LIMIT 15
""").to_pandas()

df_taxi.columns = df_taxi.columns.str.lower()

import plotly.express as px

fig = px.scatter(
    df_taxi,
    x="avg_taxi_out",
    y="avg_delay",
    size="avg_taxi_in",
    hover_name="iata_code",
    title="Taxi Time vs Delay (Airport Congestion)"
)

st.plotly_chart(fig, use_container_width=True)

# ================================
# CLEAN TABLE
# ================================

with st.expander("🔍 View Data"):
    st.dataframe(df)

with st.expander("🔍 View Taxi Data"):
    st.dataframe(df_taxi)