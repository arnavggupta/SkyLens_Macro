import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px

st.title("⏱ Delay Insights")

session = get_active_session()

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
        SUM(F."weather_delay_min") AS weather_delay,
        SUM(F."airline_delay_min") AS airline_delay,
        SUM(F."nas_delay_min") AS air_system_delay,
        SUM(F."late_aircraft_delay_min") AS late_aircraft_delay
    FROM GOLD.FACT_FLIGHTS F
    JOIN GOLD.DIM_AIRLINE A
        ON F."airline_key" = A."airline_key"
    WHERE NOT F."is_cancelled"
    {filter_clause}
    GROUP BY 1
""").to_pandas()

df.columns = df.columns.str.lower()

# ================================
# Melt (IMPORTANT for Plotly)
# ================================

df_melt = df.melt(
    id_vars="airline_name",
    value_vars=[
        "weather_delay",
        "airline_delay",
        "air_system_delay",
        "late_aircraft_delay"
    ],
    var_name="delay_type",
    value_name="minutes"
)

# ================================
# Plotly Chart
# ================================

fig = px.bar(
    df_melt,
    x="airline_name",
    y="minutes",
    color="delay_type",
    title="Delay Breakdown by Airline",
    barmode="stack"
)

st.plotly_chart(fig, use_container_width=True)

with st.expander("🔍 View Data"):
    st.dataframe(df)

# ================================
# % Contribution
# ================================

df_pct = df.copy()

df_pct["total"] = (
    df_pct["weather_delay"] +
    df_pct["airline_delay"] +
    df_pct["air_system_delay"] +
    df_pct["late_aircraft_delay"]
)

for col in ["weather_delay", "airline_delay", "air_system_delay", "late_aircraft_delay"]:
    df_pct[col] = (df_pct[col] / df_pct["total"]) * 100

df_pct_melt = df_pct.melt(
    id_vars="airline_name",
    value_vars=[
        "weather_delay",
        "airline_delay",
        "air_system_delay",
        "late_aircraft_delay"
    ],
    var_name="delay_type",
    value_name="percentage"
)

fig2 = px.bar(
    df_pct_melt,
    x="airline_name",
    y="percentage",
    color="delay_type",
    title="Delay Contribution (%)",
    barmode="stack"
)

st.plotly_chart(fig2, use_container_width=True)

# Insight
st.markdown("""
### 📌 Insights
- Airline delays typically dominate
- Late aircraft delays show cascading impact
- Weather impact varies significantly across airlines
""")