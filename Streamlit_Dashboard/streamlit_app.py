import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px

st.set_page_config(page_title="SkyLens 360", layout="wide")

st.title("✈️ SkyLens 360 Dashboard")
st.caption("End-to-End Flight Analytics Platform")

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

filter_clause = ""
if selected_airline != "All":
    filter_clause = f""" AND A."airline_name" = '{selected_airline}' """

# ================================
# KPIs
# ================================

df_kpi = session.sql(f"""
SELECT 
    COUNT(*) AS total_flights,
    ROUND(AVG(F."arrival_delay_min"), 2) AS avg_arr_delay,
    ROUND(AVG(F."departure_delay_min"), 2) AS avg_dep_delay,
    ROUND(100 * AVG(IFF(F."is_cancelled",1,0)),2) AS cancel_rate
FROM GOLD.FACT_FLIGHTS F
JOIN GOLD.DIM_AIRLINE A
    ON F."airline_key" = A."airline_key"
WHERE A."airline_name" IS NOT NULL
{filter_clause}
""").to_pandas()

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Flights", f"{int(df_kpi['TOTAL_FLIGHTS'][0]):,}")
c2.metric("Avg Arrival Delay (mins)", df_kpi['AVG_ARR_DELAY'][0])
c3.metric("Avg Departure Delay (mins)", df_kpi['AVG_DEP_DELAY'][0])
c4.metric("Cancellation %", df_kpi['CANCEL_RATE'][0])

st.markdown("---")

# ================================
# MONTHLY TREND (IMPROVED UI)
# ================================

st.subheader("📈 Monthly Trends")

df_month = session.sql("""
    SELECT 
        "flight_month",
        "total_flights",
        "avg_arrival_delay_min",
        "on_time_pct"
    FROM GOLD.GOLD_MONTHLY_TRENDS
    ORDER BY "flight_month"
""").to_pandas()

fig = px.line(
    df_month,
    x="flight_month",
    y=["total_flights", "avg_arrival_delay_min"],
    title="Flights vs Delay Trend"
)

st.plotly_chart(fig, use_container_width=True)

# ================================
# QUICK INSIGHTS (🔥 NEW)
# ================================

st.markdown("---")
st.subheader("📌 Key Insights")

best_month = df_month.loc[df_month["on_time_pct"].idxmax()]
worst_month = df_month.loc[df_month["avg_arrival_delay_min"].idxmax()]

st.markdown(f"""
- 🟢 **Best On-Time Month:** {best_month['flight_month']} ({round(best_month['on_time_pct'],2)}%)
- 🔴 **Worst Delay Month:** {worst_month['flight_month']} ({round(worst_month['avg_arrival_delay_min'],2)} min delay)
- ✈️ Flights show seasonal variation with delay fluctuations
""")