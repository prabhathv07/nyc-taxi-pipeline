"""
NYC Taxi — interactive analytics dashboard (Streamlit).

Reads the small parquet bundle in dashboard/data/ (rolled up from the real
Jan-Feb 2024 TLC gold marts) and renders a fully cross-filterable view:
KPIs + revenue-by-borough + by-hour patterns + payment mix + top zones.

Run locally:   streamlit run app.py
Deploy:        push this folder to a Hugging Face Space (sdk: streamlit) or
               Streamlit Community Cloud — see README.md.
"""
import json
from pathlib import Path

import duckdb
import plotly.express as px
import streamlit as st

DATA = Path(__file__).parent / "data"
PX_THEME = "plotly_white"

st.set_page_config(page_title="NYC Taxi Analytics — Jan–Feb 2024",
                   page_icon="🚕", layout="wide")


@st.cache_data
def load():
    # DuckDB reads parquet natively (no pyarrow needed at serve time).
    con = duckdb.connect()
    rd = lambda f: con.execute(f"SELECT * FROM read_parquet('{DATA / f}')").df()
    main, zone, outliers = rd("agg_main.parquet"), rd("agg_zone.parquet"), rd("agg_outliers.parquet")
    con.close()
    meta = json.loads((DATA / "meta.json").read_text())
    return main, zone, outliers, meta


main, zone, outliers, meta = load()

MONTH_LABEL = {"2024-01": "Jan 2024", "2024-02": "Feb 2024"}

# ----------------------------------------------------------------- header
st.title("🚕 NYC Yellow Taxi — Analytics Dashboard")
st.caption(
    f"**Data source: {meta['data_source']}** · "
    f"{meta['bronze_rows']:,} raw trips → PySpark cleaning dropped "
    f"{meta['rows_dropped']:,} ({meta['pct_dropped']}%) → "
    f"{meta['silver_rows']:,} clean trips → dbt gold marts. "
    "Numbers update live with the filters."
)

# ----------------------------------------------------------------- filters
with st.sidebar:
    st.header("Filters")
    months = st.multiselect(
        "Month", options=sorted(main.source_month.unique()),
        default=sorted(main.source_month.unique()),
        format_func=lambda m: MONTH_LABEL.get(m, m))
    pays = st.multiselect(
        "Payment method", options=sorted(main.payment_method.unique()),
        default=sorted(main.payment_method.unique()))
    hours = st.slider("Pickup hour of day", 0, 23, (0, 23))
    boroughs = st.multiselect(
        "Pickup borough", options=sorted(main.borough.unique()),
        default=sorted(main.borough.unique()))
    st.markdown("---")
    st.caption("Built from a medallion pipeline: PySpark → dbt → DuckDB. "
               "Filters recompute from a 1,136-row gold aggregate.")

f = main[
    main.source_month.isin(months)
    & main.payment_method.isin(pays)
    & main.borough.isin(boroughs)
    & main.pickup_hour.between(hours[0], hours[1])
]
fz = zone[zone.source_month.isin(months) & zone.borough.isin(boroughs)]

if f.empty:
    st.warning("No trips match the current filters. Widen them in the sidebar.")
    st.stop()

# ----------------------------------------------------------------- KPIs
trips = int(f.trips.sum())
revenue = float(f.revenue.sum())
avg_fare = f.fare_sum.sum() / trips
tip_pct = 100 * f.tip_sum.sum() / f.fare_sum.sum()
avg_dist = f.dist_sum.sum() / trips

k = st.columns(5)
k[0].metric("Trips", f"{trips:,}")
k[1].metric("Revenue", f"${revenue/1e6:,.1f}M")
k[2].metric("Avg fare", f"${avg_fare:,.2f}")
k[3].metric("Avg tip", f"{tip_pct:,.1f}%")
k[4].metric("Avg distance", f"{avg_dist:,.2f} mi")

st.markdown("---")

# ----------------------------------------------------------------- charts row 1
c1, c2 = st.columns(2)

with c1:
    st.subheader("Revenue by pickup borough")
    g = (f.groupby("borough", as_index=False).revenue.sum()
           .sort_values("revenue", ascending=True))
    g["rev_m"] = g.revenue / 1e6
    fig = px.bar(g, x="rev_m", y="borough", orientation="h",
                 labels={"rev_m": "Revenue ($M)", "borough": ""},
                 template=PX_THEME, text=g.rev_m.map(lambda v: f"${v:,.1f}M"))
    fig.update_traces(marker_color="#2f6db0", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Payment method mix")
    g = f.groupby("payment_method", as_index=False).trips.sum()
    fig = px.pie(g, values="trips", names="payment_method", hole=0.45,
                 template=PX_THEME)
    fig.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)

# ----------------------------------------------------------------- charts row 2
c3, c4 = st.columns(2)

with c3:
    st.subheader("Trips & average fare by hour of day")
    g = f.groupby("pickup_hour", as_index=False).agg(
        trips=("trips", "sum"), fare_sum=("fare_sum", "sum"))
    g["avg_fare"] = g.fare_sum / g.trips
    fig = px.bar(g, x="pickup_hour", y="trips", template=PX_THEME,
                 labels={"pickup_hour": "Hour", "trips": "Trips"})
    fig.update_traces(marker_color="#9ecae1")
    fig.add_scatter(x=g.pickup_hour, y=g.avg_fare, name="Avg fare ($)",
                    yaxis="y2", mode="lines+markers", line=dict(color="#c0392b"))
    fig.update_layout(yaxis2=dict(title="Avg fare ($)", overlaying="y",
                                  side="right", showgrid=False),
                      legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)

with c4:
    st.subheader("Tip % of fare by hour")
    g = f.groupby("pickup_hour", as_index=False).agg(
        tip_sum=("tip_sum", "sum"), fare_sum=("fare_sum", "sum"))
    g["tip_pct"] = 100 * g.tip_sum / g.fare_sum
    fig = px.line(g, x="pickup_hour", y="tip_pct", markers=True,
                  template=PX_THEME,
                  labels={"pickup_hour": "Hour", "tip_pct": "Tip % of fare"})
    fig.update_traces(line_color="#2f855a")
    st.plotly_chart(fig, use_container_width=True)

# ----------------------------------------------------------------- top zones
st.subheader("Top 15 pickup zones by revenue")
tz = (fz.groupby(["borough", "zone"], as_index=False)
        .agg(trips=("trips", "sum"), revenue=("revenue", "sum"))
        .sort_values("revenue", ascending=False).head(15))
tz["revenue_$M"] = (tz.revenue / 1e6).round(2)
fig = px.bar(tz.sort_values("revenue"), x="revenue_$M", y="zone",
             color="borough", orientation="h", template=PX_THEME,
             labels={"revenue_$M": "Revenue ($M)", "zone": ""})
st.plotly_chart(fig, use_container_width=True)
with st.expander("Show the zone table"):
    st.dataframe(tz[["borough", "zone", "trips", "revenue_$M"]]
                 .reset_index(drop=True), use_container_width=True)

# ----------------------------------------------------------------- footer
with st.expander("Data-quality outliers surfaced by the pipeline"):
    st.caption("Legal-but-suspicious trips the gold layer flags for ops review "
               "(kept, not dropped).")
    st.dataframe(outliers.rename(columns={"outlier_reason": "reason", "n": "trips"}),
                 use_container_width=True)

st.caption("Pipeline: raw Parquet → PySpark (silver) → dbt gold + 19 tests → "
           "DuckDB · Dashboard: Streamlit + Plotly · Data: NYC TLC, public.")
