"""Build the results chart from the gold marts and save to results/."""
import os
import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

WH = os.environ.get("NYC_WAREHOUSE", "/tmp/nyc_taxi.duckdb")
OUT = os.path.join(os.path.dirname(__file__), "..", "results", "gold_results.png")
con = duckdb.connect(WH)

pay = con.execute("""
    SELECT payment_method, SUM(total_revenue) rev, SUM(trip_count) trips
    FROM gold.mart_payment_type_over_time GROUP BY 1 ORDER BY rev DESC
""").df()
zones = con.execute("""
    SELECT borough, SUM(total_revenue) rev
    FROM gold.mart_revenue_by_zone GROUP BY 1 ORDER BY rev DESC LIMIT 6
""").df()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.6))
ax1.bar(pay.payment_method, pay.rev / 1e6, color="#2f6db0")
ax1.set_title("Gold mart: revenue by payment method")
ax1.set_ylabel("Revenue ($M)")
ax1.tick_params(axis="x", rotation=30)
for i, v in enumerate(pay.rev / 1e6):
    ax1.annotate(f"${v:.1f}M", (i, v), ha="center", va="bottom", fontsize=8)

ax2.barh(zones.borough[::-1], (zones.rev / 1e6)[::-1], color="#2f855a")
ax2.set_title("Gold mart: revenue by pickup borough (top 6)")
ax2.set_xlabel("Revenue ($M)")
plt.tight_layout()
plt.savefig(OUT, dpi=130)
print("saved", os.path.abspath(OUT))
