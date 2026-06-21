"""Static 4-panel preview of the dashboard (for the README)."""
import os, duckdb
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

D = os.path.join(os.path.dirname(__file__), "data")
con = duckdb.connect()
m = con.execute(f"SELECT * FROM read_parquet('{D}/agg_main.parquet')").df()

fig, ax = plt.subplots(2, 2, figsize=(13, 8))
fig.suptitle("NYC Yellow Taxi — Dashboard preview (real TLC, Jan–Feb 2024)", fontsize=14, weight="bold")

b = m.groupby("borough").revenue.sum().sort_values() / 1e6
ax[0,0].barh(b.index, b.values, color="#2f6db0"); ax[0,0].set_title("Revenue by pickup borough ($M)")

p = m.groupby("payment_method").trips.sum().sort_values(ascending=False)
tot = p.sum()
# only print % on slices big enough to read; everything is named in the legend
autopct = lambda v: f"{v:.0f}%" if v >= 3 else ""
wedges, _, _ = ax[0,1].pie(p.values, autopct=autopct, startangle=90,
                           pctdistance=0.72, colors=plt.cm.tab10.colors,
                           textprops={"fontsize": 10, "color": "white"})
ax[0,1].legend(wedges, [f"{n} — {v/tot*100:.1f}%" for n, v in p.items()],
               loc="center left", bbox_to_anchor=(0.98, 0.5),
               fontsize=8, frameon=False)
ax[0,1].set_title("Payment method mix")

h = m.groupby("pickup_hour").agg(trips=("trips","sum"), fare=("fare_sum","sum"))
h["avg_fare"] = h.fare/h.trips
ax[1,0].bar(h.index, h.trips/1e3, color="#9ecae1"); ax[1,0].set_title("Trips (k) & avg fare by hour"); ax[1,0].set_xlabel("hour")
ax2 = ax[1,0].twinx(); ax2.plot(h.index, h.avg_fare, color="#c0392b", marker="o", ms=3); ax2.set_ylabel("avg fare $")

t = m.groupby("pickup_hour").agg(tip=("tip_sum","sum"), fare=("fare_sum","sum"))
ax[1,1].plot(t.index, 100*t.tip/t.fare, color="#2f855a", marker="o", ms=3); ax[1,1].set_title("Tip % of fare by hour"); ax[1,1].set_xlabel("hour")

plt.tight_layout(rect=[0,0,1,0.96])
out = os.path.join(os.path.dirname(__file__), "preview.png")
plt.savefig(out, dpi=120); print("saved", out)
