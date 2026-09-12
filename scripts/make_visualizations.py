"""Builds simple visualizations from the Spark job's output CSVs."""
import glob
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output"


def load_spark_csv(folder):
    files = glob.glob(str(OUT / folder / "*.csv"))
    return pd.read_csv(files[0])


def main():
    monthly = load_spark_csv("monthly_revenue_trend")
    regional = load_spark_csv("regional_category_revenue")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].plot(monthly["year_month"], monthly["revenue"], marker="o", color="#2563eb")
    axes[0].set_title("Monthly Revenue Trend")
    axes[0].tick_params(axis="x", rotation=90)
    axes[0].set_ylabel("Revenue")

    top_region_cat = regional.sort_values("revenue", ascending=False).head(10)
    axes[1].barh(top_region_cat["region"] + " - " + top_region_cat["category"],
                 top_region_cat["revenue"], color="#16a34a")
    axes[1].invert_yaxis()
    axes[1].set_title("Top 10 Region x Category by Revenue")
    axes[1].set_xlabel("Revenue")

    plt.tight_layout()
    plt.savefig(OUT / "spark_insights.png", dpi=150)
    print(f"saved {OUT / 'spark_insights.png'}")


if __name__ == "__main__":
    main()
