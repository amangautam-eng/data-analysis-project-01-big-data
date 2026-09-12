"""
Big Data Analytics & Business Insights (PySpark)

Analyzes large-scale e-commerce data with PySpark DataFrames and Spark SQL,
demonstrates transformation/aggregation workflows, and optimizes jobs with
partitioning and caching.

Run:  python3 scripts/spark_analytics.py
"""
import time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "output"


def main():
    spark = (
        SparkSession.builder
        .appName("EcommerceBigDataAnalytics")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")   # right-sized for demo data volume
        .config("spark.sql.ansi.enabled", "false")     # tolerate mixed/invalid date formats -> NULL instead of erroring
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------------------------------------------- load
    customers = spark.read.csv(str(RAW / "raw_customers.csv"), header=True, inferSchema=True)
    products = spark.read.csv(str(RAW / "raw_products.csv"), header=True, inferSchema=True)
    orders = spark.read.csv(str(RAW / "raw_orders.csv"), header=True, inferSchema=True)
    order_items = spark.read.csv(str(RAW / "raw_order_items.csv"), header=True, inferSchema=True)

    # ------------------------------------------------------- transformation
    customers = customers.dropDuplicates(["customer_id"]) \
        .withColumn("region", F.initcap(F.trim(F.col("region"))))

    products = products.dropDuplicates(["product_id"]) \
        .withColumn("category", F.initcap(F.trim(F.col("category")))) \
        .withColumn("unit_cost", F.abs("unit_cost")) \
        .withColumn("unit_price", F.abs("unit_price"))

    # normalize 3 mixed date formats present in the raw data
    orders = orders.withColumn(
        "order_date",
        F.coalesce(
            F.to_date("order_date", "yyyy-MM-dd"),
            F.to_date("order_date", "dd/MM/yyyy"),
            F.to_date("order_date", "MM-dd-yyyy"),
        ),
    ).filter(F.col("order_date").isNotNull())

    order_items = order_items.filter((F.col("quantity") > 0) & (F.col("unit_price") > 0)) \
        .withColumn("line_total", F.col("quantity") * F.col("unit_price"))

    # -------------------------------------------- partitioning + caching
    # repartition by region for the customer-level aggregations below, and
    # cache the joined "denormalized sales" DataFrame since it's reused by
    # several downstream aggregations (avoids recomputing the join 3x).
    sales = (
        order_items.join(orders, "order_id")
        .join(customers, "customer_id")
        .join(products, "product_id")
        .filter(F.col("status") == "Completed")
        .repartition(8, "region")
    )
    sales.cache()
    sales.count()  # materialize the cache

    # ---------------------------------------------------------- aggregations
    t0 = time.time()

    regional_revenue = (
        sales.groupBy("region", "category")
        .agg(
            F.sum("line_total").alias("revenue"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("order_id").alias("orders"),
        )
        .orderBy(F.desc("revenue"))
    )

    monthly_trend = (
        sales.withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
        .groupBy("year_month")
        .agg(F.sum("line_total").alias("revenue"), F.countDistinct("order_id").alias("orders"))
        .orderBy("year_month")
    )

    # window function: rank customers by spend within each region
    w = Window.partitionBy("region").orderBy(F.desc("customer_revenue"))
    customer_value = (
        sales.groupBy("customer_id", "customer_name", "region")
        .agg(F.sum("line_total").alias("customer_revenue"))
        .withColumn("rank_in_region", F.rank().over(w))
    )
    top_customers_per_region = customer_value.filter(F.col("rank_in_region") <= 5)

    elapsed = time.time() - t0
    print(f"Aggregations computed on cached DataFrame in {elapsed:.2f}s")

    # ------------------------------------------------------------- output
    OUT.mkdir(exist_ok=True)
    for name, df in [
        ("regional_category_revenue", regional_revenue),
        ("monthly_revenue_trend", monthly_trend),
        ("top5_customers_per_region", top_customers_per_region.orderBy("region", "rank_in_region")),
    ]:
        # coalesce(1) for a single readable CSV for the demo (Spark normally
        # writes multiple part-files across the cluster for large jobs)
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUT / name))
        print(f"wrote output/{name}/  ({df.count()} rows)")

    print("\nSample — regional_category_revenue:")
    regional_revenue.show(10, truncate=False)

    sales.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
