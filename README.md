# Big Data Analytics & Business Insights (PySpark)

Analyzes large-scale e-commerce data with **PySpark DataFrames and Spark
SQL**, with explicit **partitioning and caching** for job optimization.

## What it does
- Loads raw CSVs into Spark DataFrames, cleans dates/categories/dedup
- Joins order_items ⋈ orders ⋈ customers ⋈ products into one denormalized
  `sales` DataFrame
- **Optimization**: `repartition(8, "region")` before the join-heavy work,
  then `.cache()` since the DataFrame is reused across 3 separate
  aggregations (avoids recomputing the 4-way join each time)
- Aggregation workflows:
  - Revenue & units by region × category
  - Monthly revenue trend
  - Window function (`rank()`) — top 5 customers by spend **within each
    region**
- Writes results to `output/` as CSV, plus a matplotlib chart
  (`output/spark_insights.png`)

## Tech
PySpark 4.x (local mode), Spark SQL, Window functions, matplotlib.

## Run it
```bash
pip install pyspark matplotlib
python3 scripts/spark_analytics.py       # runs the Spark job
python3 scripts/make_visualizations.py   # builds the chart from Spark output
```

## Notes on scaling
This demo runs Spark in `local[*]` mode against ~20K order-line rows so it's
fast to demonstrate. The same code runs unchanged against a real
multi-node cluster (`spark-submit --master yarn ...`) and millions of rows —
that's the point of using Spark DataFrames/SQL instead of pandas here.
