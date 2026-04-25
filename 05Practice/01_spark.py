# Simple PySpark analysis for Episodes.csv
#
# Fix included for macOS "java.net.BindException: Can't assign requested address"
# by forcing Spark driver to bind to localhost.
#
# Run:
#   python episodes_analysis.py
#  Execution time: 81.700s

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, to_date

#   Create Spark session (local mode)
#    Important configs:
#    - spark.driver.bindAddress / spark.driver.host -> avoid bind(..) failed on macOS/VPN/hostname issues
#    - spark.ui.port -> avoid UI port collisions if you have other Spark sessions running

start = time.perf_counter()

spark = (
    SparkSession.builder
    .appName("episodes-analysis")
    .master("local[*]")  # use all CPU cores
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

# Optional: reduce Spark logs in console (comment out if you want full logs)
spark.sparkContext.setLogLevel("WARN")

# 1) Load Episodes.csv
# NOTE: Update the path if Episodes.csv is not in the same folder as this script.
df = spark.read.csv(
    "Episodes.csv",
    header=True,
    inferSchema=True
)

# 2) Basic sanity check
print("\n=== Schema ===")
df.printSchema()

print("\n=== Sample rows ===")
df.show(5, truncate=False)

# 3) Row count
print("\n=== Total rows ===")
print(df.count())

# 4) Episodes by Type (Public / Validation)
print("\n=== Count by Type ===")
(
    df.groupBy("Type")
      .count()
      .orderBy(col("count").desc())
      .show()
)

# 5) Convert CreateTime / EndTime to timestamps
# Assumes format like: "01/03/2020 21:08:01"
df = (
    df.withColumn("CreateTime_ts", to_timestamp("CreateTime", "MM/dd/yyyy HH:mm:ss"))
      .withColumn("EndTime_ts", to_timestamp("EndTime", "MM/dd/yyyy HH:mm:ss"))
)

# 6) Episode duration in seconds
df = df.withColumn(
    "duration_sec",
    unix_timestamp("EndTime_ts") - unix_timestamp("CreateTime_ts")
)

# Summary stats for duration (min/max/mean/etc.)
print("\n=== Duration (seconds) summary ===")
df.select("duration_sec").summary().show()

# 7) Episodes per day (simple time analysis)
print("\n=== Episodes per day (first 10 dates) ===")
(
    df.withColumn("date", to_date("CreateTime_ts"))
      .groupBy("date")
      .count()
      .orderBy("date")
      .show(10, truncate=False)
)


# Stop Spark
spark.stop()
print(f"Execution time: {time.perf_counter() - start:.3f}s")