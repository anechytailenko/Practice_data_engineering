# Simple pandas analysis for Episodes.csv
#
# Equivalent to the PySpark script:
# - load CSV
# - show schema/sample
# - count rows
# - count by Type
# - parse CreateTime/EndTime
# - compute duration_sec + summary stats
# - episodes per day
# - negative duration count
#
# Run:
#  python episodes_analysis_pandas.py
#  Execution time: 188.508s

import time
import pandas as pd

start = time.perf_counter()


# 1) Load Episodes.csv
# NOTE: Update the path if Episodes.csv is not in the same folder as this script.
df = pd.read_csv("Episodes.csv")

# 2) Basic sanity check
print("\n=== Columns & dtypes ===")
print(df.dtypes)

print("\n=== Sample rows ===")
print(df.head(5).to_string(index=False))

# 3) Row count
print("\n=== Total rows ===")
print(len(df))

# 4) Episodes by Type (Public / Validation)
print("\n=== Count by Type ===")
print(df["Type"].value_counts(dropna=False).to_string())

# 5) Convert CreateTime / EndTime to timestamps
# Assumes format like: "01/03/2020 21:08:01"
df["CreateTime_ts"] = pd.to_datetime(df["CreateTime"], format="%m/%d/%Y %H:%M:%S", errors="coerce")
df["EndTime_ts"] = pd.to_datetime(df["EndTime"], format="%m/%d/%Y %H:%M:%S", errors="coerce")

# 6) Episode duration in seconds
df["duration_sec"] = (df["EndTime_ts"] - df["CreateTime_ts"]).dt.total_seconds()

# Summary stats for duration (min/max/mean/etc.)
print("\n=== Duration (seconds) summary ===")
print(df["duration_sec"].describe().to_string())

# 7) Episodes per day (simple time analysis)
print("\n=== Episodes per day (first 10 dates) ===")
df["date"] = df["CreateTime_ts"].dt.date
per_day = df.groupby("date").size().sort_index()
print(per_day.head(10).to_string())
print(f"Execution time: {time.perf_counter() - start:.3f}s")