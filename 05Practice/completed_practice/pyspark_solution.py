import os
import time
import kagglehub
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# force PySpark to use the Java 17 version
os.environ["JAVA_HOME"] = (
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
)

start = time.perf_counter()

csv_file_path = kagglehub.dataset_download(
    "hhs/health-insurance-marketplace", path="BenefitsCostSharing.csv"
)

spark = (
    SparkSession.builder.appName("benefits-cost-sharing-analysis")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.ui.port", "4041")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)


df_spark = spark.read.csv(csv_file_path, header=True, inferSchema=True)

print("\n=== Schema ===")
df_spark.printSchema()

print("\n=== Total rows ===")
print(df_spark.count())

print("\n=== Count by BusinessYear and EHBVarReason ===")
(
    df_spark.groupBy("BusinessYear", "EHBVarReason")
    .count()
    .orderBy(col("BusinessYear").desc(), col("count").desc())
    .show(50, truncate=False)
)

spark.stop()
print(f"Execution time: {time.perf_counter() - start:.3f}s")
