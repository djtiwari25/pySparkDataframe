from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

os.environ["PYSPARK_PYTHON"] = "C:/Program Files/Python310/python.exe"
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"] = "C:/apps/spark-3.3.0-bin-hadoop3"

spark = SparkSession.builder\
        .appName("dj1")\
        .master("local[*]")\
        .getOrCreate()

data = (
 (1, "2024-07-27"),
 (2, "2024-12-25"),
 (3, "2025-01-01")
)
df = spark.createDataFrame(data).toDF("event_id", "date")

df.select(
    col("event_id"),
    col("date"),
    when((col("date") == "2024-12-25") | (col("date") == "2025-01-01"), "true").otherwise("false").alias("is_holiday")).show()