import pyspark
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
 (1, "2024-07-01", "2024-07-10"),
 (2, "2024-08-01", "2024-08-15"),
 (3, "2024-09-01", "2024-09-05")
)

df =spark.createDataFrame(data).toDF("task_id", "start_date", "end_date")

df.select(
    col("task_id"),
    col("start_date"),
    col("end_date"),
    when(datediff(col("end_date"),col("start_date")) < 7, "Short").
    when((datediff(col("end_date"),col("start_date")) > 7) & (datediff(col("end_date"),col("start_date")) < 14) , "Medium").
    otherwise("Long").
    alias("task_duration")).show()


