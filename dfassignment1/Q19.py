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

data=(
 (1, "2024-07-15"),
 (2, "2024-12-25"),
 (3, "2024-11-01")
)

df = spark.createDataFrame(data).toDF("payment_id", "payment_date")
df.select(
    col("*"),
    when((month(col("payment_date")) == "01") | (month(col("payment_date")) == "02") | (month(col("payment_date")) == "03"),"Q1").
    when((month(col("payment_date")) == "04") | (month(col("payment_date")) == "05") | (month(col("payment_date")) == "06"),"Q2").
    when((month(col("payment_date")) == "07") | (month(col("payment_date")) == "08") | (month(col("payment_date")) == "09"),"Q3").
    otherwise("Q4").
    alias("quarter")).show()