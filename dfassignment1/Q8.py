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
 (1, "2024-07-01"),
 (2, "2024-12-01"),
 (3, "2024-05-01")
)

df = spark.createDataFrame(data).toDF("order_id", "order_date")
df1 = pyspark.sql.functions.split(df['order_date'],'-')
# df.select(col("order_date"), df1.getItem(1).alias("month")).show()
df.select(
    col("order_id"),
    col("order_date"),
    when((df1.getItem(1).contains("06"))|(df1.getItem(1).contains("07"))|(df1.getItem(1).contains("08")),"Summer").
    when((df1.getItem(1).contains("12"))|(df1.getItem(1).contains("01"))|(df1.getItem(1).contains("02")),"Winter").
    otherwise("Other").
    alias("season")).show()