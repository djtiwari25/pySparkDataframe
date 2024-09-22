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
 (1, 5),
 (2, 15),
 (3, 25)
)

df = spark.createDataFrame(data).toDF("item_id", "quantity")

df.select(
    col("item_id"),
    col("quantity"),
    when(col("quantity") < 10,"low").
    when((col("quantity") >=10) & (col("quantity") <= 20), "Medium").
    otherwise("High").
    alias("stock_level")).show()
