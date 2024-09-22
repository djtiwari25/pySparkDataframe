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

data = ((1, 30.5),
       (2, 150.75),
       (3, 75.25))
df = spark.createDataFrame(data).toDF("product_id", "price")

df.select(
    col("product_id"),
    col("price"),
    when(col("price") < 50,"cheap").
    when((col("price") >=50) & (col("price") <= 100), "Moderate").
    otherwise("Expensive").
    alias("price_range")).show()
#df.show()