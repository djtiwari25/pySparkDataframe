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

data =(
 (1, 100),
 (2, 1500),
 (3, 300)
)

df =spark.createDataFrame(data).toDF("sale_id", "amount")

df.select(
    col("sale_id"),
    col("amount"),
    when(col("amount") < 200,"0").
    when((col("amount") >=200) & (col("amount") <= 1000), "10").
    otherwise("20").
    alias("discount")).show()
