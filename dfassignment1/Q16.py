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
 (1, 25, 60),
 (2, 35, 40),
 (3, 15, 80)
)

df =spark.createDataFrame(data).toDF("day_id", "temperature", "humidity")
df1 =df.select(
    col("day_id"),
    col("temperature"),
    col("humidity"),
    when(col("temperature") > 30,"true").
    otherwise("false").
    alias("is_hot"))

df1.select(col("*"),
    when(col("humidity") > 50, "true").
    otherwise("false").
    alias("is_humid")).show()