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
 (1, "09:00"),
 (2, "18:30"),
 (3, "14:00")
)

df =spark.createDataFrame(data).toDF("login_id", "login_time")
df.select(
    col("login_id"),
    col("login_time"),
    when(hour(col("login_time")) < 12,"true").
    otherwise("false").
    alias("is_morning")).show()
