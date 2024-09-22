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
 (1, "user@gmail.com"),
 (2, "admin@yahoo.com"),
 (3, "info@hotmail.com")
)

df = spark.createDataFrame(data).toDF("email_id", "email_address")


df.select(
    col("email_id"),
    col("email_address"),
    when(col("email_address").contains("gmail"),"Gmail").
    when(col("email_address").contains("yahoo"),"Yahoo").
    otherwise("Hotmail").
    alias("email_domain")).show()