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
 (1, "john@gmail.com"),
 (2, "jane@yahoo.com"),
 (3, "doe@hotmail.com")
)
df = spark.createDataFrame(data).toDF("customer_id", "email")

df.select(
    col("customer_id"),
    col("email"),
    when(col("email").contains("gmail"),"Gmail").
    when(col("email").contains("yahoo"),"Yahoo").
    otherwise("Other").
    alias("email_provider")).show()