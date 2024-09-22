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
 (1, "The quick brown fox"),
 (2, "Lorem ipsum dolor sit amet"),
 (3, "Spark is a unified analytics engine")
)

df =spark.createDataFrame(data).toDF("doc_id", "content")

df.select(
    col("doc_id"),
    col("content"),
    when(col("content").contains("fox"),"Animal Related").
    when(col("content").contains("Lorem"),"Placeholder Text").
    when(col("content").contains("Spark"),"Tech Related").
    alias("content_category")).show()
