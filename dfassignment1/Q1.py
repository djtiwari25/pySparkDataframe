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

data =  ((1, "John", 28),(2, "Jane", 35),(3, "Doe", 22))
df = spark.createDataFrame(data).toDF("id","name","age")

df.select(col("id"),col("name"),col("age"), when(col("age") >= 18, "true").otherwise("false").alias("is_adult")).show()
