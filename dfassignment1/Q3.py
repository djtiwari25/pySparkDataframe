from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
# import pyspark
# import findspark
# findspark.init()


os.environ["PYSPARK_PYTHON"] = "C:/Program Files/Python310/python.exe"
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk1.8.0_202"
os.environ["SPARK_HOME"] = "C:/apps/spark-3.3.0-bin-hadoop3"

spark = SparkSession.builder\
        .appName("dj1")\
        .master("local[*]")\
        .getOrCreate()

data = ((1, 1000),(2, 200),(3, 5000))
df = spark.createDataFrame(data).toDF("transaction_id", "amount")

df.select(col("transaction_id"),col("amount"), when(col("amount")>1000, "High").when((col("amount") > 500) & (col("amount") <= 1000), "Medium").otherwise("Low").alias("range")).show()