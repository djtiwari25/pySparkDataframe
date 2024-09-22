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

data = ((1, 85),(2, 42),(3, 73))
df = spark.createDataFrame(data).toDF("student_id","score")
df.select(col("student_id"),col("score"), when(col("score")>50, "Pass").otherwise("Fail").alias("grade")).show()
