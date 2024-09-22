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
 (1, 85, 92),
 (2, 58, 76),
 (3, 72, 64)
)
df =spark.createDataFrame(data).toDF("student_id", "math_score", "english_score")
df1 = df.select(
    col("*"),
    when(col("math_score") > 80 ,"A").
    when((col("math_score") >=60) & (col("math_score") <= 80), "B").
    otherwise("C").
    alias("math_grade"))

df1.select(
    col("*"),
    when(col("english_score") > 80 ,"A").
    when((col("english_score") >=60) & (col("english_score") <= 80), "B").
    otherwise("C").
    alias("english_grade")).show()