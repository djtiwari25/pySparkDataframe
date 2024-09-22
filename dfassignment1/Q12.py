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
 (1, 1),
 (2, 4),
 (3, 5)
)

df =spark.createDataFrame(data).toDF("review_id", "rating")

# df.createOrReplaceTempView("rating")
#
# spark.sql("""
#     SELECT review_id,
#            rating,
#            CASE
#                WHEN rating < 3 THEN 'Bad'
#                WHEN rating BETWEEN 3 AND 4 THEN 'Good'
#                ELSE 'Excellent'
#            END AS feedback,
#            CASE
#                WHEN rating >= 3 THEN 'true'
#                ELSE 'false'
#            END AS is_positive
#     FROM salary
# """).show()

df1 = df.select(
    col("review_id"),
    col("rating"),
    when(col("rating") <3 ,"Bad").
    when((col("rating") >=3) & (col("rating") <= 4), "Good").
    otherwise("Excellent").
    alias("feedback"))

df1.select(
    col("*"),
    when(col("rating") >= 3, "true").
    otherwise("false").
    alias("is_positive")).show()