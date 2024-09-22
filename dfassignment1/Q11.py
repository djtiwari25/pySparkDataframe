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
 (1, 25, 30000),
 (2, 45, 50000),
 (3, 35, 40000)
)

df =spark.createDataFrame(data).toDF("employee_id", "age", "salary")

df.createOrReplaceTempView("salary")

spark.sql("""
    SELECT employee_id,
           age,
           salary,
           CASE
               WHEN age < 30 AND salary < 35000 THEN 'Young & Low Salary'
               WHEN age BETWEEN 30 AND 40 AND salary BETWEEN 35000 AND 40000 THEN 'Middle Aged & Medium Salary'
               ELSE 'Old & High Salary'
           END AS category
    FROM salary
""").show()
