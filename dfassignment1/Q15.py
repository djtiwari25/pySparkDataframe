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
 (1, 5, 100),
 (2, 10, 150),
 (3, 20, 300)
)

df =spark.createDataFrame(data).toDF("order_id", "quantity", "total_price")
df.createOrReplaceTempView("order")


spark.sql("""
    SELECT order_id,
           quantity,
           total_price,
           CASE
               WHEN quantity < 10 AND total_price < 200 THEN 'Small & Cheap'
               WHEN quantity >= 10 AND total_price < 200 THEN 'Bulk & Discounted'
               ELSE 'Premium Order'
           END AS order_type
    FROM order
""").show()